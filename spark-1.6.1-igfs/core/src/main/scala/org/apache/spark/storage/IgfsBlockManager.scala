/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.storage

import java.io.IOException
import java.nio.ByteBuffer
import java.text.SimpleDateFormat
import java.util.{Date, Random}

import com.google.common.io.ByteStreams
import org.apache.ignite.configuration.{FileSystemConfiguration, IgniteConfiguration}
import org.apache.ignite.igfs.{IgfsFile, IgfsPath}
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder
import org.apache.ignite.{IgniteFileSystem, Ignition}
import org.apache.spark.Logging
import org.apache.spark.executor.ExecutorExitCode
import org.apache.spark.util.{ShutdownHookManager, Utils}

import scala.util.control.NonFatal


/**
  * Creates and maintains the logical mapping between logical blocks and Ignite fs locations. By
  * default, one block is mapped to one file with a name given by its BlockId.
  *
  */
private[spark] class IgfsBlockManager() extends ExternalBlockManager with Logging {

  var rootDirs: String = _
  var master: String = _
  var client: IgniteFileSystem = _
  private var subDirsPerIgfsDir: Int = _

  // Create one Tachyon directory for each path mentioned in spark.tachyonStore.folderName;
  // then, inside this directory, create multiple subdirectories that we will hash files into,
  // in order to avoid having really large inodes at the top level in Tachyon.
  private var igfsDirs: Array[IgfsFile] = _
  private var subDirs: Array[Array[IgfsFile]] = _


  override def init(blockManager: BlockManager, executorId: String): Unit = {
    super.init(blockManager, executorId)
    val storeDir = blockManager.conf.get(ExternalBlockStore.BASE_DIR, "/tmp_spark_igfs")
    val appFolderName = blockManager.conf.get(ExternalBlockStore.FOLD_NAME)

    val igniteConfig = new IgniteConfiguration()
    val disoSpi = new TcpDiscoverySpi()
    disoSpi.setIpFinder(new TcpDiscoveryVmIpFinder(true))
    igniteConfig.setDiscoverySpi(disoSpi)
    igniteConfig.setCacheConfiguration()
    val igfsConfig = new FileSystemConfiguration()

    igfsConfig.setName("igfs")
    igfsConfig.setDataCacheName("igfsDatacache")
    igfsConfig.setMetaCacheName("igfsMetacache")
    igniteConfig.setFileSystemConfiguration(igfsConfig)
    igniteConfig.setLocalHost("127.0.0.1")

    rootDirs = s"$storeDir/$appFolderName/$executorId"
    master = blockManager.conf.get(ExternalBlockStore.MASTER_URL, "localhost:10500")
    client = if (master != null && master != "") {
      Ignition.start("/home/com/workspace/apache-ignite-1.5.0.final-src/config/default-config.xml")
      val ignite = Ignition.ignite()
      //    ignite.addCacheConfiguration(cacheConfig)
      val cachecfg = ignite.configuration().getCacheConfiguration
      if (cachecfg != null) {
        logInfo(s"BM@IgfsBlockManager cache set success")

      }
      logInfo(s"BM@IgfsBlockManager cache name")
      ignite.fileSystem("igniteFileSystem")

    } else {
      null

    }

    // original implementation call System.exit, we change it to run without extblkstore support
    if (client == null) {
      logError("Failed to connect to the Igfs as the master address is not configured")
      throw new IOException("Failed to connect to the Igfs as the master " +
        "address is not configured")

    }
    subDirsPerIgfsDir = blockManager.conf.get("spark.externalBlockStore.subDirectories",
      ExternalBlockStore.SUB_DIRS_PER_DIR).toInt

    // Create one Igfs directory for each path mentioned in spark.tachyonStore.folderName;
    // then, inside this directory, create multiple subdirectories that we will hash files into,
    // in order to avoid having really large inodes at the top level in Tachyon.
    igfsDirs = createIgfsDirs()
    subDirs = Array.fill(igfsDirs.length)(new Array[IgfsFile](subDirsPerIgfsDir))
    igfsDirs.foreach(igfsDir => ShutdownHookManager.registerShutdownDeleteDir(igfsDir))
    logInfo("Igfs init finished")

  }

  // TODO: Some of the logic here could be consolidated/de-duplicated with that in the DiskStore.
  private def createIgfsDirs(): Array[IgfsFile] = {
    logDebug(s"Creating tachyon directories at root dirs '$rootDirs'")
    val dateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    rootDirs.split(",").map { rootDir =>
      var foundLocalDir = false
      var igfsDir: IgfsFile = null
      var igfsDirId: String = null
      var tries = 0
      val rand = new Random()
      while (!foundLocalDir && tries < ExternalBlockStore.MAX_DIR_CREATION_ATTEMPTS) {
        tries += 1
        try {
          igfsDirId = "%s-%04x".format(dateFormat.format(new Date), rand.nextInt(65536))
          val path = new IgfsPath(s"$rootDir/spark-igfs-$igfsDirId")
          if (!client.exists(path)) {
            client.mkdirs(path)
            foundLocalDir = client.exists(path)
            igfsDir = client.info(path)

          }

        } catch {
          case NonFatal(e) =>
            logWarning(s"Attempt ${tries}s to create igfs dir $igfsDir failed", e)

        }

      }
      if (!foundLocalDir) {
        logError(s"Failed ${ExternalBlockStore.MAX_DIR_CREATION_ATTEMPTS}"
          + s" attempts to create tachyon dir in $rootDir")
        System.exit(ExecutorExitCode.EXTERNAL_BLOCK_STORE_FAILED_TO_CREATE_DIR)

      }
      logInfo(s"Created igfs directory at $igfsDir")
      igfsDir
    }

  }

  override def toString: String = {
    "ExternalBlockStore-Igfs"

  }

  override def removeBlock(blockId: BlockId): Boolean = {
    val file = getFile(blockId)
    if (fileExists(file)) {
      removeFile(file)

    } else {
      false

    }

  }

  def removeFile(file: IgfsFile): Boolean = {
    client.delete(file.path(), false)

  }

  override def blockExists(blockId: BlockId): Boolean = {
    val file = getFile(blockId)
    fileExists(file)

  }

  def fileExists(file: IgfsFile): Boolean = {
    client.exists(file.path())

  }

  override def putBytes(blockId: BlockId, bytes: ByteBuffer): Unit = {
    val file = getFile(blockId)
    val os = client.create(file.path(), false)
    try {
      os.write(bytes.array())

    } catch {
      case NonFatal(e) =>
        logWarning(s"Failed to put bytes of block $blockId into Igfs", e)
        os.close()

    } finally {
      os.close()

    }

  }

  override def putValues(blockId: BlockId, values: Iterator[_]): Unit = {
    val file = getFile(blockId)
    val os = client.create(file.path(), false)
    try {
      blockManager.dataSerializeStream(blockId, os, values)

    } catch {
      case NonFatal(e) =>
        logWarning(s"Failed to put values of block $blockId into Tachyon", e)
        os.close()

    } finally {
      os.close()

    }

  }

  def getFile(blockId: BlockId): IgfsFile = getFile(blockId.name)

  override def getBytes(blockId: BlockId): Option[ByteBuffer] = {
    val file = getFile(blockId)
    if (file == null) {
      return None

    }
    val is = client.open(file.path())
    try {
      val size = file.length
      val bs = new Array[Byte](size.asInstanceOf[Int])
      ByteStreams.readFully(is, bs)
      Some(ByteBuffer.wrap(bs))

    } catch {
      case NonFatal(e) =>
        logWarning(s"Failed to get bytes of block $blockId from Igfs", e)
        None

    } finally {
      is.close()

    }

  }

  override def getValues(blockId: BlockId): Option[Iterator[_]] = {
    val file = getFile(blockId)
    if (file == null) {
      return None

    }
    val is = client.open(file.path())
    Option(is).map { is =>
      blockManager.dataDeserializeStream(blockId, is)
    }

  }

  override def getSize(blockId: BlockId): Long = {
    getFile(blockId.name).length

  }

  def getFile(filename: String): IgfsFile = {
    // Figure out which igfs directory it hashes to, and which subdirectory in that
    val hash = Utils.nonNegativeHash(filename)
    val dirId = hash % igfsDirs.length
    val subDirId = (hash / igfsDirs.length) % subDirsPerIgfsDir

    // Create the subdirectory if it doesn't already exist
    var subDir = subDirs(dirId)(subDirId)
    if (subDir == null) {
      subDir = subDirs(dirId).synchronized {
        val old = subDirs(dirId)(subDirId)
        if (old != null) {
          old

        } else {
          val path = new IgfsPath(s"${igfsDirs(dirId)}/${"%02x".format(subDirId)}")
          client.mkdirs(path)
          val newDir = client.info(path)
          subDirs(dirId)(subDirId) = newDir
          newDir

        }

      }

    }
    val filePath = new IgfsPath(s"$subDir/$filename")

    if (!client.exists(filePath)) {
      client.info(filePath)

    } else {
      logError(s"BM@Igfs can not find file ${filePath.name()}")
      null

    }

  }

  override def shutdown() {
    logDebug("Shutdown hook called")
    igfsDirs.foreach { igfsDir =>
      try {
        if (!ShutdownHookManager.hasRootAsShutdownDeleteDir(0)) {
          Utils.deleteRecursively(igfsDir, client)

        }

      } catch {
        case NonFatal(e) =>
          logError(s"Exception while deleting igfs spark dir: $igfsDir", e)

      }
    }
    client = null

  }

}

