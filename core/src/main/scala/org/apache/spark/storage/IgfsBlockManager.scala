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
import java.util.Date

import com.google.common.io.ByteStreams
import org.apache.ignite.Ignition
import org.apache.ignite.igfs.IgfsPath
import org.apache.spark.executor.ExecutorExitCode
import org.apache.spark.util.{ShutdownHookManager, Utils}
import org.apache.spark.Logging

import scala.util.Random
import scala.util.control.NonFatal

/**
  * Created by INFI on 2016/3/20.
  */
private[spark] class IgfsBlockManager() extends ExternalBlockManager with Logging{

  var rootDirs: String = _
  var master: String = _
  private var subDirsPerIgfsDir: Int = _

  private var igfsDirs: Array[IgfsPath] = _
  private var subdirs: Array[Array[IgfsPath]] = _

  private val ignite = Ignition.ignite()
  private val fs = ignite.fileSystem("spark-on-igfs")

  override def init(blockManager: BlockManager, executorId: String): Unit = {
    super.init(blockManager, executorId)
    val storeDir = blockManager.conf.get(ExternalBlockStore.BASE_DIR, "/tmp_spark_igfs")
    val appFolderName = blockManager.conf.get(ExternalBlockStore.FOLD_NAME)

    rootDirs = s"$storeDir/$appFolderName/$executorId"
    master = blockManager.conf.get(ExternalBlockStore.MASTER_URL, "igfs:///")


    if ( fs == null ) {
      logError("Failed to connect to the IGFS as the master address is not configured ")
      throw new IOException("Failed to connect to the tachyon as the master" +
       "address is not configured")
    }

    subDirsPerIgfsDir = blockManager.conf.get("spark.externalBlockStore.subDirectories" ,
      ExternalBlockStore.SUB_DIRS_PER_DIR).toInt

    igfsDirs = createIgfsDirs()
    subdirs = Array.fill(igfsDirs.length)(new Array[IgfsPath](subDirsPerIgfsDir))
    igfsDirs.foreach(igfsDir => ShutdownHookManager.registerShutdownDeleteDir(igfsDir))
  }

  override def toString(): String = {"ExternalBlockStore-Igfs"}

  override def removeBlock(blockId: BlockId): Boolean = {
    val file = getFile(blockId)
    if (fileExists(file)) {
      removeFile(file)
    } else {
      false
    }
  }

  override def blockExists(blockId: BlockId): Boolean = {
    val file = getFile(blockId)
    fileExists(file)
  }

  override def putBytes(blockId: BlockId, bytes: ByteBuffer): Unit = {
    val file = getFile(blockId)
    val os = fs.create(file, false)
    try {
      os.write(bytes.array())
    } catch {
      case NonFatal(e) =>
        logWarning(s"Failed to put bytes of block $blockId into Tachyon", e)
        os.close()
    } finally {
      os.close()
    }
  }

  override def putValues(blockId: BlockId, values: Iterator[_]): Unit = {
    val file = getFile(blockId)
    val os = fs.create(file, false)
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

  override def getBytes(blockId: BlockId): Option[ByteBuffer] = {
    val file = getFile(blockId)
    if (file == null) {
      return None
    }
    val is = fs.open(file)
    try {
      val size = fs.info(getFile(blockId.name)).length()
      val bs = new Array[Byte](size.asInstanceOf[Int])
      ByteStreams.readFully(is, bs)
      Some(ByteBuffer.wrap(bs))
    } catch {
      case NonFatal(e) =>
        logWarning(s"Failed to get bytes of block $blockId from igfs", e)
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
    val is = fs.open(file)
    Option(is).map { is =>
      blockManager.dataDeserializeStream(blockId, is)
    }
  }

  override def getSize(blockId: BlockId): Long = {
    val fileMeta = fs.info(getFile(blockId.name))
    fileMeta.length()
  }

  def removeFile(file: IgfsPath): Boolean = {
    fs.delete(file, false)
  }

  def fileExists(file: IgfsPath): Boolean = {
    fs.exists(file)
  }

  def getFile(filename: String): IgfsPath = {
    val hash = Utils.nonNegativeHash(filename)
    val dirId = hash % igfsDirs.length
    val subDirId = (hash / igfsDirs.length) % subDirsPerIgfsDir

    var subDir = subdirs(dirId)(subDirId)
    if (subDir == null) {
      subDir = subdirs(dirId).synchronized {
        val old = subdirs(dirId)(subDirId)
        if (old != null) {
          old
        } else {
          val path = new IgfsPath(s"${igfsDirs(dirId)}/${"%02x".format(subDirId)}")
          fs.mkdirs(path)
          val newDir = path
          subdirs(dirId)(subDirId) = newDir
          newDir
        }
      }
    }
    val filePath = new IgfsPath(s"$subDir/$filename")
    filePath
  }

  def getFile(blockId: BlockId): IgfsPath = getFile(blockId.name)

  private def createIgfsDirs(): Array[IgfsPath] = {
    logDebug("Creating igfs directories at root dirs '" + rootDirs + "'")
    val dataFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    rootDirs.split(",").map {
      rootdir =>
        var foundLocalDir = false
        var igfsDir: IgfsPath = null
        var igfsDirId: String = null
        var tries = 0
        val rand = new Random()
        while (!foundLocalDir && tries < ExternalBlockStore.MAX_DIR_CREATION_ATTEMPTS) {
          tries += 1
          try  {
            igfsDirId = "%s-%04x".format(dataFormat.format(new Date), rand.nextInt(65536))
            val path = new IgfsPath(s"$rootdir/spark-igfs-$igfsDirId")
            if (!fs.exists(path)) {
              foundLocalDir = fs.exists(path)
              igfsDir = path
            }
          } catch {
            case NonFatal(e) =>
              logWarning(s"Attempt $tries to create igfs dir + $igfsDir faild", e)
          }
        }

        if (!foundLocalDir) {
          logError(s"Failed ${ExternalBlockStore.MAX_DIR_CREATION_ATTEMPTS} " +
            s"attemps to create igfs dir in $rootdir}")
          System.exit(ExecutorExitCode.EXTERNAL_BLOCK_STORE_FAILED_TO_CREATE_DIR)
        }

        logInfo(s"Created igfs directory at $igfsDir")
        igfsDir
    }
  }

  override def shutdown(): Unit = {
    logDebug("Shutdown hook called")
    igfsDirs.foreach { igfsDir =>
      try {
        if (!ShutdownHookManager.hasRootAsShutdownDeleteDir(igfsDir)) {
          Utils.deleteRecursively()
        }
      }
    }
  }
}
