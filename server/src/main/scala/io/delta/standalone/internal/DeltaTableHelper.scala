/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.standalone.internal

import java.net.URI
import java.nio.charset.StandardCharsets.UTF_8

import io.delta.sharing.server.{CloudFileSigner, S3FileSigner, model}
import com.google.common.hash.Hashing
import io.delta.sharing.server.config.TableConfig
import io.delta.standalone.DeltaLog
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.s3a.S3AFileSystem

/**
 * TODO
 *  - Clean up this class.
 *  - Better error message.
 *  - Support predicateHits and limitHit.
 */
object DeltaTableHelper {

  /**
   * Run `func` under the classloader of `DeltaTableHelper`. We cannot use the classloader set by
   * Armeria as Hadoop needs to search the classpath to find its classes.
   */
  def withClassLoader[T](func: => T): T = {
    val classLoader = Thread.currentThread().getContextClassLoader
    if (classLoader == null) {
      Thread.currentThread().setContextClassLoader(this.getClass.getClassLoader)
      try {
        func
      } finally {
        Thread.currentThread().setContextClassLoader(null)
      }
    } else {
      func
    }
  }

  def getTableVersion(tableConfig: TableConfig): Long = withClassLoader {
    // TODO only list files
    val conf = new Configuration()
    val tablePath = new Path(tableConfig.getLocation)
    val deltaLog = DeltaLog.forTable(conf, tablePath).asInstanceOf[DeltaLogImpl]
    deltaLog.snapshot.version
  }

  def query(
      tableConfig: TableConfig,
      shouldReturnFiles: Boolean,
      predicateHits: Seq[String],
      limitHint: Option[Int],
      preSignedUrlTimeoutSeconds: Long): (Long, Seq[Any]) = withClassLoader {
    val conf = new Configuration()
    val tablePath = new Path(tableConfig.getLocation)
    val fs = tablePath.getFileSystem(conf)
    if (!fs.isInstanceOf[S3AFileSystem]) {
      throw new IllegalStateException("Cannot share tables on non S3 file systems")
    }
    val deltaLog = DeltaLog.forTable(conf, tablePath).asInstanceOf[DeltaLogImpl]
    val snapshot = deltaLog.snapshot
    if (snapshot.version < 0) {
      throw new IllegalStateException(s"The table ${tableConfig.getName} " +
        s"doesn't exist on the file system or is not a Delta table")
    }
    val stateMethod = snapshot.getClass.getMethod("state")
    val state = stateMethod.invoke(snapshot).asInstanceOf[SnapshotImpl.State]
    val selectedFiles = state.activeFiles.values.toSeq
    val signer = new S3FileSigner(tablePath.toUri, conf, preSignedUrlTimeoutSeconds)
    val modelProtocol = model.Protocol(state.protocol.minReaderVersion)
    val modelMetadata = model.Metadata(
      id = state.metadata.id,
      name = state.metadata.name,
      description = state.metadata.description,
      format = model.Format(),
      // TODO Clean up Column's metadata and add test
      schemaString = state.metadata.schemaString,
      partitionColumns = state.metadata.partitionColumns
    )
    snapshot.version -> (Seq(modelProtocol.wrap, modelMetadata.wrap) ++ {
      if (shouldReturnFiles) {
        selectedFiles.map { addFile =>
          val cloudPath = absolutePath(deltaLog.dataPath, addFile.path)
          val signedUrl = signFile(signer, cloudPath)
          val modelAddFile = model.AddFile(url = signedUrl,
            id = Hashing.md5().hashString(addFile.path, UTF_8).toString,
            partitionValues = addFile.partitionValues,
            size = addFile.size,
            stats = addFile.stats)
          modelAddFile.wrap
        }
      } else {
        Nil
      }
    })
  }

  private def absolutePath(path: Path, child: String): Path = {
    val p = new Path(new URI(child))
    if (p.isAbsolute) {
      throw new IllegalStateException("table containing absolute paths cannot be shared")
    } else {
      new Path(path, p)
    }
  }

  private def signFile(signer: CloudFileSigner, path: Path): String = {
    val absPath = path.toUri
    val bucketName = absPath.getHost
    val objectKey = absPath.getPath.stripPrefix("/")
    signer.sign(bucketName, objectKey).toString
  }
}
