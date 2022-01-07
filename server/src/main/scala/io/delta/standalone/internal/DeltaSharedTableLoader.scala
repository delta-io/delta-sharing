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

// Putting these classes in this package to access Delta Standalone internal APIs
package io.delta.standalone.internal

import java.net.URI
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.TimeUnit

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem
import com.google.common.cache.CacheBuilder
import com.google.common.hash.Hashing
import io.delta.standalone.DeltaLog
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.azure.NativeAzureFileSystem
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem
import org.apache.hadoop.fs.s3a.S3AFileSystem
import org.apache.spark.sql.types.{DataType, MetadataBuilder, StructType}

import io.delta.sharing.server.{model, AbfsFileSigner, GCSFileSigner, S3FileSigner, WasbFileSigner}
import io.delta.sharing.server.config.{ServerConfig, TableConfig}

/**
 * A class to load Delta tables from `TableConfig`. It also caches the loaded tables internally
 * to speed up the loading.
 */
class DeltaSharedTableLoader(serverConfig: ServerConfig) {
  private val deltaSharedTableCache = {
    CacheBuilder.newBuilder()
      .expireAfterAccess(60, TimeUnit.MINUTES)
      .maximumSize(serverConfig.deltaTableCacheSize)
      .build[String, DeltaSharedTable]()
  }

  def loadTable(tableConfig: TableConfig): DeltaSharedTable = {
    val deltaSharedTable =
      deltaSharedTableCache.get(tableConfig.location, () => {
        new DeltaSharedTable(
          tableConfig,
          serverConfig.preSignedUrlTimeoutSeconds,
          serverConfig.evaluatePredicateHints)
      })
    if (!serverConfig.stalenessAcceptable) {
      deltaSharedTable.update()
    }
    deltaSharedTable
  }
}

/**
 * A table class that wraps `DeltaLog` to provide the methods used by the server.
 */
class DeltaSharedTable(
    tableConfig: TableConfig,
    preSignedUrlTimeoutSeconds: Long,
    evaluatePredicateHints: Boolean) {

  private val conf = withClassLoader {
    new Configuration()
  }

  private val deltaLog = withClassLoader {
    val tablePath = new Path(tableConfig.getLocation)
    DeltaLog.forTable(conf, tablePath).asInstanceOf[DeltaLogImpl]
  }

  private val fileSigner = withClassLoader {
    val tablePath = new Path(tableConfig.getLocation)
    val fs = tablePath.getFileSystem(conf)
    fs match {
      case _: S3AFileSystem =>
        new S3FileSigner(deltaLog.dataPath.toUri, conf, preSignedUrlTimeoutSeconds)
      case wasb: NativeAzureFileSystem =>
        WasbFileSigner(wasb, deltaLog.dataPath.toUri, conf, preSignedUrlTimeoutSeconds)
      case abfs: AzureBlobFileSystem =>
        AbfsFileSigner(abfs, deltaLog.dataPath.toUri, preSignedUrlTimeoutSeconds)
      case gc: GoogleHadoopFileSystem =>
        new GCSFileSigner(deltaLog.dataPath.toUri, conf, preSignedUrlTimeoutSeconds)
      case _ =>
        throw new IllegalStateException(s"File system ${fs.getClass} is not supported")
    }
  }

  /**
   * Run `func` under the classloader of `DeltaSharedTable`. We cannot use the classloader set by
   * Armeria as Hadoop needs to search the classpath to find its classes.
   */
  private def withClassLoader[T](func: => T): T = {
    val classLoader = Thread.currentThread().getContextClassLoader
    if (classLoader == null) {
      Thread.currentThread().setContextClassLoader(this.getClass.getClassLoader)
      try func finally {
        Thread.currentThread().setContextClassLoader(null)
      }
    } else {
      func
    }
  }

  /** Check if the table version in deltalog is valid */
  private def validateDeltaTable(snapshot: SnapshotImpl): Unit = {
    if (snapshot.version < 0) {
      throw new IllegalStateException(s"The table ${tableConfig.getName} " +
        s"doesn't exist on the file system or is not a Delta table")
    }
  }

  /** Return the current table version */
  def tableVersion: Long = withClassLoader {
    val snapshot = deltaLog.snapshot
    validateDeltaTable(snapshot)
    snapshot.version
  }

  def query(
      includeFiles: Boolean,
      predicateHits: Seq[String],
      limitHint: Option[Long]): (Long, Seq[model.SingleAction]) = withClassLoader {
    // TODO Support `limitHint`
    val snapshot = deltaLog.snapshot
    validateDeltaTable(snapshot)
    // TODO Open the `state` field in Delta Standalone library.
    val stateMethod = snapshot.getClass.getMethod("state")
    val state = stateMethod.invoke(snapshot).asInstanceOf[SnapshotImpl.State]
    val modelProtocol = model.Protocol(state.protocol.minReaderVersion)
    val modelMetadata = model.Metadata(
      id = state.metadata.id,
      name = state.metadata.name,
      description = state.metadata.description,
      format = model.Format(),
      schemaString = cleanUpTableSchema(state.metadata.schemaString),
      partitionColumns = state.metadata.partitionColumns
    )
    val actions = Seq(modelProtocol.wrap, modelMetadata.wrap) ++ {
      if (includeFiles) {
        val selectedFiles = state.activeFiles.values.toSeq
        val filteredFilters =
          if (evaluatePredicateHints && modelMetadata.partitionColumns.nonEmpty) {
            PartitionFilterUtils.evaluatePredicate(
              modelMetadata.schemaString,
              modelMetadata.partitionColumns,
              predicateHits,
              selectedFiles
            )
          } else {
            selectedFiles
          }
        filteredFilters.map { addFile =>
          val cloudPath = absolutePath(deltaLog.dataPath, addFile.path)
          val signedUrl = fileSigner.sign(cloudPath)
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
    }
    snapshot.version -> actions
  }

  def update(): Unit = withClassLoader {
    deltaLog.update()
  }

  private def cleanUpTableSchema(schemaString: String): String = {
    StructType(DataType.fromJson(schemaString).asInstanceOf[StructType].map { field =>
      val newMetadata = new MetadataBuilder()
      // Only keep the column comment
      if (field.metadata.contains("comment")) {
        newMetadata.putString("comment", field.metadata.getString("comment"))
      }
      field.copy(metadata = newMetadata.build())
    }).json
  }

  private def absolutePath(path: Path, child: String): Path = {
    val p = new Path(new URI(child))
    if (p.isAbsolute) {
      throw new IllegalStateException("table containing absolute paths cannot be shared")
    } else {
      new Path(path, p)
    }
  }
}
