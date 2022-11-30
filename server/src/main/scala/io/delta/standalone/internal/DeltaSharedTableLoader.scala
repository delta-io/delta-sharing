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

import scala.collection.JavaConverters._

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem
import com.google.common.cache.CacheBuilder
import com.google.common.hash.Hashing
import com.google.common.util.concurrent.UncheckedExecutionException
import io.delta.standalone.DeltaLog
import io.delta.standalone.internal.actions.{AddCDCFile, AddFile, Metadata, Protocol, RemoveFile}
import io.delta.standalone.internal.exception.DeltaErrors
import io.delta.standalone.internal.util.ConversionUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.azure.NativeAzureFileSystem
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem
import org.apache.hadoop.fs.s3a.S3AFileSystem
import org.apache.spark.sql.types.{DataType, MetadataBuilder, StructType}
import scala.collection.mutable.ListBuffer

import io.delta.sharing.server.{
  model,
  AbfsFileSigner,
  CausedBy,
  DeltaSharingIllegalArgumentException,
  DeltaSharingUnsupportedOperationException,
  ErrorStrings,
  GCSFileSigner,
  S3FileSigner,
  WasbFileSigner
}
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
    try {
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
    catch {
      case CausedBy(e: DeltaSharingUnsupportedOperationException) => throw e
      case e: Throwable => throw e
    }
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
    try {
      DeltaLog.forTable(conf, tablePath).asInstanceOf[DeltaLogImpl]
    } catch {
      // convert InvalidProtocolVersionException to client error(400)
      case e: DeltaErrors.InvalidProtocolVersionException =>
        throw new DeltaSharingUnsupportedOperationException(e.getMessage)
      case e: Throwable => throw e
    }
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

  /** Get table version at or after startingTimestamp if it's provided, otherwise return
   *  the latest table version.
   */
  def getTableVersion(startingTimestamp: Option[String]): Long = withClassLoader {
    if (startingTimestamp.isEmpty) {
      tableVersion
    } else {
      val ts = DeltaSharingHistoryManager.getTimestamp("startingTimestamp", startingTimestamp.get)
      // get a version at or after the provided timestamp, if the timestamp is early than version 0,
      // return 0.
      try {
        deltaLog.getVersionAtOrAfterTimestamp(ts.getTime())
      } catch {
        // Convert to DeltaSharingIllegalArgumentException to return 4xx instead of 5xx error code
        // Only convert known exceptions around timestamp too late or too early
        case e: IllegalArgumentException =>
          throw new DeltaSharingIllegalArgumentException(e.getMessage)
      }
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
      predicateHints: Seq[String],
      limitHint: Option[Long],
      version: Option[Long],
      timestamp: Option[String],
      startingVersion: Option[Long]): (Long, Seq[model.SingleAction]) = withClassLoader {
    // TODO Support `limitHint`
    if (Seq(version, timestamp, startingVersion).filter(_.isDefined).size >= 2) {
      throw new DeltaSharingIllegalArgumentException(
        ErrorStrings.multipleParametersSetErrorMsg(Seq("version", "timestamp", "startingVersion"))
      )
    }
    val snapshot = if (version.orElse(startingVersion).isDefined) {
      deltaLog.getSnapshotForVersionAsOf(version.orElse(startingVersion).get)
    } else if (timestamp.isDefined) {
      val ts = DeltaSharingHistoryManager.getTimestamp("timestamp", timestamp.get)
      try {
        deltaLog.getSnapshotForTimestampAsOf(ts.getTime())
      } catch {
        // Convert to DeltaSharingIllegalArgumentException to return 4xx instead of 5xx error code
        // Only convert known exceptions around timestamp too late or too early
        case e: IllegalArgumentException =>
          throw new DeltaSharingIllegalArgumentException(e.getMessage)
      }
    } else {
      deltaLog.snapshot
    }
    // TODO Open the `state` field in Delta Standalone library.
    val stateMethod = snapshot.getClass.getMethod("state")
    val state = stateMethod.invoke(snapshot).asInstanceOf[SnapshotImpl.State]
    val modelProtocol = model.Protocol(snapshot.protocolScala.minReaderVersion)
    val modelMetadata = model.Metadata(
      id = snapshot.metadataScala.id,
      name = snapshot.metadataScala.name,
      description = snapshot.metadataScala.description,
      format = model.Format(),
      schemaString = cleanUpTableSchema(snapshot.metadataScala.schemaString),
      configuration = getMetadataConfiguration(snapshot.metadataScala.configuration),
      partitionColumns = snapshot.metadataScala.partitionColumns,
      version = if (startingVersion.isDefined) {
        startingVersion.get
      } else {
        null
      }
    )

    val isVersionQuery = !Seq(version, timestamp).filter(_.isDefined).isEmpty
    val actions = Seq(modelProtocol.wrap, modelMetadata.wrap) ++ {
      if (startingVersion.isDefined) {
        // Only read changes up to snapshot.version, and ignore changes that are committed during
        // queryDataChangeSinceStartVersion.
        queryDataChangeSinceStartVersion(startingVersion.get)
      } else if (includeFiles) {
        val ts = if (isVersionQuery) {
          val timestampsByVersion = DeltaSharingHistoryManager.getTimestampsByVersion(
            deltaLog.store,
            deltaLog.logPath,
            snapshot.version,
            snapshot.version + 1,
            conf
          )
          Some(timestampsByVersion.get(snapshot.version).orNull.getTime)
        } else {
          None
        }

        val selectedFiles = state.activeFiles.toSeq
        val filteredFilters =
          if (evaluatePredicateHints && modelMetadata.partitionColumns.nonEmpty) {
            PartitionFilterUtils.evaluatePredicate(
              modelMetadata.schemaString,
              modelMetadata.partitionColumns,
              predicateHints,
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
            stats = addFile.stats,
            version = if (isVersionQuery) { snapshot.version } else null,
            timestamp = if (isVersionQuery) { ts.get} else null
          )
          modelAddFile.wrap
        }
      } else {
        Nil
      }
    }

    snapshot.version -> actions
  }

  private def queryDataChangeSinceStartVersion(startingVersion: Long): Seq[model.SingleAction] = {
    val latestVersion = tableVersion
    if (startingVersion > latestVersion) {
      throw DeltaCDFErrors.startVersionAfterLatestVersion(startingVersion, latestVersion)
    }
    val timestampsByVersion = DeltaSharingHistoryManager.getTimestampsByVersion(
      deltaLog.store,
      deltaLog.logPath,
      startingVersion,
      latestVersion + 1,
      conf
    )

    val actions = ListBuffer[model.SingleAction]()
    deltaLog.getChanges(startingVersion, true).asScala.toSeq.foreach{versionLog =>
      val v = versionLog.getVersion
      val versionActions = versionLog.getActions.asScala.map(x => ConversionUtils.convertActionJ(x))
      val ts = timestampsByVersion.get(v).orNull
      versionActions.foreach {
        case a: AddFile if a.dataChange =>
          val modelAddFile = model.AddFileForCDF(
            url = fileSigner.sign(absolutePath(deltaLog.dataPath, a.path)),
            id = Hashing.md5().hashString(a.path, UTF_8).toString,
            partitionValues = a.partitionValues,
            size = a.size,
            stats = a.stats,
            version = v,
            timestamp = ts.getTime
          )
          actions.append(modelAddFile.wrap)
        case r: RemoveFile if r.dataChange =>
          val modelRemoveFile = model.RemoveFile(
            url = fileSigner.sign(absolutePath(deltaLog.dataPath, r.path)),
            id = Hashing.md5().hashString(r.path, UTF_8).toString,
            partitionValues = r.partitionValues,
            size = r.size.get,
            version = v,
            timestamp = ts.getTime
          )
          actions.append(modelRemoveFile.wrap)
        case p: Protocol =>
          assertProtocolRead(p)
        case m: Metadata =>
          if (v > startingVersion) {
            val modelMetadata = model.Metadata(
              id = m.id,
              name = m.name,
              description = m.description,
              format = model.Format(),
              schemaString = cleanUpTableSchema(m.schemaString),
              configuration = getMetadataConfiguration(m.configuration),
              partitionColumns = m.partitionColumns,
              version = v
            )
            actions.append(modelMetadata.wrap)
          }
        case _ => ()
      }
    }
    actions.toSeq
  }

  def queryCDF(
      cdfOptions: Map[String, String],
      includeHistoricalMetadata: Boolean = false
  ): (Long, Seq[model.SingleAction]) = withClassLoader {
    val actions = ListBuffer[model.SingleAction]()

    // First: validate cdf options are greater than startVersion
    val cdcReader = new DeltaSharingCDCReader(deltaLog, conf)
    val latestVersion = tableVersion
    val (start, end) = cdcReader.validateCdfOptions(
      cdfOptions, latestVersion, tableConfig.startVersion)

    // Second: get Protocol and Metadata
    val snapshot = deltaLog.snapshot
    val modelProtocol = model.Protocol(snapshot.protocolScala.minReaderVersion)
    val modelMetadata = model.Metadata(
      id = snapshot.metadataScala.id,
      name = snapshot.metadataScala.name,
      description = snapshot.metadataScala.description,
      format = model.Format(),
      schemaString = cleanUpTableSchema(snapshot.metadataScala.schemaString),
      configuration = getMetadataConfiguration(snapshot.metadataScala.configuration),
      partitionColumns = snapshot.metadataScala.partitionColumns,
      version = latestVersion
    )
    actions.append(modelProtocol.wrap)
    actions.append(modelMetadata.wrap)

    // Third: get files
    val (changeFiles, addFiles, removeFiles, metadatas) = cdcReader.queryCDF(
      start, end, latestVersion, includeHistoricalMetadata)
    // If includeHistoricalMetadata is not true, metadatas will be empty.
    metadatas.foreach { cdcDataSpec =>
      cdcDataSpec.actions.foreach { action =>
        val metadata = action.asInstanceOf[Metadata]
        val modelMetadata = model.Metadata(
          id = metadata.id,
          name = metadata.name,
          description = metadata.description,
          format = model.Format(),
          schemaString = cleanUpTableSchema(metadata.schemaString),
          configuration = getMetadataConfiguration(metadata.configuration),
          partitionColumns = metadata.partitionColumns,
          version = cdcDataSpec.version
        )
        actions.append(modelMetadata.wrap)
      }
    }
    changeFiles.foreach { cdcDataSpec =>
      cdcDataSpec.actions.foreach { action =>
        val addCDCFile = action.asInstanceOf[AddCDCFile]
        val cloudPath = absolutePath(deltaLog.dataPath, addCDCFile.path)
        val signedUrl = fileSigner.sign(cloudPath)
        val modelCDCFile = model.AddCDCFile(
          url = signedUrl,
          id = Hashing.md5().hashString(addCDCFile.path, UTF_8).toString,
          partitionValues = addCDCFile.partitionValues,
          size = addCDCFile.size,
          version = cdcDataSpec.version,
          timestamp = cdcDataSpec.timestamp.getTime
        )
        actions.append(modelCDCFile.wrap)
      }
    }
    addFiles.foreach { cdcDataSpec =>
      cdcDataSpec.actions.foreach { action =>
        val addFile = action.asInstanceOf[AddFile]
        val cloudPath = absolutePath(deltaLog.dataPath, addFile.path)
        val signedUrl = fileSigner.sign(cloudPath)
        val modelAddFile = model.AddFileForCDF(
          url = signedUrl,
          id = Hashing.md5().hashString(addFile.path, UTF_8).toString,
          partitionValues = addFile.partitionValues,
          size = addFile.size,
          stats = addFile.stats,
          version = cdcDataSpec.version,
          timestamp = cdcDataSpec.timestamp.getTime
        )
        actions.append(modelAddFile.wrap)
      }
    }
    removeFiles.foreach { cdcDataSpec =>
      cdcDataSpec.actions.foreach { action =>
        val removeFile = action.asInstanceOf[RemoveFile]
        val cloudPath = absolutePath(deltaLog.dataPath, removeFile.path)
        val signedUrl = fileSigner.sign(cloudPath)
        val modelRemoveFile = model.RemoveFile(
          url = signedUrl,
          id = Hashing.md5().hashString(removeFile.path, UTF_8).toString,
          partitionValues = removeFile.partitionValues,
          size = removeFile.size.get,
          version = cdcDataSpec.version,
          timestamp = cdcDataSpec.timestamp.getTime
        )
        actions.append(modelRemoveFile.wrap)
      }
    }
    snapshot.version -> actions.toSeq
  }

  def update(): Unit = withClassLoader {
    deltaLog.update()
  }

  private def assertProtocolRead(protocol: Protocol): Unit = {
    if (protocol.minReaderVersion > model.Action.maxReaderVersion) {
      val e = new DeltaErrors.InvalidProtocolVersionException(Protocol(
        model.Action.maxReaderVersion, model.Action.maxWriterVersion), protocol)
      throw new DeltaSharingUnsupportedOperationException(e.getMessage)
    }
  }

  private def getMetadataConfiguration(tableConf: Map[String, String]): Map[String, String ] = {
    if (tableConfig.cdfEnabled &&
      tableConf.getOrElse("delta.enableChangeDataFeed", "false") == "true") {
      Map("enableChangeDataFeed" -> "true")
    } else {
      Map.empty
    }
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
