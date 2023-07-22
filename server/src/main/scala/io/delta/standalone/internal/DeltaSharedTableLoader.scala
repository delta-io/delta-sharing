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
import java.util.Base64
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
import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.azure.NativeAzureFileSystem
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem
import org.apache.hadoop.fs.s3a.S3AFileSystem
import org.apache.spark.sql.types.{DataType, MetadataBuilder, StructType}
import scala.collection.mutable.ListBuffer
import scala.util.control.NonFatal

import io.delta.sharing.server.{
  model,
  AbfsFileSigner,
  CausedBy,
  DeltaSharingIllegalArgumentException,
  DeltaSharingUnsupportedOperationException,
  ErrorStrings,
  GCSFileSigner,
  PreSignedUrl,
  S3FileSigner,
  WasbFileSigner
}
import io.delta.sharing.server.config.{ServerConfig, TableConfig}
import io.delta.sharing.server.protocol.QueryTablePageToken
import io.delta.sharing.server.util.JsonUtils

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
        deltaSharedTableCache.get(
          tableConfig.location,
          () => {
            new DeltaSharedTable(
              tableConfig,
              serverConfig.preSignedUrlTimeoutSeconds,
              serverConfig.evaluatePredicateHints,
              serverConfig.evaluateJsonPredicateHints,
              serverConfig.queryTablePageSizeLimit,
              serverConfig.queryTablePageTokenTtlMs
            )
          }
        )
      if (!serverConfig.stalenessAcceptable) {
        deltaSharedTable.update()
      }
      deltaSharedTable
    } catch {
      case CausedBy(e: DeltaSharingUnsupportedOperationException) => throw e
      case e: Throwable => throw e
    }
  }
}

/**
 * A util class stores all query parameters. Used to compute the checksum in the page token for
 * query validation.
 */
private case class QueryParamChecksum(
    version: Option[Long],
    timestamp: Option[String],
    startingVersion: Option[Long],
    startingTimestamp: Option[String],
    endingVersion: Option[Long],
    endingTimestamp: Option[String],
    predicateHints: Seq[String],
    jsonPredicateHints: Option[String],
    limitHint: Option[Long],
    includeHistoricalMetadata: Option[Boolean])

/**
 * A table class that wraps `DeltaLog` to provide the methods used by the server.
 */
class DeltaSharedTable(
    tableConfig: TableConfig,
    preSignedUrlTimeoutSeconds: Long,
    evaluatePredicateHints: Boolean,
    evaluateJsonPredicateHints: Boolean,
    queryTablePageSizeLimit: Int,
    queryTablePageTokenTtlMs: Int) {

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

  // Construct and return the protocol class to be returned in the response based on the
  // responseFormat.
  private def getResponseProtocol(p: Protocol, responseFormat: String): Object = {
    if (responseFormat == DeltaSharedTable.RESPONSE_FORMAT_DELTA) {
      model.DeltaProtocol(p.minReaderVersion).wrap
    } else {
      model.Protocol(p.minReaderVersion).wrap
    }
  }

  // Construct and return the metadata class to be returned in the response based on the
  // responseFormat.
  private def getResponseMetadata(
      m: Metadata,
      startingVersion: Option[Long],
      responseFormat: String
  ): Object = {
    if (responseFormat == DeltaSharedTable.RESPONSE_FORMAT_DELTA) {
      model.DeltaMetadata(
        id = m.id,
        name = m.name,
        description = m.description,
        format = model.Format(),
        schemaString = cleanUpTableSchema(m.schemaString),
        partitionColumns = m.partitionColumns,
        configuration = m.configuration,
        version = if (startingVersion.isDefined) {
          startingVersion.get
        } else {
          null
        },
        createdTime = m.createdTime
      ).wrap
    } else {
      model.Metadata(
        id = m.id,
        name = m.name,
        description = m.description,
        format = model.Format(),
        schemaString = cleanUpTableSchema(m.schemaString),
        configuration = getMetadataConfiguration(m.configuration),
        partitionColumns = m.partitionColumns,
        version = if (startingVersion.isDefined) {
          startingVersion.get
        } else {
          null
        }
      ).wrap
    }
  }

  // Construct and return the AddFile class to be returned in the response based on the
  // responseFormat.
  private def getResponseAddFile(
      addFile: AddFile,
      signedUrl: PreSignedUrl,
      version: java.lang.Long,
      timestamp: java.lang.Long,
      responseFormat: String,
      returnAddFileForCDF: Boolean = false): Object = {
    if (responseFormat == DeltaSharedTable.RESPONSE_FORMAT_DELTA) {
      model.DeltaAddFile(
        path = signedUrl.url,
        id = Hashing.md5().hashString(addFile.path, UTF_8).toString,
        expirationTimestamp = signedUrl.expirationTimestamp,
        partitionValues = addFile.partitionValues,
        size = addFile.size,
        modificationTime = addFile.modificationTime,
        dataChange = addFile.dataChange,
        stats = addFile.stats,
        version = version,
        timestamp = timestamp
      ).wrap
    } else if (returnAddFileForCDF) {
      model.AddFileForCDF(
        url = signedUrl.url,
        id = Hashing.md5().hashString(addFile.path, UTF_8).toString,
        expirationTimestamp = signedUrl.expirationTimestamp,
        partitionValues = addFile.partitionValues,
        size = addFile.size,
        stats = addFile.stats,
        version = version,
        timestamp = timestamp
      ).wrap
    } else {
      model.AddFile(
        url = signedUrl.url,
        id = Hashing.md5().hashString(addFile.path, UTF_8).toString,
        expirationTimestamp = signedUrl.expirationTimestamp,
        partitionValues = addFile.partitionValues,
        size = addFile.size,
        stats = addFile.stats,
        version = version,
        timestamp = timestamp
      ).wrap
    }
  }

  // Construct and return the RemoveFile class to be returned in the response based on the
  // responseFormat.
  private def getResponseRemoveFile(
    removeFile: RemoveFile,
    signedUrl: PreSignedUrl,
    version: java.lang.Long,
    timestamp: java.lang.Long,
    responseFormat: String): Object = {
    if (responseFormat == DeltaSharedTable.RESPONSE_FORMAT_DELTA) {
      model.DeltaRemoveFile(
        path = signedUrl.url,
        id = Hashing.md5().hashString(removeFile.path, UTF_8).toString,
        expirationTimestamp = signedUrl.expirationTimestamp,
        deletionTimestamp = removeFile.deletionTimestamp,
        dataChange = removeFile.dataChange,
        extendedFileMetadata = removeFile.extendedFileMetadata,
        partitionValues = removeFile.partitionValues,
        size = removeFile.size,
        version = version,
        timestamp = timestamp
      ).wrap
    } else {
      model.RemoveFile(
        url = signedUrl.url,
        id = Hashing.md5().hashString(removeFile.path, UTF_8).toString,
        expirationTimestamp = signedUrl.expirationTimestamp,
        partitionValues = removeFile.partitionValues,
        size = removeFile.size.get,
        version = version,
        timestamp = timestamp
      ).wrap
    }
  }

  // Construct and return the AddCDCFile class to be returned in the response based on the
  // responseFormat.
  private def getResponseAddCDCFile(
    addCDCFile: AddCDCFile,
    signedUrl: PreSignedUrl,
    version: java.lang.Long,
    timestamp: java.lang.Long,
    responseFormat: String
  ): Object = {
    if (responseFormat == DeltaSharedTable.RESPONSE_FORMAT_DELTA) {
      model.DeltaAddCDCFile(
        path = signedUrl.url,
        id = Hashing.md5().hashString(addCDCFile.path, UTF_8).toString,
        expirationTimestamp = signedUrl.expirationTimestamp,
        partitionValues = addCDCFile.partitionValues,
        size = addCDCFile.size,
        version = version,
        timestamp = timestamp
      ).wrap
    } else {
      model.AddCDCFile(
        url = signedUrl.url,
        id = Hashing.md5().hashString(addCDCFile.path, UTF_8).toString,
        expirationTimestamp = signedUrl.expirationTimestamp,
        partitionValues = addCDCFile.partitionValues,
        size = addCDCFile.size,
        version = version,
        timestamp = timestamp
      ).wrap
    }
  }

  // scalastyle:off argcount
  def query(
      includeFiles: Boolean,
      predicateHints: Seq[String],
      jsonPredicateHints: Option[String],
      limitHint: Option[Long],
      version: Option[Long],
      timestamp: Option[String],
      startingVersion: Option[Long],
      endingVersion: Option[Long],
      maxFiles: Option[Int],
      pageToken: Option[String],
      responseFormat: String): (Long, Seq[Object]) = withClassLoader {
    // scalastyle:on argcount
    // TODO Support `limitHint`
    if (Seq(version, timestamp, startingVersion).filter(_.isDefined).size >= 2) {
      throw new DeltaSharingIllegalArgumentException(
        ErrorStrings.multipleParametersSetErrorMsg(Seq("version", "timestamp", "startingVersion"))
      )
    }
    // Validate pageToken if it's specified
    lazy val queryParamChecksum = computeChecksum(
      QueryParamChecksum(
        version = version,
        timestamp = timestamp,
        startingVersion = startingVersion,
        startingTimestamp = None,
        endingVersion = endingVersion,
        endingTimestamp = None,
        predicateHints = predicateHints,
        jsonPredicateHints = jsonPredicateHints,
        limitHint = limitHint,
        includeHistoricalMetadata = None
      )
    )
    val pageTokenOpt = pageToken.map(decodeAndValidatePageToken(_, queryParamChecksum))
    // For queryTable at snapshot, override version in subsequent page calls using the version
    // in the pageToken to make sure we're querying the same version across pages. Especially
    // when the first page is querying the latest snapshot, table changes that are committed
    // after the first page call should be ignored.
    val versionFromPageToken = pageTokenOpt.flatMap(_.version)
    val snapshot =
      if (versionFromPageToken.orElse(version).orElse(startingVersion).isDefined) {
        deltaLog.getSnapshotForVersionAsOf(
          versionFromPageToken.orElse(version).orElse(startingVersion).get
        )
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

    val isVersionQuery = !Seq(version, timestamp).filter(_.isDefined).isEmpty
    val actions = Seq(
      getResponseProtocol(snapshot.protocolScala, responseFormat),
      getResponseMetadata(snapshot.metadataScala, startingVersion, responseFormat)
    ) ++ {
      if (startingVersion.isDefined) {
        // Only read changes up to snapshot.version, and ignore changes that are committed during
        // queryDataChangeSinceStartVersion.
        queryDataChangeSinceStartVersion(
          startingVersion.get,
          endingVersion,
          maxFiles,
          pageTokenOpt,
          queryParamChecksum,
          responseFormat
        )
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
        // Enforce page size only when `maxFiles` is specified for backwards compatibility.
        val pageSizeOpt = maxFiles.map(_.min(queryTablePageSizeLimit))
        var nextPageTokenStr: String = null

        // Skip files that are already processed in previous pages
        val selectedIndexedFiles = state.activeFiles.toSeq.zipWithIndex
          .drop(pageTokenOpt.map(_.getStartingActionIndex).getOrElse(0))
        // Select files that satisfy partition and predicate hints
        var filteredIndexedFiles =
          if (evaluateJsonPredicateHints && snapshot.metadataScala.partitionColumns.nonEmpty) {
            JsonPredicateFilterUtils.evaluatePredicate(jsonPredicateHints, selectedIndexedFiles)
          } else {
            selectedIndexedFiles
          }
        filteredIndexedFiles =
          if (evaluatePredicateHints && snapshot.metadataScala.partitionColumns.nonEmpty) {
            PartitionFilterUtils.evaluatePredicate(
              snapshot.metadataScala.schemaString,
              snapshot.metadataScala.partitionColumns,
              predicateHints,
              filteredIndexedFiles
            )
          } else {
            filteredIndexedFiles
          }
        // If number of valid files is greater than page size, generate nextPageToken and
        // drop additional files.
        if (pageSizeOpt.exists(_ < filteredIndexedFiles.length)) {
          nextPageTokenStr = encodeQueryTablePageToken(
            QueryTablePageToken(
              id = Some(tableConfig.id),
              version = Some(snapshot.version),
              checksum = Some(queryParamChecksum),
              startingActionIndex = Some(filteredIndexedFiles(pageSizeOpt.get)._2),
              expirationTimestamp = Some(System.currentTimeMillis() + queryTablePageTokenTtlMs)
            )
          )
          filteredIndexedFiles = filteredIndexedFiles.take(pageSizeOpt.get)
        }
        val filteredFiles = filteredIndexedFiles.map {
          case (addFile, _) =>
            val cloudPath = absolutePath(deltaLog.dataPath, addFile.path)
            val signedUrl = fileSigner.sign(cloudPath)
            getResponseAddFile(
              addFile,
              signedUrl,
              if (isVersionQuery) { snapshot.version } else null,
              if (isVersionQuery) { ts.get } else null,
              responseFormat
            )
        }
        // Return `nextPageToken` object only when `maxFiles` is specified for backwards
        // compatibility. If this is the last page, an empty `nextPageToken` object will
        // be returned to explicitly indicate that there are no more pages.
        filteredFiles ++ {
          if (maxFiles.isDefined) {
            Seq(model.NextPageToken(nextPageTokenStr).wrap)
          } else {
            Nil
          }
        }
      } else {
        Nil
      }
    }

    snapshot.version -> actions
  }

  private def queryDataChangeSinceStartVersion(
      startingVersion: Long,
      endingVersion: Option[Long],
      maxFilesOpt: Option[Int],
      pageTokenOpt: Option[QueryTablePageToken],
      queryParamChecksum: String,
      responseFormat: String
    ): Seq[Object] = {
    // For subsequent page calls, instead of using the current latestVersion, use latestVersion in
    // the pageToken (which is equal to the latestVersion when the first page call is received),
    // in case the latestVersion changes after the first page call.
    val latestVersion = pageTokenOpt.map(_.getLatestVersion).getOrElse(tableVersion)
    if (startingVersion > latestVersion) {
      throw DeltaCDFErrors.startVersionAfterLatestVersion(startingVersion, latestVersion)
    }
    if (endingVersion.isDefined && endingVersion.get > latestVersion) {
      throw DeltaCDFErrors.endVersionAfterLatestVersion(endingVersion.get, latestVersion)
    }
    // We override (start, end) in subsequent page calls because:
    // - Versions that are processed in previous pages can be skipped.
    // - Versions that are committed after the first page call should be ignored, especially
    //   when the endingVersion is not specified and resolved to latestVersion.
    val start = pageTokenOpt.map(_.getStartingVersion).getOrElse(startingVersion)
    val end = pageTokenOpt
      .map(_.getEndingVersion)
      .orElse(endingVersion)
      .getOrElse(latestVersion)
      .min(latestVersion)
    val timestampsByVersion = DeltaSharingHistoryManager.getTimestampsByVersion(
      deltaLog.store,
      deltaLog.logPath,
      start,
      end + 1,
      conf
    )

    // Enforce page size only when `maxFiles` is specified for backwards compatibility.
    val pageSizeOpt = maxFilesOpt.map(_.min(queryTablePageSizeLimit))
    val tokenGenerator = { (v: Long, idx: Int) =>
      val nextPageTokenStr = encodeQueryTablePageToken(
        QueryTablePageToken(
          id = Some(tableConfig.id),
          startingVersion = Some(v),
          endingVersion = Some(end),
          latestVersion = Some(latestVersion),
          checksum = Some(queryParamChecksum),
          startingActionIndex = Some(idx),
          expirationTimestamp = Some(System.currentTimeMillis() + queryTablePageTokenTtlMs)
        )
      )
      model.NextPageToken(nextPageTokenStr).wrap
    }
    var numSignedFiles = 0
    val actions = ListBuffer[Object]()
    deltaLog
      .getChanges(start, true)
      .asScala
      .toSeq
      .filter(_.getVersion <= end)
      .foreach { versionLog =>
        val v = versionLog.getVersion
        var indexedVersionActions =
          versionLog.getActions.asScala.map(x => ConversionUtils.convertActionJ(x)).zipWithIndex
        val ts = timestampsByVersion.get(v).orNull
        if (pageTokenOpt.exists(_.getStartingVersion == v)) {
          // Skip actions that are already processed in previous pages
          indexedVersionActions =
            indexedVersionActions.drop(pageTokenOpt.get.getStartingActionIndex)
        }
        indexedVersionActions.foreach {
          case (a: AddFile, idx) if a.dataChange =>
            // Return early if we already have enough files in the current page
            if (pageSizeOpt.contains(numSignedFiles)) {
              actions.append(tokenGenerator(v, idx))
              return actions.toSeq
            }
            actions.append(
              getResponseAddFile(
                a,
                fileSigner.sign(absolutePath(deltaLog.dataPath, a.path)),
                v,
                ts.getTime,
                responseFormat,
                true
              )
            )
            numSignedFiles += 1
          case (r: RemoveFile, idx) if r.dataChange =>
            // Return early if we already have enough files in the current page
            if (pageSizeOpt.contains(numSignedFiles)) {
              actions.append(tokenGenerator(v, idx))
              return actions.toSeq
            }
            actions.append(
              getResponseRemoveFile(
                r,
                fileSigner.sign(absolutePath(deltaLog.dataPath, r.path)),
                v,
                ts.getTime,
                responseFormat
              )
            )
            numSignedFiles += 1
          case (p: Protocol, _) =>
            assertProtocolRead(p)
          case (m: Metadata, _) =>
            if (v > startingVersion) {
              actions.append(
                getResponseMetadata(
                  m,
                  Some(v),
                  responseFormat
                )
              )
            }
          case _ => ()
        }
      }
    // Return an empty `nextPageToken` object only when `maxFiles` is specified for
    // backwards compatibility.
    if (maxFilesOpt.isDefined) {
      actions.append(model.NextPageToken(null).wrap)
    }
    actions.toSeq
  }

  def queryCDF(
      cdfOptions: Map[String, String],
      includeHistoricalMetadata: Boolean = false,
      responseFormat: String = DeltaSharedTable.RESPONSE_FORMAT_PARQUET
  ): (Long, Seq[Object]) = withClassLoader {
    val actions = ListBuffer[Object]()

    // First: validate cdf options are greater than startVersion
    val cdcReader = new DeltaSharingCDCReader(deltaLog, conf)
    val latestVersion = tableVersion
    val (start, end) = cdcReader.validateCdfOptions(
      cdfOptions, latestVersion, tableConfig.startVersion)

    // Second: get Protocol and Metadata
    val snapshot = if (includeHistoricalMetadata) {
      deltaLog.getSnapshotForVersionAsOf(start)
    } else {
      deltaLog.snapshot
    }
    actions.append(getResponseProtocol(snapshot.protocolScala, responseFormat))
    actions.append(
      getResponseMetadata(
        snapshot.metadataScala,
        Some(snapshot.version),
        responseFormat
      )
    )

    // Third: get files
    val (changeFiles, addFiles, removeFiles, metadatas) = cdcReader.queryCDF(
      start, end, latestVersion, includeHistoricalMetadata)
    // If includeHistoricalMetadata is not true, metadatas will be empty.
    metadatas.foreach { cdcDataSpec =>
      cdcDataSpec.actions.foreach { action =>
        val metadata = action.asInstanceOf[Metadata]
        actions.append(
          getResponseMetadata(
            metadata,
            Some(cdcDataSpec.version),
            responseFormat
          )
        )
      }
    }
    changeFiles.foreach { cdcDataSpec =>
      cdcDataSpec.actions.foreach { action =>
        val addCDCFile = action.asInstanceOf[AddCDCFile]
        val cloudPath = absolutePath(deltaLog.dataPath, addCDCFile.path)
        val signedUrl = fileSigner.sign(cloudPath)
        actions.append(
          getResponseAddCDCFile(
            addCDCFile,
            signedUrl,
            cdcDataSpec.version,
            cdcDataSpec.timestamp.getTime,
            responseFormat
          )
        )
      }
    }
    addFiles.foreach { cdcDataSpec =>
      cdcDataSpec.actions.foreach { action =>
        val addFile = action.asInstanceOf[AddFile]
        val cloudPath = absolutePath(deltaLog.dataPath, addFile.path)
        val signedUrl = fileSigner.sign(cloudPath)
        actions.append(
          getResponseAddFile(
            addFile,
            signedUrl,
            cdcDataSpec.version,
            cdcDataSpec.timestamp.getTime,
            responseFormat,
            returnAddFileForCDF = true
          )
        )
      }
    }
    removeFiles.foreach { cdcDataSpec =>
      cdcDataSpec.actions.foreach { action =>
        val removeFile = action.asInstanceOf[RemoveFile]
        val cloudPath = absolutePath(deltaLog.dataPath, removeFile.path)
        val signedUrl = fileSigner.sign(cloudPath)
        actions.append(
          getResponseRemoveFile(
            removeFile,
            signedUrl,
            cdcDataSpec.version,
            cdcDataSpec.timestamp.getTime,
            responseFormat
          )
        )
      }
    }
    start -> actions.toSeq
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

  private def computeChecksum(queryParamChecksum: QueryParamChecksum): String = {
    DigestUtils.sha256Hex(JsonUtils.toJson(queryParamChecksum))
  }

  private def decodeAndValidatePageToken(
      tokenStr: String,
      expectedChecksum: String): QueryTablePageToken = {
    val token = decodeQueryTablePageToken(tokenStr)
    if (token.getExpirationTimestamp < System.currentTimeMillis()) {
      throw new DeltaSharingIllegalArgumentException(
        "The page token has expired. Please restart the query."
      )
    }
    if (token.getId != tableConfig.id) {
      throw new DeltaSharingIllegalArgumentException(
        "The table specified in the page token does not match the table being queried."
      )
    }
    if (token.getChecksum != expectedChecksum) {
      throw new DeltaSharingIllegalArgumentException(
        """Query parameter mismatch detected for the next page query. The query parameter
          |cannot change when querying the next page results.""".stripMargin
      )
    }
    token
  }

  private def encodeQueryTablePageToken(token: QueryTablePageToken): String = {
    Base64.getUrlEncoder.encodeToString(token.toByteArray)
  }

  private def decodeQueryTablePageToken(tokenStr: String): QueryTablePageToken = {
    try {
      QueryTablePageToken.parseFrom(Base64.getUrlDecoder.decode(tokenStr))
    } catch {
      case NonFatal(_) =>
        throw new DeltaSharingIllegalArgumentException(
          s"Error decoding the page token: $tokenStr."
        )
    }
  }
}

object DeltaSharedTable {
  val RESPONSE_FORMAT_PARQUET = "parquet"
  val RESPONSE_FORMAT_DELTA = "delta"
}
