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
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

import io.delta.sharing.server.{model, AbfsFileSigner, CausedBy, DeltaSharingIllegalArgumentException, DeltaSharingUnsupportedOperationException, ErrorStrings, GCSFileSigner, PreSignedUrl, S3FileSigner, WasbFileSigner}
import io.delta.sharing.server.config.{ServerConfig, TableConfig}
import io.delta.sharing.server.protocol.{QueryTablePageToken, RefreshToken}
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
              serverConfig.evaluateJsonPredicateHintsV2,
              serverConfig.queryTablePageSizeLimit,
              serverConfig.queryTablePageTokenTtlMs,
              serverConfig.refreshTokenTtlMs
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
 *  QueryResult of query and queryCDF function, including a version, a resopnseFormat, and a list
 *  of actions.
 */
case class QueryResult(
    version: Long,
    actions: Seq[Object],
    responseFormat: String)

/**
 * A table class that wraps `DeltaLog` to provide the methods used by the server.
 */
class DeltaSharedTable(
    tableConfig: TableConfig,
    preSignedUrlTimeoutSeconds: Long,
    evaluatePredicateHints: Boolean,
    evaluateJsonPredicateHints: Boolean,
    evaluateJsonPredicateHintsV2: Boolean,
    queryTablePageSizeLimit: Int,
    queryTablePageTokenTtlMs: Int,
    refreshTokenTtlMs: Int) {

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

  // Construct the protocol class to be returned in the response based on the responseFormat.
  private def getResponseProtocol(p: Protocol, responseFormat: String): Object = {
    if (responseFormat == DeltaSharedTable.RESPONSE_FORMAT_DELTA) {
      DeltaResponseProtocol(deltaProtocol = p).wrap
    } else {
      model.Protocol(p.minReaderVersion).wrap
    }
  }

  // Construct the metadata class to be returned in the response based on the responseFormat.
  private def getResponseMetadata(
      m: Metadata,
      startingVersion: Option[Long],
      responseFormat: String
  ): Object = {
    if (responseFormat == DeltaSharedTable.RESPONSE_FORMAT_DELTA) {
      DeltaResponseMetadata(
        version = if (startingVersion.isDefined) {
          startingVersion.get
        } else {
          null
        },
        deltaMetadata = DeltaMetadataCopy(m)
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

  // Construct the returning class for addFile based on requested responseFormat.
  private def getResponseAddFile(
      addFile: AddFile,
      signedUrl: PreSignedUrl,
      version: java.lang.Long,
      timestamp: java.lang.Long,
      responseFormat: String,
      returnAddFileForCDF: Boolean = false): Object = {
    if (responseFormat == DeltaSharedTable.RESPONSE_FORMAT_DELTA) {
      DeltaResponseFileAction(
        id = Hashing.md5().hashString(addFile.path, UTF_8).toString,
        expirationTimestamp = signedUrl.expirationTimestamp,
        version = version,
        timestamp = timestamp,
        deltaSingleAction = addFile.copy(path = signedUrl.url).wrap
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
      DeltaResponseFileAction(
        id = Hashing.md5().hashString(removeFile.path, UTF_8).toString,
        expirationTimestamp = signedUrl.expirationTimestamp,
        version = version,
        timestamp = timestamp,
        deltaSingleAction = removeFile.copy(path = signedUrl.url).wrap
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
      DeltaResponseFileAction(
        id = Hashing.md5().hashString(addCDCFile.path, UTF_8).toString,
        expirationTimestamp = signedUrl.expirationTimestamp,
        version = version,
        timestamp = timestamp,
        deltaSingleAction = addCDCFile.copy(path = signedUrl.url).wrap
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

  // Construct and return the end action of the streaming response.
  private def getEndStreamAction(
      nextPageTokenStr: String,
      minUrlExpirationTimestamp: Long,
      refreshTokenStr: String = null): model.SingleAction = {
    model.EndStreamAction(
      refreshTokenStr,
      nextPageTokenStr,
      if (minUrlExpirationTimestamp == Long.MaxValue) null else minUrlExpirationTimestamp
    ).wrap
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
      includeRefreshToken: Boolean,
      refreshToken: Option[String],
      responseFormatSet: Set[String]): QueryResult = withClassLoader {
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
    // Validate refreshToken if it's specified
    val refreshTokenOpt = refreshToken.map(decodeAndValidateRefreshToken)
    // The version of the snapshot should follow the below precedence:
    // 1. Use version specified in the pageToken, which is equal to the version we use in the
    //    first page request. This is to make sure that responses are consistent across pages.
    // 2. Use version/timestamp/startingVersion specified by the user.
    // 3. Use version specified in the refreshToken, which is equal to latest table version upon
    //    initial request. In this case, it must be a latest snapshot query and version/timestamp/
    //    startingVersion must not be specified.
    val specifiedVersion = pageTokenOpt.flatMap(_.version)
      .orElse(version)
      .orElse(startingVersion)
      .orElse(refreshTokenOpt.map(_.getVersion))
    val snapshot =
      if (specifiedVersion.isDefined) {
        try {
          deltaLog.getSnapshotForVersionAsOf(specifiedVersion.get)
        } catch {
          case e: io.delta.standalone.exceptions.DeltaStandaloneException =>
            throw new DeltaSharingIllegalArgumentException(e.getMessage)
        }
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
    // If the client accept parquet format and it's a basic table, return as parquet format.
    val responseFormat = if (snapshot.protocolScala.minReaderVersion == 1 &&
      responseFormatSet.contains(DeltaSharedTable.RESPONSE_FORMAT_PARQUET)) {
      DeltaSharedTable.RESPONSE_FORMAT_PARQUET
    } else {
      DeltaSharedTable.RESPONSE_FORMAT_DELTA
    }
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
        var minUrlExpirationTimestamp = Long.MaxValue

        // Skip files that are already processed in previous pages
        val selectedIndexedFiles = state.activeFiles.toSeq.zipWithIndex
          .drop(pageTokenOpt.map(_.getStartingActionIndex).getOrElse(0))

        // Select files that satisfy predicate hints
        val useJsonPredicateHints =
          (evaluateJsonPredicateHints && snapshot.metadataScala.partitionColumns.nonEmpty) ||
          evaluateJsonPredicateHintsV2
        var filteredIndexedFiles =
          if (useJsonPredicateHints) {
            JsonPredicateFilterUtils.evaluatePredicate(
              jsonPredicateHints,
              evaluateJsonPredicateHintsV2,
              selectedIndexedFiles
            )
          } else {
            selectedIndexedFiles
          }

        // Select files that satisfy partition hints
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
          nextPageTokenStr = DeltaSharedTable.encodeToken(
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
            minUrlExpirationTimestamp = minUrlExpirationTimestamp.min(signedUrl.expirationTimestamp)
            getResponseAddFile(
              addFile,
              signedUrl,
              if (isVersionQuery) { snapshot.version } else null,
              if (isVersionQuery) { ts.get } else null,
              responseFormat
            )
        }
        val refreshTokenStr = if (includeRefreshToken) {
          DeltaSharedTable.encodeToken(
            RefreshToken(
              id = Some(tableConfig.id),
              version = Some(snapshot.version),
              expirationTimestamp = Some(System.currentTimeMillis() + refreshTokenTtlMs)
            )
          )
        } else {
          null
        }
        // For backwards compatibility, return an `endStreamAction` object only when
        // `includeRefreshToken` is true or `maxFiles` is specified
        filteredFiles ++ {
          if (includeRefreshToken || maxFiles.isDefined) {
            Seq(getEndStreamAction(nextPageTokenStr, minUrlExpirationTimestamp, refreshTokenStr))
          } else {
            Nil
          }
        }
      } else {
        Nil
      }
    }

    QueryResult(snapshot.version, actions, responseFormat)
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
    // We use (start, end) from the page token instead of the original request because:
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
      DeltaSharedTable.encodeToken(
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
    }
    var minUrlExpirationTimestamp = Long.MaxValue
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
              actions.append(getEndStreamAction(tokenGenerator(v, idx), minUrlExpirationTimestamp))
              return actions.toSeq
            }
            val preSignedUrl = fileSigner.sign(absolutePath(deltaLog.dataPath, a.path))
            minUrlExpirationTimestamp =
              minUrlExpirationTimestamp.min(preSignedUrl.expirationTimestamp)
            actions.append(
              getResponseAddFile(
                a,
                preSignedUrl,
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
              actions.append(getEndStreamAction(tokenGenerator(v, idx), minUrlExpirationTimestamp))
              return actions.toSeq
            }
            val preSignedUrl = fileSigner.sign(absolutePath(deltaLog.dataPath, r.path))
            minUrlExpirationTimestamp =
              minUrlExpirationTimestamp.min(preSignedUrl.expirationTimestamp)
            actions.append(
              getResponseRemoveFile(
                r,
                preSignedUrl,
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
    // Return an `endStreamAction` object only when `maxFiles` is specified for
    // backwards compatibility.
    if (maxFilesOpt.isDefined) {
      actions.append(getEndStreamAction(null, minUrlExpirationTimestamp))
    }
    actions.toSeq
  }

  def queryCDF(
      cdfOptions: Map[String, String],
      includeHistoricalMetadata: Boolean = false,
      maxFiles: Option[Int],
      pageToken: Option[String],
      responseFormatSet: Set[String] = Set(DeltaSharedTable.RESPONSE_FORMAT_PARQUET)
  ): QueryResult = withClassLoader {
    // Step 1: validate pageToken if it's specified
    lazy val queryParamChecksum = computeChecksum(
      QueryParamChecksum(
        version = None,
        timestamp = None,
        startingVersion = cdfOptions.get(DeltaDataSource.CDF_START_VERSION_KEY).map(_.toLong),
        startingTimestamp = cdfOptions.get(DeltaDataSource.CDF_START_TIMESTAMP_KEY),
        endingVersion = cdfOptions.get(DeltaDataSource.CDF_END_VERSION_KEY).map(_.toLong),
        endingTimestamp = cdfOptions.get(DeltaDataSource.CDF_END_TIMESTAMP_KEY),
        predicateHints = Nil,
        jsonPredicateHints = None,
        limitHint = None,
        includeHistoricalMetadata = Some(includeHistoricalMetadata)
      )
    )
    val pageTokenOpt = pageToken.map(decodeAndValidatePageToken(_, queryParamChecksum))

    // Step 2: validate cdfOptions
    val cdcReader = new DeltaSharingCDCReader(deltaLog, conf)
    // For subsequent page calls, instead of using the current latestVersion, use latestVersion in
    // the pageToken (which is equal to the latestVersion when the first page call is received),
    // in case the latestVersion changes after the first page call.
    val latestVersion = pageTokenOpt.map(_.getLatestVersion).getOrElse(tableVersion)
    val (start, end) = cdcReader.validateCdfOptions(
      cdfOptions, latestVersion, tableConfig.startVersion)

    // Step 3: get Protocol and Metadata
    val snapshot = if (includeHistoricalMetadata) {
      deltaLog.getSnapshotForVersionAsOf(start)
    } else {
      deltaLog.getSnapshotForVersionAsOf(latestVersion)
    }
    val actions = ListBuffer[Object]()
    // If the client accept parquet format and it's a basic table, return as parquet format.
    val responseFormat = if (snapshot.protocolScala.minReaderVersion == 1 &&
      responseFormatSet.contains(DeltaSharedTable.RESPONSE_FORMAT_PARQUET)) {
      DeltaSharedTable.RESPONSE_FORMAT_PARQUET
    } else {
      DeltaSharedTable.RESPONSE_FORMAT_DELTA
    }
    actions.append(getResponseProtocol(snapshot.protocolScala, responseFormat))
    actions.append(
      getResponseMetadata(
        snapshot.metadataScala,
        Some(snapshot.version),
        responseFormat
      )
    )

    // Step 4: get files
    // Enforce page size only when `maxFiles` is specified for backwards compatibility.
    val pageSizeOpt = maxFiles.map(_.min(queryTablePageSizeLimit))
    val tokenGenerator = { (v: Long, idx: Int) =>
      DeltaSharedTable.encodeToken(
        QueryTablePageToken(
          id = Some(tableConfig.id),
          startingVersion = Some(v),
          endingVersion = Some(pageTokenOpt.map(_.getEndingVersion).getOrElse(end)),
          latestVersion = Some(latestVersion),
          checksum = Some(queryParamChecksum),
          startingActionIndex = Some(idx),
          expirationTimestamp = Some(System.currentTimeMillis() + queryTablePageTokenTtlMs)
        )
      )
    }
    var minUrlExpirationTimestamp = Long.MaxValue
    var numSignedFiles = 0
    // We use (start, end) from the page token instead of the original request because:
    // - Versions that are processed in previous pages can be skipped.
    // - Versions that are committed after the first page call should be ignored, especially
    //   when the endingVersion is not specified and resolved to latestVersion.
    val changes = cdcReader.queryCDF(
      pageTokenOpt.map(_.getStartingVersion).getOrElse(start),
      pageTokenOpt.map(_.getEndingVersion).getOrElse(end).min(latestVersion),
      latestVersion,
      includeHistoricalMetadata
    )
    changes.foreach { cdcDataSpec =>
      val v = cdcDataSpec.version
      val ts = cdcDataSpec.timestamp
      var indexedActions = cdcDataSpec.actions.zipWithIndex
      if (pageTokenOpt.exists(_.getStartingVersion == v)) {
        // Skip actions that are already processed in previous pages
        indexedActions = indexedActions.drop(pageTokenOpt.get.getStartingActionIndex)
      }
      indexedActions.foreach {
        case (m: Metadata, _) =>
          actions.append(
            getResponseMetadata(
              m,
              Some(v),
              responseFormat
            )
          )
        case (c: AddCDCFile, idx) =>
          // Return early if we already have enough files in the current page
          if (pageSizeOpt.contains(numSignedFiles)) {
            actions.append(getEndStreamAction(tokenGenerator(v, idx), minUrlExpirationTimestamp))
            return QueryResult(start, actions.toSeq, responseFormat)
          }
          val preSignedUrl = fileSigner.sign(absolutePath(deltaLog.dataPath, c.path))
          minUrlExpirationTimestamp =
            minUrlExpirationTimestamp.min(preSignedUrl.expirationTimestamp)
          actions.append(
            getResponseAddCDCFile(
              c,
              preSignedUrl,
              v,
              ts.getTime,
              responseFormat
            )
          )
          numSignedFiles += 1
        case (a: AddFile, idx) =>
          // Return early if we already have enough files in the current page
          if (pageSizeOpt.contains(numSignedFiles)) {
            actions.append(getEndStreamAction(tokenGenerator(v, idx), minUrlExpirationTimestamp))
            return QueryResult(start, actions.toSeq, responseFormat)
          }
          val preSignedUrl = fileSigner.sign(absolutePath(deltaLog.dataPath, a.path))
          minUrlExpirationTimestamp =
            minUrlExpirationTimestamp.min(preSignedUrl.expirationTimestamp)
          actions.append(
            getResponseAddFile(
              a,
              preSignedUrl,
              v,
              ts.getTime,
              responseFormat,
              returnAddFileForCDF = true
            )
          )
          numSignedFiles += 1
        case (r: RemoveFile, idx) =>
          // Return early if we already have enough files in the current page
          if (pageSizeOpt.contains(numSignedFiles)) {
            actions.append(getEndStreamAction(tokenGenerator(v, idx), minUrlExpirationTimestamp))
            return QueryResult(start, actions.toSeq, responseFormat)
          }
          val preSignedUrl = fileSigner.sign(absolutePath(deltaLog.dataPath, r.path))
          minUrlExpirationTimestamp =
            minUrlExpirationTimestamp.min(preSignedUrl.expirationTimestamp)
          actions.append(
            getResponseRemoveFile(
              r,
              preSignedUrl,
              v,
              ts.getTime,
              responseFormat
            )
          )
          numSignedFiles += 1
        case _ => ()
      }
    }
    // Return an `endStreamAction` object only when `maxFiles` is specified for
    // backwards compatibility.
    if (maxFiles.isDefined) {
      actions.append(getEndStreamAction(null, minUrlExpirationTimestamp))
    }
    QueryResult(start, actions.toSeq, responseFormat)
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
    if (tableConfig.historyShared &&
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
    val token = try {
      DeltaSharedTable.decodeToken[QueryTablePageToken](tokenStr)
    } catch {
      case NonFatal(_) =>
        throw new DeltaSharingIllegalArgumentException(
          s"Error decoding the page token: $tokenStr."
        )
    }
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

  private def decodeAndValidateRefreshToken(tokenStr: String): RefreshToken = {
    val token = try {
      DeltaSharedTable.decodeToken[RefreshToken](tokenStr)
    } catch {
      case NonFatal(_) =>
        throw new DeltaSharingIllegalArgumentException(
          s"Error decoding refresh token: $tokenStr."
        )
    }
    if (token.getExpirationTimestamp < System.currentTimeMillis()) {
      throw new DeltaSharingIllegalArgumentException(
        "The refresh token has expired. Please restart the query."
      )
    }
    if (token.getId != tableConfig.id) {
      throw new DeltaSharingIllegalArgumentException(
        "The table specified in the refresh token does not match the table being queried."
      )
    }
    token
  }
}

object DeltaSharedTable {
  val RESPONSE_FORMAT_PARQUET = "parquet"
  val RESPONSE_FORMAT_DELTA = "delta"

  private def encodeToken[T <: GeneratedMessage](token: T): String = {
    Base64.getUrlEncoder.encodeToString(token.toByteArray)
  }

  private def decodeToken[T <: GeneratedMessage](tokenStr: String)(
    implicit protoCompanion: GeneratedMessageCompanion[T]): T = {
    protoCompanion.parseFrom(Base64.getUrlDecoder.decode(tokenStr))
  }
}
