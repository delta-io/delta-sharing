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

package io.delta.sharing.spark

// scalastyle:off import.ordering.noEmptyLine
import java.lang.ref.WeakReference

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.delta.sharing.{CachedTableManager, TableRefreshResult}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, DeltaSharingScanUtils, SparkSession}
import org.apache.spark.sql.connector.read.streaming
import org.apache.spark.sql.connector.read.streaming.{ReadAllAvailable, ReadLimit, ReadMaxFiles, SupportsAdmissionControl}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.types.StructType

import io.delta.sharing.client.model.{AddCDCFile, AddFile, AddFileForCDF, DeltaTableFiles, FileAction, RemoveFile}
import io.delta.sharing.client.util.ConfUtils
import io.delta.sharing.spark.util.SchemaUtils

/**
 * A case class to help with `Dataset` operations regarding Offset indexing, representing a
 * FileAction in a Delta log.
 * For proper offset tracking(move the offset to the next version if all data is consumed in the
 * current version), there are also special sentinel values with index=-1 and getFileAction=null.
 *
 * This class is not designed to be persisted in offset logs or such.
 *
 * @param version The version of the Delta log containing this AddFile.
 * @param index The index of this FileAction in the Delta log in the version.
 * @param add The AddFileForCDF.
 * @param remove The RemoveFile.
 * @param cdc The AddCDCFile.
 * @param isLast A flag to indicate whether this is the last FileAction in the version. This is used
 *               to resolve an off-by-one issue in the streaming offset interface; once we've read
 *               to the end of a log version file, we check this flag to advance immediately to the
 *               next one in the persisted offset. Without this special case we would re-read the
 *               already completed log file from the delta sharing server.
 */
private[sharing] case class IndexedFile(
  version: Long,
  index: Long,
  add: AddFileForCDF,
  remove: RemoveFile = null,
  cdc: AddCDCFile = null,
  isSnapshot: Boolean = false,
  isLast: Boolean = false) {

  assert(Seq(add, remove, cdc).filter(_ != null).size <= 1, "There could be at most one non-null " +
    s"FileAction for an IndexedFile, add:$add, remove:$remove, cdc:$cdc.")

  def getFileAction: FileAction = {
    if (add != null) {
      add
    } else if (remove != null) {
      remove
    } else {
      cdc
    }
  }
}

/**
 * Base trait for the Delta Sharing Source, that contains methods that deal with
 * getting changes from the delta sharing server.
 */
/**
 * A streaming source for a Delta Sharing table.
 *
 * When a new stream is started, delta sharing starts by constructing a [[RemoteDeltaSnapshot]]
 * at the current version of the table. This snapshot is broken up into batches until
 * all existing data has been processed. Subsequent processing is done by tailing
 * the change log looking for new data. This results in the streaming query returning
 * the same answer as a batch query that had processed the entire dataset at any given point.
 */
case class DeltaSharingSource(
  spark: SparkSession,
  deltaLog: RemoteDeltaLog,
  options: DeltaSharingOptions) extends Source
  with SupportsAdmissionControl
  with Logging {

  // This is to ensure that the request sent from the client contains the http header for streaming.
  assert(deltaLog.client.getForStreaming,
    "forStreaming must be true for client in DeltaSharingSource.")

  // The snapshot that's used to construct the dataframe, constructed when source is initialized.
  // Use latest snapshot instead of snapshot at startingVersion, to allow easy recovery from
  // failures on schema incompatibility.
  private val initSnapshot: RemoteSnapshot = deltaLog.snapshot()

  override val schema: StructType = {
    val schemaWithoutCDC = initSnapshot.schema
    if (options.readChangeFeed) {
      DeltaTableUtils.addCdcSchema(schemaWithoutCDC)
    } else {
      schemaWithoutCDC
    }
  }

  // This is checked before creating DeltaSharingSource
  assert(schema.nonEmpty, "schema cannot be empty in DeltaSharingSource.")

  /** A check on the source table that skips commits that contain removes from the set of files. */
  private val skipChangeCommits = options.skipChangeCommits

  private val tableId = initSnapshot.metadata.id

  // Records until which offset the delta sharing source has been processing the table files.
  private var previousOffset: DeltaSharingSourceOffset = null

  // Serves as local cache to store all the files fetched from the delta sharing server.
  // If not empty, will advance the offset and fetch data from this list based on the read limit.
  // If empty, will try to load all possible new data files through delta sharing rpc to this list,
  //   sorted by version and id.
  // GLOBAL variable which should be protected by synchronized
  @volatile private var sortedFetchedFiles: Seq[IndexedFile] = Seq.empty
  // The latest timestamp in millisecond, records the time of the last rpc sent to the server to
  // fetch the pre-signed urls.
  // This is used to track whether the pre-signed urls stored in sortedFetchedFiles are going to
  // expire and need a refresh.
  // GLOBAL variable which should be protected by synchronized
  private var lastQueryTableTimestamp: Long = -1
  // The minimum url expiration timestamp for urls returned from a rpc.
  // GLOBAL variable which should be protected by synchronized
  private var minUrlExpirationTimestamp: Option[Long] = None

  private var lastGetVersionTimestamp: Long = -1
  private var latestTableVersion: Long = -1
  // minimum 30 seconds
  private val QUERY_TABLE_VERSION_INTERVAL_MILLIS = {
    val interval = 30000.max(
      ConfUtils.streamingQueryTableVersionIntervalSeconds(spark.sessionState.conf) * 1000
    )
    if (interval < 30000) {
      throw new IllegalArgumentException(s"QUERY_TABLE_VERSION_INTERVAL_MILLIS($interval) must " +
        "not be less than 30 seconds.")
    }
    interval
  }
  private val maxVersionsPerRpc: Int = options.maxVersionsPerRpc.getOrElse(
    DeltaSharingOptions.MAX_VERSIONS_PER_RPC_DEFAULT
  )

  // The latest function used to fetch presigned urls for the delta sharing table, record it in
  // a variable to be used by the CachedTableManager to refresh the presigned urls if the query
  // runs for a long time.
  private var latestRefreshFunc = (_: Option[String]) => {
    TableRefreshResult(Map.empty[String, String], None, None)
  }

  // Check the latest table version from the delta sharing server through the client.getTableVersion
  // RPC. Adding a minimum interval of QUERY_TABLE_VERSION_INTERVAL_MILLIS between two consecutive
  // rpcs to avoid traffic jam on the delta sharing server.
  private def getOrUpdateLatestTableVersion: Long = {
    val currentTimeMillis = System.currentTimeMillis()
    if (lastGetVersionTimestamp == -1 ||
      (currentTimeMillis - lastGetVersionTimestamp) >= QUERY_TABLE_VERSION_INTERVAL_MILLIS) {
      val serverVersion = deltaLog.client.getTableVersion(deltaLog.table)
      if (serverVersion < 0) {
        throw new IllegalStateException(s"Delta Sharing Server returning negative table version:" +
          s"$serverVersion.")
      } else if (serverVersion < latestTableVersion) {
        logWarning(s"Delta Sharing Server returning smaller table version:$serverVersion < " +
          s"$latestTableVersion.")
      }
      latestTableVersion = serverVersion
      lastGetVersionTimestamp = currentTimeMillis
    }
    latestTableVersion
  }

  // The actual order of files doesn't matter much.
  // Sort by id gives us a stable order of the files within a version.
  private def fileActionCompareFunc(f1: FileAction, f2: FileAction): Boolean = {
    f1.id < f2.id
  }

  private def appendToSortedFetchedFiles(
      indexedFile: IndexedFile,
      urlExpirationTimestamp: java.lang.Long = null
  ): Unit = {
    synchronized {
      sortedFetchedFiles = sortedFetchedFiles :+ indexedFile
      if (urlExpirationTimestamp != null) {
        minUrlExpirationTimestamp = if (minUrlExpirationTimestamp.isDefined &&
          minUrlExpirationTimestamp.get < urlExpirationTimestamp) {
          minUrlExpirationTimestamp
        } else {
          Some(urlExpirationTimestamp)
        }
      }
    }
  }

  /**
   * Fetch the file changes from delta sharing server starting from (fromVersion, fromIndex), based
   * on option.readChangeFeed, it may fetch table files or cdf files.
   *
   * The start point should not be included in the result, it's already consumed in the previous
   * getBatch.
   *
   * If sortedFetchedFiles is not empty, this is a no-op.
   * Else, fetch file changes from the delta sharing server and store them in sortedFetchedFiles.
   *
   * @param fromVersion - a table version, initially would be the startingVersion or the latest
   *                      table version.
   * @param fromIndex - index of a file within the same version,
   * @param isStartingVersion - If true, will load fromVersion as a table snapshot(including files
   *                            from previous versions). If false, will only load files since
   *                            fromVersion.
   */
  private def maybeGetFileChanges(
      fromVersion: Long,
      fromIndex: Long,
      isStartingVersion: Boolean): Unit = {
    if (!sortedFetchedFiles.isEmpty) {
      // Clean up local sortedFileIndex, re-fetch files, to ensure the correct set of files are
      // returned for latestOffset and getBatch.
      // We need to apply this check because the spark streaming engine assumes the DataSource is
      // stateless, and sortedFetchedFiles makes DeltaSharingSource stateful.
      val headFile = sortedFetchedFiles.head
      if (headFile.version > fromVersion || (
        headFile.version == fromVersion && headFile.index > fromIndex && fromIndex != -1) ||
        (isStartingVersion != headFile.isSnapshot)) {
        val lastFile = sortedFetchedFiles.last
        logWarning(s"The asked file(" +
          s"$fromVersion, $fromIndex, $isStartingVersion) is not included in sortedFetchedFiles[" +
          s"(${headFile.version}, ${headFile.index}, ${headFile.isSnapshot}) to " +
          s"(${lastFile.version}, ${lastFile.index}, ${lastFile.isSnapshot})], " +
          s"for table(id:$tableId, name:${deltaLog.table.toString})")
        sortedFetchedFiles = Seq.empty
      } else {
        return
      }
    }

    val currentLatestVersion = getOrUpdateLatestTableVersion
    if (fromVersion > currentLatestVersion) {
      // If true, it means that there's no new data from the delta sharing server.
      return
    }

    // using "fromVersion + maxVersionsPerRpc - 1" because the endingVersion is inclusive.
    val endingVersionForQuery = currentLatestVersion.min(fromVersion + maxVersionsPerRpc - 1)
    if (endingVersionForQuery < currentLatestVersion) {
      logInfo(s"Reducing ending version for delta sharing rpc from currentLatestVersion(" +
        s"$currentLatestVersion) to endingVersionForQuery($endingVersionForQuery), fromVersion:" +
        s"$fromVersion, maxVersionsPerRpc:$maxVersionsPerRpc, " +
        s"for table(id:$tableId, name:${deltaLog.table.toString})."
      )
    }

    if (isStartingVersion || !options.readChangeFeed) {
      getTableFileChanges(fromVersion, fromIndex, isStartingVersion, endingVersionForQuery)
    } else {
      getCDFFileChanges(fromVersion, fromIndex, endingVersionForQuery)
    }
  }

  private def resetGlobalTimestamp(): Unit = {
    synchronized {
      lastQueryTableTimestamp = System.currentTimeMillis()
      minUrlExpirationTimestamp = None
    }
  }

  // Validate the minimum url expiration timestamp, and set it to None if it's invalid.
  // It's considered valid only when it gives enough time for the client to read data out of the
  // pre-signed url. If not valid, we will use the spark config to decide the refresh schedule, and
  // if the url expired before that, we'll leverage the driver log to debug why the expiration
  // timestamp is invalid.
  private def validateMinUrlExpirationTimestamp(inputTimestamp: Option[Long] = None): Unit = {
    synchronized {
      if (inputTimestamp.isDefined) {
        minUrlExpirationTimestamp = inputTimestamp
      }
      if (!CachedTableManager.INSTANCE.isValidUrlExpirationTime(minUrlExpirationTimestamp)) {
        // reset to None to indicate that it's not a valid url expiration timestamp.
        minUrlExpirationTimestamp = None
      }
    }
  }

  // Pop a list of file actions from the local sortedFetchedFiles, until the given endOffset, to
  // be processed by the micro batch.
  // (fileActions, lastQueryTableTimestamp, minUrlExpirationTimestamp) are returned together within
  // a single synchronized wrap, to avoid using old urls with refreshed timestamps when a refresh
  // happens after this function and before register().
  private def popSortedFetchedFiles(
      startVersion: Long,
      startIndex: Long,
      endOffset: DeltaSharingSourceOffset): (Seq[IndexedFile], Long, Option[Long]) = {
    synchronized {
      val fileActions = sortedFetchedFiles.takeWhile {
        case IndexedFile(version, index, _, _, _, _, _) =>
          // Ensure (version, index) is in the range of
          // [(startVersion, startIndex), (endVersion, endOffset)]
          (version > startVersion || (version == startVersion && (index == -1 ||
            index >= startIndex))) && (version < endOffset.tableVersion ||
            (version == endOffset.tableVersion && index <= endOffset.index))
      }
      sortedFetchedFiles = sortedFetchedFiles.drop(fileActions.size)
      (fileActions, lastQueryTableTimestamp, minUrlExpirationTimestamp)
    }
  }

  // Function to be called in latestRefreshFunc, to refresh the pre-signed urls in
  // sortedFetchedFiles. newIdToUrl contains the refreshed urls.
  private def refreshSortedFetchedFiles(
      newIdToUrl: Map[String, String],
      queryTimestamp: Long,
      newMinUrlExpiration: Option[Long]
  ): Unit = {
    synchronized {
      logInfo(s"Refreshing sortedFetchedFiles(size: ${sortedFetchedFiles.size}) with newIdToUrl(" +
        s"size: ${newIdToUrl.size}), for table(id:$tableId, name:${deltaLog.table.toString}).")
      lastQueryTableTimestamp = queryTimestamp
      minUrlExpirationTimestamp = newMinUrlExpiration
      if (!CachedTableManager.INSTANCE.isValidUrlExpirationTime(minUrlExpirationTimestamp)) {
        // reset to None to indicate that it's not a valid url expiration timestamp.
        minUrlExpirationTimestamp = None
      }
      var numUrlsRefreshed = 0
      sortedFetchedFiles = sortedFetchedFiles.map { indexedFile =>
        IndexedFile(
          version = indexedFile.version,
          index = indexedFile.index,
          add = if (indexedFile.add == null) {
            null
          } else {
            numUrlsRefreshed += 1
            val newUrl = newIdToUrl.getOrElse(
              indexedFile.add.id,
              throw new IllegalStateException(s"cannot find url for id ${indexedFile.add.id} " +
                s"when refreshing table ${deltaLog.path}")
            )
            indexedFile.add.copy(url = newUrl)
          },
          remove = if (indexedFile.remove == null) {
            null
          } else {
            numUrlsRefreshed += 1
            val newUrl = newIdToUrl.getOrElse(
              indexedFile.remove.id,
              throw new IllegalStateException(s"cannot find url for id ${indexedFile.remove.id} " +
                s"when refreshing table ${deltaLog.path}")
            )
            indexedFile.remove.copy(url = newUrl)
          },
          cdc = if (indexedFile.cdc == null) {
            null
          } else {
            numUrlsRefreshed += 1
            val newUrl = newIdToUrl.getOrElse(
              indexedFile.cdc.id,
              throw new IllegalStateException(s"cannot find url for id ${indexedFile.cdc.id} " +
                s"when refreshing table ${deltaLog.path}")
            )
            indexedFile.cdc.copy(url = newUrl)
          },
          isSnapshot = indexedFile.isSnapshot,
          isLast = indexedFile.isLast
        )
      }
      logInfo(s"Refreshed ${numUrlsRefreshed} urls in sortedFetchedFiles(size: " +
        s"${sortedFetchedFiles.size}).")
    }
  }

  /**
   * Fetch the table changes from delta sharing server starting from (fromVersion, fromIndex), and
   * store them in sortedFetchedFiles.
   *
   * @param fromVersion - a table version, initially would be the startingVersion or the latest
   *                      table version.
   * @param fromIndex - index of a file within the same version,
   * @param isStartingVersion - If true, will load fromVersion as a table snapshot(including files
   *                            from previous versions). If false, will only load files since
   *                            fromVersion.
   * @param endingVersionForQuery - The ending version used for the query, always smaller than
   *                                latestTableVersion.
   *                                This is used to insert an indexedFile for each version in the
   *                                sortedFetchedFiles, in order to ensure the offset move beyond
   *                                this version.
   */
  private def getTableFileChanges(
      fromVersion: Long,
      fromIndex: Long,
      isStartingVersion: Boolean,
      endingVersionForQuery: Long): Unit = {
    logInfo(s"Fetching files with fromVersion($fromVersion), fromIndex($fromIndex), " +
      s"isStartingVersion($isStartingVersion), endingVersionForQuery($endingVersionForQuery), " +
      s"for table(id:$tableId, name:${deltaLog.table.toString})."
    )
    resetGlobalTimestamp()
    if (isStartingVersion) {
      // If isStartingVersion is true, it means to fetch the snapshot at the fromVersion, which may
      // include table changes from previous versions.
      val tableFiles = deltaLog.client.getFiles(
        table = deltaLog.table,
        predicates = Nil,
        limit = None,
        versionAsOf = Some(fromVersion),
        timestampAsOf = None,
        jsonPredicateHints = None,
        refreshToken = None
      )
      latestRefreshFunc = _ => {
        val queryTimestamp = System.currentTimeMillis()
        val files = deltaLog.client.getFiles(
          table = deltaLog.table,
          predicates = Nil,
          limit = None,
          versionAsOf = Some(fromVersion),
          timestampAsOf = None,
          jsonPredicateHints = None,
          refreshToken = None
        ).files
        var minUrlExpiration: Option[Long] = None
        val idToUrl = files.map { f =>
          if (f.expirationTimestamp != null) {
            minUrlExpiration = if (minUrlExpiration.isDefined &&
              minUrlExpiration.get < f.expirationTimestamp) {
              minUrlExpiration
            } else {
              Some(f.expirationTimestamp)
            }
          }
          f.id -> f.url
        }.toMap

        refreshSortedFetchedFiles(idToUrl, queryTimestamp, minUrlExpiration)

        TableRefreshResult(idToUrl, minUrlExpiration, None)
      }

      val numFiles = tableFiles.files.size
      logInfo(
        s"Fetched ${numFiles} files for table version ${tableFiles.version} from" +
          " delta sharing server."
      )
      tableFiles.files.sortWith(fileActionCompareFunc).zipWithIndex.foreach {
        case (file, index) if (index > fromIndex) =>
          appendToSortedFetchedFiles(
            IndexedFile(
              fromVersion,
              index,
              AddFileForCDF(
                file.url,
                file.id,
                file.partitionValues,
                file.size,
                fromVersion,
                file.timestamp,
                file.stats,
                file.expirationTimestamp
              ),
              isSnapshot = true,
              isLast = (index + 1 == numFiles)
            ),
            file.expirationTimestamp
          )
        // For files with index <= fromIndex, skip them, otherwise an exception will be thrown.
        case _ => ()
      }
    } else {
      // If isStartingVersion is false, it means to fetch table changes since fromVersion, not
      // including files from previous versions.
      val tableFiles = deltaLog.client.getFiles(
        deltaLog.table, fromVersion, Some(endingVersionForQuery)
      )
      latestRefreshFunc = _ => {
        val queryTimestamp = System.currentTimeMillis()
        val addFiles = deltaLog.client.getFiles(
          deltaLog.table, fromVersion, Some(endingVersionForQuery)
        ).addFiles
        var minUrlExpiration: Option[Long] = None
        val idToUrl = addFiles.map { a =>
          if (a.expirationTimestamp != null) {
            minUrlExpiration = if (minUrlExpiration.isDefined &&
              minUrlExpiration.get < a.expirationTimestamp) {
              minUrlExpiration
            } else {
              Some(a.expirationTimestamp)
            }
          }
          a.id -> a.url
        }.toMap

        refreshSortedFetchedFiles(idToUrl, queryTimestamp, minUrlExpiration)

        TableRefreshResult(idToUrl, minUrlExpiration, None)
      }
      val allAddFiles = validateCommitAndFilterAddFiles(tableFiles).groupBy(a => a.version)
      logInfo(
        s"Fetched and filtered ${allAddFiles.size} files from startingVersion " +
          s"${fromVersion} to endingVersion ${endingVersionForQuery} from " +
          "delta sharing server."
      )
      for (v <- fromVersion to endingVersionForQuery) {
        val vAddFiles = allAddFiles.getOrElse(v, ArrayBuffer[AddFileForCDF]())
        val numFiles = vAddFiles.size
        appendToSortedFetchedFiles(
          IndexedFile(v, -1, add = null, isSnapshot = false, isLast = (numFiles == 0))
        )
        vAddFiles.sortWith(fileActionCompareFunc).zipWithIndex.foreach {
          case (add, index) if (v > fromVersion || (v == fromVersion && index > fromIndex)) =>
            appendToSortedFetchedFiles(IndexedFile(
              add.version, index, add, isSnapshot = false, isLast = (index + 1 == numFiles)),
              add.expirationTimestamp
            )
          // For files with v <= fromVersion, skip them, otherwise an exception will be thrown.
          case _ => ()
        }
      }
    }
    validateMinUrlExpirationTimestamp()
  }

  /**
   * Fetch the cdf changes from delta sharing server starting from (fromVersion, fromIndex), and
   * store them in sortedFetchedFiles.
   *
   * @param fromVersion - a table version, initially would be the startingVersion or the latest
   *                      table version.
   * @param fromIndex - index of a file within the same version,
   * @param endingVersionForQuery - The ending version used for the query, always smaller than
   *                                latestTableVersion.
   *                                This is used to insert an indexedFile for each version in the
   *                                sortedFetchedFiles, in order to ensure the offset move beyond
   *                                this version.
   */
  private def getCDFFileChanges(
      fromVersion: Long,
      fromIndex: Long,
      endingVersionForQuery: Long): Unit = {
    logInfo(s"Fetching CDF files with fromVersion($fromVersion), fromIndex($fromIndex), " +
      s"endingVersionForQuery($endingVersionForQuery), " +
      s"for table(id:$tableId, name:${deltaLog.table.toString}).")
    resetGlobalTimestamp()
    val tableFiles = deltaLog.client.getCDFFiles(
      deltaLog.table,
      Map(
        DeltaSharingOptions.CDF_START_VERSION -> fromVersion.toString,
        DeltaSharingOptions.CDF_END_VERSION -> endingVersionForQuery.toString
      ),
      true
    )
    latestRefreshFunc = _ => {
      val queryTimestamp = System.currentTimeMillis()
      val d = deltaLog.client.getCDFFiles(
        deltaLog.table,
        Map(
          DeltaSharingOptions.CDF_START_VERSION -> fromVersion.toString,
          DeltaSharingOptions.CDF_END_VERSION -> endingVersionForQuery.toString
        ),
        true
      )

      val idToUrl = DeltaSharingCDFReader.getIdToUrl(d.addFiles, d.cdfFiles, d.removeFiles)

      val minUrlExpiration = DeltaSharingCDFReader.getMinUrlExpiration(
        d.addFiles, d.cdfFiles, d.removeFiles)
      refreshSortedFetchedFiles(idToUrl, queryTimestamp, minUrlExpiration)

      TableRefreshResult(idToUrl, minUrlExpiration, None)
    }

    (Seq(tableFiles.metadata) ++ tableFiles.additionalMetadatas).foreach { m =>
      val schemaToCheck = DeltaTableUtils.addCdcSchema(DeltaTableUtils.toSchema(m.schemaString))
      if (!SchemaUtils.isReadCompatible(schemaToCheck, schema)) {
        throw DeltaSharingErrors.schemaChangedException(schema, schemaToCheck)
      }
    }
    val cdfUrlExpirationTimestamp = DeltaSharingCDFReader.getMinUrlExpiration(
      tableFiles.addFiles,
      tableFiles.cdfFiles,
      tableFiles.removeFiles
    )
    validateMinUrlExpirationTimestamp(cdfUrlExpirationTimestamp)

    val perVersionAddFiles = tableFiles.addFiles.groupBy(f => f.version)
    val perVersionCdfFiles = tableFiles.cdfFiles.groupBy(f => f.version)
    val perVersionRemoveFiles = tableFiles.removeFiles.groupBy(f => f.version)

    for (v <- fromVersion to endingVersionForQuery) {
      if (perVersionCdfFiles.contains(v)) {
        // Process cdf files if it exists, and ignore add/remove files. This is the property of
        // delta table, when cdf file exists in a version, it represents the same data change as
        // add/remove files, so it's good enough to process the cdf files only, and only cdf files
        // are returned from the delta sharing server for this version.
        val cdfFiles = perVersionCdfFiles.get(v).get.sortWith(fileActionCompareFunc)
        cdfFiles.zipWithIndex.foreach {
          case (cdc, index) if (v > fromVersion || (v == fromVersion && index > fromIndex)) =>
            appendToSortedFetchedFiles(IndexedFile(
              v,
              index,
              add = null,
              cdc = cdc,
              isSnapshot = false,
              isLast = (index + 1 == cdfFiles.size))
            )
          // For files with v <= fromVersion, skip them, otherwise an exception will be thrown.
          case _ => ()
        }
      } else if (perVersionAddFiles.contains(v) || perVersionRemoveFiles.contains(v)) {
        // process add files and remove files
        val fileActions = perVersionAddFiles.getOrElse(v, ArrayBuffer[AddFileForCDF]()) ++
          perVersionRemoveFiles.getOrElse(v, ArrayBuffer[RemoveFile]())
        val numFiles = fileActions.size
        fileActions.sortWith(fileActionCompareFunc).zipWithIndex.foreach {
          case (add: AddFileForCDF, index) if (
            v > fromVersion || (v == fromVersion && index > fromIndex)) =>
            appendToSortedFetchedFiles(IndexedFile(
              v,
              index,
              add,
              isSnapshot = false,
              isLast = (index + 1 == numFiles))
            )
          case (remove: RemoveFile, index) if (
            v > fromVersion || (v == fromVersion && index > fromIndex)) =>
            appendToSortedFetchedFiles(IndexedFile(
              v,
              index,
              add = null,
              remove = remove,
              isSnapshot = false,
              isLast = (index + 1 == numFiles))
            )
          // For files with v <= fromVersion, skip them, otherwise an exception will be thrown.
          case _ => ()
        }
      } else {
        // Still append an IndexedFile for this version with index = -1 and getFileAction = null.
        // This is to proceed through the versions without data files, to avoid processing them
        // repeatedly, i.e., sending useless rpcs to the delta sharing server.
        // This may happen when there's a protocol change of the table, or optimize of a table where
        // there are no data files with dataChange=true, so the server won't return any files for
        // the version.
        appendToSortedFetchedFiles(
          IndexedFile(v, -1, add = null, isSnapshot = false, isLast = true)
        )
      }
    }
  }

  /**
   * Get the last IndexedFile, and uses its version and index to calculate the latestOffset.
   * @param fromVersion - a table version, initially would be the startingVersion or the latest
   *                      table version.
   * @param fromIndex - index of a file within the same version.
   * @param isStartingVersion - If true, will load fromVersion as a table snapshot(including files
   *                            from previous versions). If false, will only load files since
   *                            fromVersion.
   * @param limits - Indicates how much data can be processed by a micro batch.
   * @return the last IndexedFile or None if there are no new data.
   */
  private def getLastFileChangeWithRateLimit(
      fromVersion: Long,
      fromIndex: Long,
      isStartingVersion: Boolean,
      limits: Option[AdmissionLimits] = Some(new AdmissionLimits())): Option[IndexedFile] = {
    maybeGetFileChanges(fromVersion, fromIndex, isStartingVersion)

    if (limits.isEmpty) return sortedFetchedFiles.lastOption

    // Check each change until we've seen the configured number of addFiles. Return the last one
    // for the caller to build offset.
    var admissionControl = limits.get
    var lastFileChange: Option[IndexedFile] = None
    var index = 0
    while (index < sortedFetchedFiles.size) {
      var indexedFile = sortedFetchedFiles(index)

      if (admissionControl.admit(indexedFile.getFileAction)) {
        // For CDC commits we either admit the entire commit or nothing at all.
        // We may exceed the admission control limit. And it's ok because correctness is more
        // important: This is to avoid returning `update_preimage` and `update_postimage` in
        // separate batches.
        while (indexedFile.cdc != null && index + 1 < sortedFetchedFiles.size
          && sortedFetchedFiles(index + 1).cdc != null &&
          sortedFetchedFiles(index + 1).version == indexedFile.version
        ) {
          // while is cdc file and on the same version, admit the file.
          indexedFile = sortedFetchedFiles(index + 1)
          admissionControl.admit(indexedFile.getFileAction)
          index += 1
        }
        lastFileChange = Some(indexedFile)
      } else {
        return lastFileChange
      }

      index += 1
    }

    // If here, it means all files are admitted by the limits.
    lastFileChange
  }

  /**
   * Create DataFrame from startVersion, startIndex to the endOffset, based on sortedFetchedFiles.
   *
   * Since we use sortedFetchedFiles to serve as local cache of all table files, the table files
   * that are used to construct the DataFrame will be dropped from sortedFetchedFiles (they are
   * considered as processed).
   *
   * @param startVersion - calculated starting version.
   * @param startIndex - calculated starting index.
   * @param isStartingVersion - If true, will load fromVersion as a table snapshot(including files
   *                            from previous versions). If false, will only load files since
   *                            fromVersion.
   * @param endOffset - Offset that signifies the end of the stream.
   * @return the created DataFrame.
   */
  private def createDataFrameFromOffset(
      startVersion: Long,
      startIndex: Long,
      isStartingVersion: Boolean,
      endOffset: DeltaSharingSourceOffset): DataFrame = {
    maybeGetFileChanges(startVersion, startIndex, isStartingVersion)

    val (fileActions, lastQueryTimestamp, urlExpirationTimestamp) = popSortedFetchedFiles(
      startVersion, startIndex, endOffset
    )
    // Proceed the offset as the files before the endOffset are processed.
    previousOffset = endOffset

    // indexedFile.getFileAction is null for index=-1 on each version, where we add it to ensure the
    // offset proceed through all table versions even there's no interested files returned for the
    // version.
    val filteredActions = fileActions.filter{ indexedFile => indexedFile.getFileAction != null }

    if (options.readChangeFeed) {
      return createCDFDataFrame(filteredActions, lastQueryTimestamp, urlExpirationTimestamp)
    }

    createDataFrame(filteredActions, lastQueryTimestamp, urlExpirationTimestamp)
  }

  /**
   * Given a list of file actions, create a DataFrame representing the files added to a table
   * Only AddFile actions will be used to create the DataFrame.
   * @param indexedFiles actions list from which to generate the DataFrame.
   */
  private def createDataFrame(
      indexedFiles: Seq[IndexedFile],
      lastQueryTimestamp: Long,
      urlExpirationTimestamp: Option[Long]): DataFrame = {
    val addFilesList = indexedFiles.map { indexedFile =>
      // add won't be null at this step as addFile is the only interested file when
      // options.readChangeFeed is false, which is when this function is called.
      assert(indexedFile.add != null, "add file cannot be null.")
      val add = indexedFile.add
      AddFile(add.url, add.id, add.partitionValues, add.size, add.stats)
    }
    val idToUrl = addFilesList.map { add =>
      add.id -> add.url
    }.toMap

    val params = new RemoteDeltaFileIndexParams(
      spark, initSnapshot, deltaLog.client.getProfileProvider)
    val fileIndex = new RemoteDeltaBatchFileIndex(params, addFilesList)
    CachedTableManager.INSTANCE.register(
      params.path.toString,
      idToUrl,
      Seq(new WeakReference(fileIndex)),
      params.profileProvider,
      latestRefreshFunc,
      if (urlExpirationTimestamp.isDefined) {
        urlExpirationTimestamp.get
      } else {
        lastQueryTimestamp + CachedTableManager.INSTANCE.preSignedUrlExpirationMs
      },
      None
    )


    val relation = HadoopFsRelation(
      fileIndex,
      partitionSchema = initSnapshot.partitionSchema,
      dataSchema = schema,
      bucketSpec = None,
      initSnapshot.fileFormat,
      Map.empty)(spark)

    DeltaSharingScanUtils.ofRows(spark, LogicalRelation(relation, isStreaming = true))
  }

  /**
   * Given a list of file actions, create a DataFrame representing the Change Data Feed of the
   * table.
   * @param indexedFiles actions list from which to generate the DataFrame.
   */
  private def createCDFDataFrame(
      indexedFiles: Seq[IndexedFile],
      lastQueryTimestamp: Long,
      urlExpirationTimestamp: Option[Long]): DataFrame = {
    val addFiles = ArrayBuffer[AddFileForCDF]()
    val cdfFiles = ArrayBuffer[AddCDCFile]()
    val removeFiles = ArrayBuffer[RemoveFile]()
    indexedFiles.foreach{indexedFile =>
      indexedFile.getFileAction match {
        case cdf: AddCDCFile => cdfFiles.append(cdf)
        case add: AddFileForCDF => addFiles.append(add)
        case remove: RemoveFile => removeFiles.append(remove)
        case f => throw new IllegalStateException(s"Unexpected File:${f}")
      }
    }

    DeltaSharingCDFReader.changesToDF(
      new RemoteDeltaFileIndexParams(spark, initSnapshot, deltaLog.client.getProfileProvider),
      schema.fields.map(f => f.name),
      addFiles.toSeq,
      cdfFiles.toSeq,
      removeFiles.toSeq,
      schema,
      isStreaming = true,
      latestRefreshFunc,
      lastQueryTimestamp,
      urlExpirationTimestamp
    )
  }

  /**
   * Returns the offset that starts from a specific delta table version. This function is
   * called when starting a new stream query.
   * @param fromVersion The version of the delta table to calculate the offset from.
   * @param isStartingVersion - If true, will load fromVersion as a table snapshot(including files
   *                            from previous versions). If false, will only load files since
   *                            fromVersion.
   * @param limits Indicates how much data can be processed by a micro batch.
   */
  private def getStartingOffsetFromSpecificDeltaVersion(
      fromVersion: Long,
      isStartingVersion: Boolean,
      limits: Option[AdmissionLimits]): Option[Offset] = {
    val lastFileChange = getLastFileChangeWithRateLimit(
      fromVersion,
      fromIndex = -1L,
      isStartingVersion = isStartingVersion,
      limits)
    if (lastFileChange.isEmpty) {
      None
    } else {
      buildOffsetFromIndexedFile(lastFileChange.get, fromVersion, isStartingVersion)
    }
  }

  /**
   * Return the next offset when previous offset exists.
   */
  private def getNextOffsetFromPreviousOffset(
      limits: Option[AdmissionLimits]): Option[Offset] = {
    val lastFileChange = getLastFileChangeWithRateLimit(
      previousOffset.tableVersion,
      previousOffset.index,
      previousOffset.isStartingVersion,
      limits)

    if (lastFileChange.isEmpty) {
      // Return the previousOffset if there are no more changes, which still indicates until which
      // offset we've processed the data.
      Some(previousOffset)
    } else {
      buildOffsetFromIndexedFile(lastFileChange.get, previousOffset.tableVersion,
        previousOffset.isStartingVersion)
    }
  }

  /**
   * Build the latest offset based on the last indexedFile. The function also checks if latest
   * version is valid by comparing with previous version.
   * @param lastIndexedFile - The last indexed file used to build offset from.
   * @param previousOffsetVersion - Previous offset table version.
   * @param ispreviousOffsetStartingVersion - Whether previous offset is starting version or not.
   * @return the constructed offset.
   */
  private def buildOffsetFromIndexedFile(
      lastIndexedFile: IndexedFile,
      previousOffsetVersion: Long,
      ispreviousOffsetStartingVersion: Boolean): Option[DeltaSharingSourceOffset] = {
    val IndexedFile(v, i, _, _, _, _, isLastFileInVersion) = lastIndexedFile
    assert(v >= previousOffsetVersion,
      s"buildOffsetFromIndexedFile receives an invalid previousOffsetVersion: $v " +
        s"(expected: >= $previousOffsetVersion), tableId: $tableId")

    // If the last file in previous batch is the last file of that version, automatically bump
    // to next version to skip getting files from the same version again from the delta sharing
    // server (through deltaLog.client.getFiles), this is safe because logically for latest offset:
    // (previousVersion, lastIndex> == <nextVersion, -1>
    if (isLastFileInVersion) {
      // isStartingVersion must be false here as we have bumped the version.
      Some(DeltaSharingSourceOffset(DeltaSharingSourceOffset.VERSION_1, tableId, v + 1, index = -1,
        isStartingVersion = false))
    } else {
      // isStartingVersion will be true only if previous isStartingVersion is true and the next file
      // is still at the same version (i.e v == previousOffsetVersion).
      Some(DeltaSharingSourceOffset(DeltaSharingSourceOffset.VERSION_1, tableId, v, i,
        isStartingVersion = (v == previousOffsetVersion && ispreviousOffsetStartingVersion)))
    }
  }

  private def validateCommitAndFilterAddFiles(
      tableFiles: DeltaTableFiles): Seq[AddFileForCDF] = {
    (Seq(tableFiles.metadata) ++ tableFiles.additionalMetadatas).foreach { m =>
      val schemaToCheck = DeltaTableUtils.toSchema(m.schemaString)
      if (!SchemaUtils.isReadCompatible(schemaToCheck, schema)) {
        throw DeltaSharingErrors.schemaChangedException(schema, schemaToCheck)
      }
    }

    val addFiles = tableFiles.addFiles
    if (tableFiles.removeFiles.nonEmpty) {
      /** A check on the source table that disallows changes on the source data. */
      val shouldAllowChanges = options.ignoreChanges || skipChangeCommits
      /** A check on the source table that disallows commits that only include deletes or
       * contain changes on the source data. */
      val shouldAllowDeletes = shouldAllowChanges || options.ignoreDeletes
      val versionsWithRemoveFiles = tableFiles.removeFiles.map(r => r.version).toSet
      val versionsWithAddFiles = tableFiles.addFiles.map(a => a.version).toSet
      if (skipChangeCommits) {
        // Filter out addFiles that have the same version in versionsWithRemoveFiles and directly
        // return the result. Below verification is not needed if skipChangeCommits is true.
        return addFiles.filter(addFile => !versionsWithRemoveFiles.contains(addFile.version))
      }
      versionsWithRemoveFiles.foreach {
        version =>
          if (versionsWithAddFiles.contains(version) && !shouldAllowChanges) {
            throw DeltaSharingErrors.deltaSharingSourceIgnoreChangesError(version)
          } else if (!versionsWithAddFiles.contains(version) && !shouldAllowDeletes) {
            throw DeltaSharingErrors.deltaSharingSourceIgnoreDeleteError(version)
          }
      }
    }

    addFiles
  }

  /**
   * Get the latest offset when the streaming query starts. It tries to fetch all table files
   * from the delta sharing server based on the startingVersion, and then return the latest offset
   * based on the provided read limit.
   * @param limits - Indicates how much data can be processed by a micro batch.
   * @return the latest offset.
   */
  private def getStartingOffset(
      limits: Option[AdmissionLimits] = Some(new AdmissionLimits())): Option[Offset] = {

    val (version, isStartingVersion) = getStartingVersion match {
      case Some(v) => (v, false)
      case None => (getOrUpdateLatestTableVersion, true)
    }
    if (version < 0) {
      return None
    }

    getStartingOffsetFromSpecificDeltaVersion(version, isStartingVersion, limits)
  }

  override def getDefaultReadLimit: ReadLimit = {
    new AdmissionLimits().toReadLimit
  }

  override def latestOffset(startOffset: streaming.Offset, limit: ReadLimit): streaming.Offset = {
    val limits = AdmissionLimits(limit)

    val currentOffset = if (previousOffset == null) {
      getStartingOffset(limits)
    } else {
      getNextOffsetFromPreviousOffset(limits)
    }
    logDebug(s"previousOffset -> currentOffset: $previousOffset -> $currentOffset")
    currentOffset.orNull
  }

  override def getOffset: Option[Offset] = {
    throw new UnsupportedOperationException(
      "latestOffset(Offset, ReadLimit) should be called instead of this method")
  }

  override def getBatch(startOffsetOption: Option[Offset], end: Offset): DataFrame = {
    logInfo(s"getBatch with startOffsetOption($startOffsetOption) and end($end), " +
      s"for table(id:$tableId, name:${deltaLog.table.toString})")
    val endOffset = DeltaSharingSourceOffset(tableId, end)

    val (startVersion, startIndex, isStartingVersion, startSourceVersion) = if (
      startOffsetOption.isEmpty) {
      getStartingVersion match {
        case Some(v) =>
          // startingVersion is provided by the user
          (v, -1L, false, None)

        case _ =>
          // startingVersion is NOT provided by the user
          if (endOffset.isStartingVersion) {
            // Get all files in this version if endOffset is startingVersion
            (endOffset.tableVersion, -1L, true, None)
          } else {
            assert(
              endOffset.tableVersion > 0, s"invalid tableVersion in endOffset: $endOffset")
            // Load from snapshot `endOffset.tableVersion - 1L` if endOffset is not
            // startingVersion, this is the same behavior as DeltaSource.
            (endOffset.tableVersion - 1L, -1L, true, None)
          }
      }
    } else {
      val startOffset = DeltaSharingSourceOffset(tableId, startOffsetOption.get)
      if (startOffset == endOffset) {
        logInfo(s"startOffset($startOffset) is the same as endOffset($endOffset) in getBatch, " +
          s"for table(id:$tableId, name:${deltaLog.table.toString})")
        previousOffset = endOffset
        // This happens only if we recover from a failure and `MicroBatchExecution` tries to call
        // us with the previous offsets. The returned DataFrame will be dropped immediately, so we
        // can return any DataFrame.
        return DeltaSharingScanUtils.internalCreateDataFrame(spark, schema)
      }
      (startOffset.tableVersion, startOffset.index, startOffset.isStartingVersion,
        Some(startOffset.sourceVersion))
    }
    logDebug(s"start: $startOffsetOption end: $end")

    val createdDf = createDataFrameFromOffset(
      startVersion,
      startIndex,
      isStartingVersion,
      endOffset
    )

    createdDf
  }

  override def stop(): Unit = {}

  override def toString(): String = s"DeltaSharingSource[${deltaLog.table.toString}]"

  /**
   * Class that helps controlling how much data should be processed by a single micro-batch.
   */
  class AdmissionLimits(
    maxFiles: Option[Int] = options.maxFilesPerTrigger,
    var bytesToTake: Long = options.maxBytesPerTrigger.getOrElse(Long.MaxValue)
  ) {

    var filesToTake = maxFiles.getOrElse {
      if (options.maxBytesPerTrigger.isEmpty) {
        DeltaSharingOptions.MAX_FILES_PER_TRIGGER_OPTION_DEFAULT
      } else {
        Int.MaxValue - 8 // - 8 to prevent JVM Array allocation OOM
      }
    }

    def toReadLimit: ReadLimit = {
      if (options.maxFilesPerTrigger.isDefined && options.maxBytesPerTrigger.isDefined) {
        CompositeLimit(
          ReadMaxBytes(options.maxBytesPerTrigger.get),
          ReadLimit.maxFiles(options.maxFilesPerTrigger.get).asInstanceOf[ReadMaxFiles])
      } else if (options.maxBytesPerTrigger.isDefined) {
        ReadMaxBytes(options.maxBytesPerTrigger.get)
      } else {
        ReadLimit.maxFiles(
          options.maxFilesPerTrigger.getOrElse(
            DeltaSharingOptions.MAX_FILES_PER_TRIGGER_OPTION_DEFAULT))
      }
    }

    /** Whether to admit the next file */
    def admit(fileAction: FileAction): Boolean = {
      if (fileAction == null) {
        // admit IndexedFile with null fileAction, which should be with index=-1 for each version.
        // This is to proceed through the versions without data files, to avoid processing them
        // repeatedly, i.e., sending useless rpcs to the delta sharing server.
        return true
      }
      val shouldAdmit = filesToTake > 0 && bytesToTake > 0
      filesToTake -= 1

      bytesToTake -= fileAction.size
      shouldAdmit
    }
  }

  object AdmissionLimits {
    def apply(limit: ReadLimit): Option[AdmissionLimits] = limit match {
      case _: ReadAllAvailable => None
      case maxFiles: ReadMaxFiles => Some(new AdmissionLimits(Some(maxFiles.maxFiles())))
      case maxBytes: ReadMaxBytes => Some(new AdmissionLimits(None, maxBytes.maxBytes))
      case composite: CompositeLimit =>
        Some(new AdmissionLimits(Some(composite.files.maxFiles()), composite.bytes.maxBytes))
      case other => throw DeltaSharingErrors.unknownReadLimit(other.toString())
    }
  }

  /**
   * Extracts whether users provided the option to time travel a relation. If a query restarts from
   * a checkpoint and the checkpoint has recorded the offset, this method should never been called.
   */
  private lazy val getStartingVersion: Option[Long] = {
    /** DeltaOption validates input and ensures that only one is provided. */
    if (options.startingVersion.isDefined) {
      val v = options.startingVersion.get match {
        case StartingVersionLatest =>
          getOrUpdateLatestTableVersion + 1
        case StartingVersion(version) =>
          version
      }
      Some(v)
    } else if (options.startingTimestamp.isDefined) {
      Some(deltaLog.client.getTableVersion(deltaLog.table, options.startingTimestamp))
    } else {
      None
    }
  }
}

/** A read limit that admits a soft-max of `maxBytes` per micro-batch. */
case class ReadMaxBytes(maxBytes: Long) extends ReadLimit

/** A read limit that admits the given soft-max of `bytes` or max `files`. */
case class CompositeLimit(bytes: ReadMaxBytes, files: ReadMaxFiles) extends ReadLimit
