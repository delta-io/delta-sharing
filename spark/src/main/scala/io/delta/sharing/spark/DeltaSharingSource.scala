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
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, DeltaSharingScanUtils, SparkSession}

import org.apache.spark.sql.connector.read.streaming
import org.apache.spark.sql.connector.read.streaming.{
  ReadAllAvailable,
  ReadLimit,
  ReadMaxFiles,
  SupportsAdmissionControl
}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.types.StructType

import io.delta.sharing.spark.model.{AddFile, AddFileForCDF, DeltaTableFiles, FileAction}

/**
 * A case class to help with `Dataset` operations regarding Offset indexing, representing AddFile
 * actions in a Delta log.
 * For proper offset tracking(move the offset to the next version if all data is consumed in the
 * current version), there are also special sentinel values with index = -1 and add = null.
 *
 * This class is not designed to be persisted in offset logs or such.
 *
 * @param version The version of the Delta log containing this AddFile.
 * @param index The index of this AddFile in the Delta log.
 * @param add The AddFile.
 * @param isLast A flag to indicate whether this is the last AddFile in the version. This is used
 *               to resolve an off-by-one issue in the streaming offset interface; once we've read
 *               to the end of a log version file, we check this flag to advance immediately to the
 *               next one in the persisted offset. Without this special case we would re-read the
 *               already completed log file.
 */
private[sharing] case class IndexedFile(
  version: Long,
  index: Long,
  add: AddFile,
  isLast: Boolean = false) {

  def getFileAction: AddFile = { add }
}

/**
 * Base trait for the Delta Sharing Source, that contains methods that deal with
 * getting changes from the delta sharing server.
 * TODO(lin.zhou) Support SupportsTriggerAvailableNow
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

  private val snapshot: RemoteSnapshot = deltaLog.snapshot()

  override val schema: StructType = {
    val schemaWithoutCDC = snapshot.schema
    if (options.readChangeFeed) {
      throw DeltaSharingErrors.CDFNotSupportedInStreaming
    } else {
      schemaWithoutCDC
    }
  }

  // This is checked before creating DeltaSharingSource
  assert(schema.nonEmpty)

  /** A check on the source table that disallows deletes on the source data. */
  private val ignoreChanges = options.ignoreChanges

  /** A check on the source table that disallows commits that only include deletes to the data. */
  private val ignoreDeletes = options.ignoreDeletes || ignoreChanges

  private val tableId = snapshot.metadata.id

  // Records until which offset the delta sharing source has been processing the table files.
  private var previousOffset: DeltaSharingSourceOffset = null

  // Serves as local cache to store all the files fetched from the delta sharing server.
  // If not empty, will advance the offset and fetch data from this list based on the read limit.
  // If empty, will try to load all possible new data files through delta sharing rpc to this list,
  //   sorted by version and id.
  private var sortedFetchedFiles: Seq[IndexedFile] = Seq.empty

  private var lastGetVersionTimestamp: Long = -1
  private var lastQueriedTableVersion: Long = -1
  private val QUERY_TABLE_VERSION_INTERVAL_MILLIS = 30000 // 30 seconds

  // Check the latest table version from the delta sharing server through the client.getTableVersion
  // RPC. Adding a minimum interval of QUERY_TABLE_VERSION_INTERVAL_MILLIS between two consecutive
  // rpcs to avoid traffic jam on the delta sharing server.
  private def getOrUpdateLatestTableVersion: Long = {
    val currentTimeMillis = System.currentTimeMillis()
    if (lastGetVersionTimestamp == -1 ||
      (currentTimeMillis - lastGetVersionTimestamp) >= QUERY_TABLE_VERSION_INTERVAL_MILLIS) {
      lastQueriedTableVersion = deltaLog.client.getTableVersion(deltaLog.table)
      lastGetVersionTimestamp = currentTimeMillis
    }
    lastQueriedTableVersion
  }

  /**
   * Fetch the changes from delta sharing server starting from (fromVersion, fromIndex).
   *
   * The start point should not be included in the result, it will be consumed in the next getBatch.
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
    if (!sortedFetchedFiles.isEmpty) { return }

    if (fromVersion > getOrUpdateLatestTableVersion) {
      // If true, it means that there's no new data from the delta sharing server.
      return
    }

    // The actual order of files doesn't matter much.
    // Sort by id gives us a stable order of the files within a version.
    def fileActionCompareFunc(f1: FileAction, f2: FileAction): Boolean = { f1.id < f2.id }

    def appendToSortedFetchedFiles(indexedFile: IndexedFile): Unit = {
      sortedFetchedFiles = sortedFetchedFiles :+ indexedFile
    }

    if (isStartingVersion) {
      // If isStartingVersion is true, it means to fetch the snapshot at the fromVersion, which may
      // include table changes from previous versions.
      val tableFiles = deltaLog.client.getFiles(deltaLog.table, Nil, None, Some(fromVersion), None)
      val numFiles = tableFiles.files.size
      tableFiles.files.sortWith(fileActionCompareFunc).zipWithIndex.foreach{
        case (file, index) if (index > fromIndex) =>
          appendToSortedFetchedFiles(
            IndexedFile(fromVersion, index, file, isLast = (index + 1 == numFiles)))
      }
    } else {
      // If isStartingVersion is false, it means to fetch table changes since fromVersion, not
      // including files from previous versions.
      // TODO(lin.zhou) return metadata for each version, and check schema read compatibility
      val tableFiles = deltaLog.client.getFiles(deltaLog.table, fromVersion)
      val addFiles = verifyStreamHygieneAndFilterAddFiles(tableFiles)
      addFiles.groupBy(a => a.version).toSeq.sortWith(_._1 < _._1).foreach{
        case (v, vAddFiles) =>
          val numFiles = vAddFiles.size
          vAddFiles.sortWith(fileActionCompareFunc).zipWithIndex.foreach{
            case (add, index) if (v > fromVersion || (v == fromVersion && index > fromIndex)) =>
              appendToSortedFetchedFiles(IndexedFile(
                add.version,
                index,
                AddFile(add.url, add.id, add.partitionValues, add.size, add.stats),
                isLast = (index + 1 == numFiles))
              )
          }
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
    if (options.readChangeFeed) {
      throw DeltaSharingErrors.CDFNotSupportedInStreaming
    } else {
      maybeGetFileChanges(fromVersion, fromIndex, isStartingVersion)

      if (limits.isEmpty) return sortedFetchedFiles.lastOption

      // Check each change until we've seen the configured number of addFiles. Return the last one
      // for the caller to build offset.
      var admissionControl = limits.get
      var lastFileChange: Option[IndexedFile] = None
      sortedFetchedFiles.foreach{indexedFile =>
        if (admissionControl.admit(Option(indexedFile.add))) {
          lastFileChange = Some(indexedFile)
        } else {
          return lastFileChange
        }
      }

      // If here, it means all files are admitted by the limits.
      lastFileChange
    }
  }

  /**
   * Get the changes from startVersion, startIndex to the endOffset, and create DataFrame.
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
  private def getFileChangesAndCreateDataFrame(
    startVersion: Long,
    startIndex: Long,
    isStartingVersion: Boolean,
    endOffset: DeltaSharingSourceOffset): DataFrame = {
    if (options.readChangeFeed) {
      throw DeltaSharingErrors.CDFNotSupportedInStreaming
    }

    maybeGetFileChanges(startVersion, startIndex, isStartingVersion)

    val fileActions = sortedFetchedFiles.takeWhile {
      case IndexedFile(version, index, _, _) =>
        version < endOffset.tableVersion ||
          (version == endOffset.tableVersion && index <= endOffset.index)
    }
    sortedFetchedFiles = sortedFetchedFiles.drop(fileActions.size)
    // Proceed the offset as the files before the endOffset are processed.
    previousOffset = endOffset

    createDataFrame(fileActions)
  }

  /**
   * Given a list of file actions, create a DataFrame representing the files added to a table
   * Only AddFile actions will be used to create the DataFrame.
   * @param indexedFiles actions list from which to generate the DataFrame.
   */
  private def createDataFrame(indexedFiles: Seq[IndexedFile]): DataFrame = {
    val addFilesList = indexedFiles.map(_.getFileAction)

    val params = new RemoteDeltaFileIndexParams(spark, snapshot)
    val fileIndex = new RemoteDeltaBatchFileIndex(params, addFilesList)

    val relation = HadoopFsRelation(
      fileIndex,
      partitionSchema = snapshot.partitionSchema,
      dataSchema = snapshot.schema,
      bucketSpec = None,
      snapshot.fileFormat,
      Map.empty)(spark)

    DeltaSharingScanUtils.ofRows(spark, LogicalRelation(relation, isStreaming = true))
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
    previousOffset: DeltaSharingSourceOffset,
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
    val IndexedFile(v, i, _, isLastFileInVersion) = lastIndexedFile
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

  private def verifyStreamHygieneAndFilterAddFiles(
    tableFiles: DeltaTableFiles): Seq[AddFileForCDF] = {
    if (!tableFiles.removeFiles.isEmpty) {
      val versionsWithRemoveFiles = tableFiles.removeFiles.map(r => r.version).toSet
      val versionsWithAddFiles = tableFiles.addFiles.map(a => a.version).toSet
      versionsWithRemoveFiles.foreach{
        case version =>
          if (versionsWithAddFiles.contains(version) && !ignoreChanges) {
            throw DeltaSharingErrors.deltaSourceIgnoreChangesError(version)
          } else if (!versionsWithAddFiles.contains(version) && !ignoreDeletes) {
            throw DeltaSharingErrors.deltaSourceIgnoreDeleteError(version)
          }
      }
    }

    tableFiles.addFiles
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
      getNextOffsetFromPreviousOffset(previousOffset, limits)
    }
    logDebug(s"previousOffset -> currentOffset: $previousOffset -> $currentOffset")
    currentOffset.orNull
  }

  override def getOffset: Option[Offset] = {
    throw new UnsupportedOperationException(
      "latestOffset(Offset, ReadLimit) should be called instead of this method")
  }

  override def getBatch(startOffsetOption: Option[Offset], end: Offset): DataFrame = {
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
        // This happens only if we recover from a failure and `MicroBatchExecution` tries to call
        // us with the previous offsets. The returned DataFrame will be dropped immediately, so we
        // can return any DataFrame.
        return DeltaSharingScanUtils.internalCreateDataFrame(spark, schema)
      }
      (startOffset.tableVersion, startOffset.index, startOffset.isStartingVersion,
        Some(startOffset.sourceVersion))
    }
    logDebug(s"start: $startOffsetOption end: $end")

    val createdDf = getFileChangesAndCreateDataFrame(
      startVersion, startIndex, isStartingVersion, endOffset
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
    def admit(addFile: Option[AddFile]): Boolean = {
      if (addFile.isEmpty) return true
      val shouldAdmit = filesToTake > 0 && bytesToTake > 0
      filesToTake -= 1

      bytesToTake -= addFile.get.size
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
      throw new UnsupportedOperationException("startingTimestamp is not supported yet")
    } else {
      None
    }
  }
}

/** A read limit that admits a soft-max of `maxBytes` per micro-batch. */
case class ReadMaxBytes(maxBytes: Long) extends ReadLimit

/** A read limit that admits the given soft-max of `bytes` or max `files`. */
case class CompositeLimit(bytes: ReadMaxBytes, files: ReadMaxFiles) extends ReadLimit
