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

import io.delta.sharing.spark.model.{AddFile, AddFileForCDF, DeltaTableFiles}

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
trait DeltaSharingSourceBase extends Source
  with SupportsAdmissionControl
  with Logging { self: DeltaSharingSource =>

  val snapshot: RemoteSnapshot = deltaLog.snapshot()

  override val schema: StructType = {
    val schemaWithoutCDC = snapshot.schema
    if (options.readChangeFeed) {
      throw DeltaSharingErrors.CDFNotSupportedInStreaming
    } else {
      schemaWithoutCDC
    }
  }

  protected var sortedFetchedFiles: Seq[IndexedFile] = Seq.empty

  /**
   * Fetch the changes from delta sharing server starting from (fromVersion, fromIndex).
   * The start point should not be included in the result, it should be consumed in the last batch.
   *
   * If sortedFetchedFiles is not empty, return directly.
   * Else, fetch file changes from the delta sharing server and store them in sortedFetchedFiles.
   */
  protected def getFileChanges(
    fromVersion: Long,
    fromIndex: Long,
    isStartingVersion: Boolean): Unit = {
    // scalastyle:off println
    Console.println(s"--------[linzhou]--------[getFileChanges-start]")
    if (!sortedFetchedFiles.isEmpty) { return }

    Console.println(s"--------[linzhou]--------[getFileChanges-empty][fv:${fromVersion}]")
    if (fromVersion > deltaLog.client.getTableVersion(deltaLog.table)) {
      return
    }

    def sortAddFile(f1: AddFile, f2: AddFile): Boolean = { f1.url < f2.url }

    def sortAddFileForCDF(f1: AddFileForCDF, f2: AddFileForCDF): Boolean = {
      f1.version < f2.version || (f1.version == f2.version && f1.url < f2.url)
    }

    def appendToSortedFetchedFiles(indexedFile: IndexedFile): Unit = {
      sortedFetchedFiles = sortedFetchedFiles :+ indexedFile
    }

    if (isStartingVersion) {
      Console.println(s"--------[linzhou]------[isStartingVersion]")
      val tableFiles = deltaLog.client.getFiles(deltaLog.table, Nil, None, Some(fromVersion), None)
      var tmpIndex = 0
      val numFiles = tableFiles.files.size
      Console.println(s"--------[linzhou]------[numFiles][${numFiles}]")
      tableFiles.files.sortWith(sortAddFile).zipWithIndex.foreach{
        case (file, index) if (index > fromIndex) =>
          Console.println(s"--------[linzhou]------[index][$index]")
          if (sortedFetchedFiles.isEmpty) {
            Console.println(s"--------[linzhou]------[appendfirst]")
            appendToSortedFetchedFiles(IndexedFile(fromVersion, -1, null))
          }
          appendToSortedFetchedFiles(
            IndexedFile(fromVersion, tmpIndex, file, isLast = (index + 1 == numFiles)))
      }
    } else {
      Console.println(s"--------[linzhou]------[not-isStartingVersion]")
      var tmpIndex = 0
      // TODO(lin.zhou) return metadata for each version, and check schema read compatibility
      val tableFiles = deltaLog.client.getFiles(deltaLog.table, fromVersion)
      val addFiles = verifyStreamHygieneAndFilterAddFiles(tableFiles)
      var firstFileForVersion = true
      Console.println(s"--------[linzhou]------[addFiles.size][${addFiles.size}]")
      addFiles.groupBy(a => a.version).toSeq.sortWith(_._1 < _._1).foreach{
        case (v, adds) =>
          firstFileForVersion = true
          val numFiles = adds.size
          adds.sortWith(sortAddFileForCDF).zipWithIndex.foreach{
            case (add, index) if (v > fromVersion || index > fromIndex) =>
              if (firstFileForVersion) {
                appendToSortedFetchedFiles(IndexedFile(v, -1, null))
                firstFileForVersion = false
              }
              appendToSortedFetchedFiles(IndexedFile(
                add.version,
                tmpIndex,
                AddFile(add.url, add.id, add.partitionValues, add.size, add.stats),
                isLast = (index + 1 == numFiles))
              )
          }
      }
    }
    Console.println(s"--------[linzhou]------[final-size][${sortedFetchedFiles.size}]")
    // scalastyle:on println
  }

  protected def getLastFileChangeWithRateLimit(
    fromVersion: Long,
    fromIndex: Long,
    isStartingVersion: Boolean,
    limits: Option[AdmissionLimits] = Some(new AdmissionLimits())): Option[IndexedFile] = {
    if (options.readChangeFeed) {
      throw DeltaSharingErrors.CDFNotSupportedInStreaming
    } else {
      // scalastyle:off println
      Console.println(s"--------[linzhou]--------[getLastFileChange-start]")
      getFileChanges(fromVersion, fromIndex, isStartingVersion)
      Console.println(s"--------[linzhou]--------[getLastFileChange-limits][$limits]")
      // scalastyle:on println

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

      lastFileChange
    }
  }

  /**
   * Get the changes from startVersion, startIndex to the endOffset, and create DataFrame
   * @param startVersion - calculated starting version
   * @param startIndex - calculated starting index
   * @param isStartingVersion - whether the stream has to return the initial snapshot or not
   * @param endOffset - Offset that signifies the end of the stream.
   * @return
   */
  protected def getFileChangesAndCreateDataFrame(
    startVersion: Long,
    startIndex: Long,
    isStartingVersion: Boolean,
    endOffset: DeltaSharingSourceOffset): DataFrame = {
    if (options.readChangeFeed) {
      throw DeltaSharingErrors.CDFNotSupportedInStreaming
    } else {
      getFileChanges(startVersion, startIndex, isStartingVersion)

      // scalastyle:off println
      Console.println(s"--------[linzhou]--------[sorted.size][${sortedFetchedFiles.size}]")
      val fileActions = sortedFetchedFiles.takeWhile {
        case IndexedFile(version, index, _, _) =>
          version < endOffset.reservoirVersion ||
            (version == endOffset.reservoirVersion && index <= endOffset.index)
      }
      Console.println(s"--------[linzhou]--------[took.size][${fileActions.size}]")
      sortedFetchedFiles.drop(fileActions.size)
      Console.println(s"--------[linzhou]--------[sorted.size][${sortedFetchedFiles.size}]")

      // filter out the first indexedFile of each version, where index = -1 and add = null
      // TODO(check if it's necessary to add a starter file)
      val filteredIndexedFiles = fileActions.filter { indexedFile =>
        indexedFile.getFileAction != null
      }
      Console.println(s"--------[linzhou]--------[filtered.size][${filteredIndexedFiles.size}]")
      // scalastyle:on println

      createDataFrame(filteredIndexedFiles)
    }
  }

  /**
   * Given an list of file actions, create a DataFrame representing the files added to a table
   * Only AddFile actions will be used to create the DataFrame.
   * @param indexedFiles actions list from which to generate the DataFrame.
   */
  protected def createDataFrame(indexedFiles: Seq[IndexedFile]): DataFrame = {
    // scalastyle:off println
    Console.println(s"--------[linzhou]--------[createDF-start]")
    val addFilesList = indexedFiles.map(_.getFileAction)

    val params = new RemoteDeltaFileIndexParams(spark, snapshot)
    val fileIndex = new RemoteDeltaBatchFileIndex(params, addFilesList)
    Console.println(s"--------[linzhou]--------[createDF-params-index]")

    val relation = HadoopFsRelation(
      fileIndex,
      partitionSchema = snapshot.partitionSchema,
      dataSchema = snapshot.schema,
      bucketSpec = None,
      snapshot.fileFormat,
      Map.empty)(spark)

    Console.println(s"--------[linzhou]--------[createDF-relation]")
    DeltaSharingScanUtils.ofRows(spark, LogicalRelation(relation, isStreaming = true))
  }

  /**
   * Returns the offset that starts from a specific delta table version. This function is
   * called when starting a new stream query.
   * @param fromVersion The version of the delta table to calculate the offset from.
   * @param isStartingVersion Whether the delta version is for the initial snapshot or not.
   * @param limits Indicates how much data can be processed by a micro batch.
   */
  protected def getStartingOffsetFromSpecificDeltaVersion(
    fromVersion: Long,
    isStartingVersion: Boolean,
    limits: Option[AdmissionLimits]): Option[Offset] = {
    // scalastyle:off println
    Console.println(s"--------[linzhou]--------[getStartingFSDV-start]")
    val lastFileChange = getLastFileChangeWithRateLimit(
      fromVersion,
      fromIndex = -1L,
      isStartingVersion = isStartingVersion,
      limits)
    Console.println(s"--------[linzhou]--------[lastFileChange][${lastFileChange}]")
    // scalastyle:on println
    if (lastFileChange.isEmpty) {
      None
    } else {
      buildOffsetFromIndexedFile(lastFileChange.get, fromVersion, isStartingVersion)
    }
  }

  /**
   * Return the next offset when previous offset exists.
   */
  protected def getNextOffsetFromPreviousOffset(
    previousOffset: DeltaSharingSourceOffset,
    limits: Option[AdmissionLimits]): Option[Offset] = {
    val lastFileChange = getLastFileChangeWithRateLimit(
      previousOffset.reservoirVersion,
      previousOffset.index,
      previousOffset.isStartingVersion,
      limits)

    if (lastFileChange.isEmpty) {
      Some(previousOffset)
    } else {
      buildOffsetFromIndexedFile(lastFileChange.get, previousOffset.reservoirVersion,
        previousOffset.isStartingVersion)
    }
  }

  /**
   * Build the latest offset based on the last indexedFile. The function also checks if latest
   * version is valid by comparing with previous version.
   * @param indexedFile The last indexed file used to build offset from.
   * @param version Previous offset reservoir version.
   * @param isStartingVersion Whether previous offset is starting version or not.
   */
  private def buildOffsetFromIndexedFile(
    indexedFile: IndexedFile,
    version: Long,
    isStartingVersion: Boolean): Option[DeltaSharingSourceOffset] = {
    val IndexedFile(v, i, _, isLastFileInVersion) = indexedFile
    assert(v >= version,
      s"buildOffsetFromIndexedFile receives an invalid version: $v (expected: >= $version), " +
        s"tableId: $tableId")

    // If the last file in previous batch is the last file of that version, automatically bump
    // to next version to skip accessing that version file altogether.
    if (isLastFileInVersion) {
      // isStartingVersion must be false here as we have bumped the version.
      Some(DeltaSharingSourceOffset(DeltaSharingSourceOffset.VERSION_1, tableId, v + 1, index = -1,
        isStartingVersion = false))
    } else {
      // isStartingVersion will be true only if previous isStartingVersion is true and the next file
      // is still at the same version (i.e v == version).
      Some(DeltaSharingSourceOffset(DeltaSharingSourceOffset.VERSION_1, tableId, v, i,
        isStartingVersion = v == version && isStartingVersion))
    }
  }
}

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
  options: DeltaSharingOptions)
  extends DeltaSharingSourceBase {

  /** A check on the source table that disallows deletes on the source data. */
  private val ignoreChanges = options.ignoreChanges

  /** A check on the source table that disallows commits that only include deletes to the data. */
  private val ignoreDeletes = options.ignoreDeletes || ignoreChanges

  // This is checked before creating ReservoirSource
  assert(schema.nonEmpty)

  protected val tableId = snapshot.metadata.id

  private var previousOffset: DeltaSharingSourceOffset = null

  protected def verifyStreamHygieneAndFilterAddFiles(
    tableFiles: DeltaTableFiles): Seq[AddFileForCDF] = {
    // scalastyle:off println
    if (!tableFiles.removeFiles.isEmpty) {
      val groupedRemoveFiles = tableFiles.removeFiles.groupBy(r => r.version)
      val groupedAddFiles = tableFiles.addFiles.groupBy(a => a.version)
      groupedRemoveFiles.foreach{
        case (version, _) =>
          if (groupedAddFiles.contains(version) && !ignoreChanges) {
            throw DeltaSharingErrors.deltaSourceIgnoreChangesError(version)
          } else if (!groupedAddFiles.contains(version) && !ignoreDeletes) {
            throw DeltaSharingErrors.deltaSourceIgnoreDeleteError(version)
          }
      }
    }

    tableFiles.addFiles
  }

  private def getStartingOffset(
    limits: Option[AdmissionLimits] = Some(new AdmissionLimits())): Option[Offset] = {

    val (version, isStartingVersion) = getStartingVersion match {
      case Some(v) => (v, false)
      case None => (deltaLog.client.getTableVersion(deltaLog.table), true)
    }
    // scalastyle:off println
    Console.println(s"--------[linzhou]--------[getStarting-v][${version}]")
    Console.println(s"--------[linzhou]--------[getStarting-isStarting][${isStartingVersion}]")
    // scalastyle:on println
    if (version < 0) {
      return None
    }

    getStartingOffsetFromSpecificDeltaVersion(version, isStartingVersion, limits)
  }

  override def getDefaultReadLimit: ReadLimit = {
    new AdmissionLimits().toReadLimit
  }

  override def latestOffset(startOffset: streaming.Offset, limit: ReadLimit): streaming.Offset = {
    // scalastyle:off println
    import java.time.LocalDateTime
    Console.println(s"--------[linzhou]-----------[latestoffset-start][${LocalDateTime.now()}]")
    Console.println(s"--------[linzhou]-----------[latestoffset-limit][${limit}]")
    // scalastyle:on println
    val limits = AdmissionLimits(limit)

    // TODO(lin.zhou) check when to call this
//    if (tableVersion < 0) {
//      tableVersion = deltaLog.client.getTableVersion
//    }

    val currentOffset = if (previousOffset == null) {
      getStartingOffset(limits)
    } else {
      getNextOffsetFromPreviousOffset(previousOffset, limits)
    }
    logDebug(s"previousOffset -> currentOffset: $previousOffset -> $currentOffset")
    // scalastyle:off println
    Console.println(s"--------[linzhou]--------[latestoffset-currentOffset][${currentOffset}]")
    // scalastyle:on println
    currentOffset.orNull
  }

  override def getOffset: Option[Offset] = {
    throw new UnsupportedOperationException(
      "latestOffset(Offset, ReadLimit) should be called instead of this method")
  }

  override def getBatch(startOffsetOption: Option[Offset], end: Offset): DataFrame = {
    // scalastyle:off println
    Console.println(s"--------[linzhou]-----------[getbatch-start]")
    Console.println(s"--------[linzhou]--------[getbatch-soo][$startOffsetOption]")
    Console.println(s"--------[linzhou]--------[getbatch-end][$end]")
    // scalastyle:on println
    val endOffset = DeltaSharingSourceOffset(tableId, end)
    previousOffset = endOffset // Proceed the offset, and for recovery,

    val (startVersion,
    startIndex,
    isStartingVersion,
    startSourceVersion) = if (startOffsetOption.isEmpty) {
      getStartingVersion match {
        case Some(v) =>
          // startingVersion is provided by the user
          (v, -1L, false, None)

        case _ =>
          // startingVersion is NOT provided by the user
          if (endOffset.isStartingVersion) {
            // get all files in this version if endOffset is startingVersion
            (endOffset.reservoirVersion, -1L, true, None)
          } else {
            assert(
              endOffset.reservoirVersion > 0, s"invalid reservoirVersion in endOffset: $endOffset")
            // Load from snapshot `endOffset.reservoirVersion - 1L` if endOffset is not
            // startingVersion
            (endOffset.reservoirVersion - 1L, -1L, true, None)
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
      (startOffset.reservoirVersion, startOffset.index, startOffset.isStartingVersion,
        Some(startOffset.sourceVersion))
    }
    logDebug(s"start: $startOffsetOption end: $end")

    // scalastyle:off println
    Console.println(s"--------[linzhou]--------[getbatch-sv,si,isv]" +
      s"[${startVersion}, $startIndex, $isStartingVersion]")
    val createdDf = getFileChangesAndCreateDataFrame(
      startVersion, startIndex, isStartingVersion, endOffset
    )

    Console.println(s"--------[linzhou]--------[getbatch-createdDF][${createdDf.show()}]")
    // scalastyle:on println
    createdDf
  }

  override def stop(): Unit = {}

  override def toString(): String = s"DeltaSharingSource[${deltaLog.table.toString}]"

  trait DeltaSharingSourceAdmissionBase { self: AdmissionLimits =>
    /** Whether to admit the next file */
    def admit(addFile: Option[AddFile]): Boolean = {
      if (addFile.isEmpty) return true
      val shouldAdmit = filesToTake > 0 && bytesToTake > 0
      filesToTake -= 1

      bytesToTake -= addFile.get.size
      shouldAdmit
    }
  }

  /**
   * Class that helps controlling how much data should be processed by a single micro-batch.
   */
  class AdmissionLimits(
    maxFiles: Option[Int] = options.maxFilesPerTrigger,
    var bytesToTake: Long = options.maxBytesPerTrigger.getOrElse(Long.MaxValue)
  ) extends DeltaSharingSourceAdmissionBase {

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
  protected lazy val getStartingVersion: Option[Long] = {
    /** DeltaOption validates input and ensures that only one is provided. */
    if (options.startingVersion.isDefined) {
      val v = options.startingVersion.get match {
        case StartingVersionLatest =>
          deltaLog.client.getTableVersion(deltaLog.table) + 1
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
