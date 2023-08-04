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

import java.sql.Timestamp

import io.delta.standalone.DeltaLog
import io.delta.standalone.internal.actions.{
  Action,
  AddCDCFile,
  AddFile,
  CommitInfo,
  Metadata,
  RemoveFile
}
import io.delta.standalone.internal.exception.DeltaErrors
import io.delta.standalone.internal.util.ConversionUtils
import org.apache.hadoop.conf.Configuration
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.util.control.NonFatal

/**
 * This is a special CDCReader that is optimized for delta sharing server usage.
 * It provides a `queryCDF` method to return all cdf actions in a table: `AddFile`s,
 * `AddCDCFile`s and `RemoveFile`s.
 */
class DeltaSharingCDCReader(val deltaLog: DeltaLogImpl, val conf: Configuration) {

  private lazy val snapshot = deltaLog.snapshot
  lazy val protocol = snapshot.protocolScala
  lazy val metadata = snapshot.metadataScala
  private lazy val history = DeltaHistoryManager(deltaLog)

  private[internal] def getCDCVersions(
      cdfOptions: Map[String, String],
      latestVersion: Long): (Long, Long) = {
    val startingVersion = getVersionForCDC(
      cdfOptions,
      DeltaDataSource.CDF_START_VERSION_KEY,
      DeltaDataSource.CDF_START_TIMESTAMP_KEY,
      latestVersion
    )
    if (startingVersion.isEmpty) {
      throw DeltaCDFErrors.noStartVersionForCDF
    }
    // add a version check here that is cheap instead of after trying to list a large version
    // that doesn't exist
    if (startingVersion.get > latestVersion) {
      throw DeltaCDFErrors.startVersionAfterLatestVersion(startingVersion.get, latestVersion)
    }

    val endingVersion = getVersionForCDC(
      cdfOptions,
      DeltaDataSource.CDF_END_VERSION_KEY,
      DeltaDataSource.CDF_END_TIMESTAMP_KEY,
      latestVersion
    )

    if (endingVersion.exists(_ < startingVersion.get)) {
      throw DeltaCDFErrors.endBeforeStartVersionInCDF(startingVersion.get, endingVersion.get)
    }

    (startingVersion.get, endingVersion.getOrElse(latestVersion))
  }


  /**
   * - If a commit version exactly matches the provided timestamp, we return it.
   * - Otherwise, we return the earliest commit version
   *   with a timestamp greater than the provided one.
   * - If the provided timestamp is larger than the timestamp
   *   of any committed version, we throw an error.
   *
   * @param timestamp - user specified timestamp
   * @param latestVersion - latest version of the table
   * @return - corresponding version number for timestamp
   */
  private def getStartingVersionFromTimestamp(timestamp: Timestamp, latestVersion: Long): Long = {
    val commit = getActiveCommitAtTime(timestamp)
    if (commit.timestamp >= timestamp.getTime) {
      // Find the commit at the `timestamp` or the earliest commit
      commit.version
    } else {
      // commit.timestamp is not the same, so this commit is a commit before the timestamp and
      // the next version if exists should be the earliest commit after the timestamp.
      // Note: `getActiveCommitAtTime` has called `update`, so we don't need to call it again.
      if (commit.version + 1 <= latestVersion) {
        commit.version + 1
      } else {
        val commitTs = new Timestamp(commit.timestamp)
        throw DeltaErrors.timestampLaterThanTableLastCommit(timestamp, commitTs)
      }
    }
  }

  /**
   * Given timestamp or version this method returns the corresponding version for that timestamp
   * or the version itself.
   */
  private def getVersionForCDC(
      options: Map[String, String],
      versionKey: String,
      timestampKey: String,
      latestVersion: Long
  ): Option[Long] = {
    if (options.contains(versionKey)) {
      Some(options(versionKey).toLong)
    } else if (options.contains(timestampKey)) {
      val ts = DeltaSharingHistoryManager.getTimestamp(timestampKey, options(timestampKey))
      if (timestampKey == DeltaDataSource.CDF_START_TIMESTAMP_KEY) {
        // For the starting timestamp we need to find a version after the provided timestamp
        // we can use the same semantics as streaming.
        val resolvedVersion = getStartingVersionFromTimestamp(ts, latestVersion)
        Some(resolvedVersion)
      } else {
        require(timestampKey == DeltaDataSource.CDF_END_TIMESTAMP_KEY)
        // For ending timestamp the version should be before the provided timestamp.
        Some(getActiveCommitAtTime(ts).version)
      }
    } else {
      None
    }
  }

  /**
   * A wrapper around DeltaHistory manager method.
   * The method implicitly validates the timestamp arg, so we remap its exception to CDF
   * illegal arg exception.
   */
   private def getActiveCommitAtTime(ts: Timestamp): DeltaSharingHistoryManager.Commit = {
     val commit = try {
       history.getActiveCommitAtTime(ts)
     } catch {
       case NonFatal(e) =>
         throw new DeltaCDFIllegalArgumentException(e.getMessage())
     }
     DeltaSharingHistoryManager.Commit(commit.version, commit.timestamp)
   }

  /**
   * Replay Delta transaction logs and return cdf files
   *
   * @param cdfOptions the starting and ending parameters.
   * @param latestVersion the latest version of the delta table, which is used to validate the
   *                      starting and ending versions, and may be used as default ending version.
   * @param validStartVersion indicates the valid start version of the query
   * @return - start and end version parsed from cdfOptions if they are valid, otherwise throws
   *           exception
   */
  def validateCdfOptions(
    cdfOptions: Map[String, String],
    latestVersion: Long,
    validStartVersion: Long): (Long, Long) = {
    val (start, end) = getCDCVersions(cdfOptions, latestVersion)
    if (validStartVersion > start) {
      throw new DeltaCDFIllegalArgumentException(
        s"You can only query table changes since version ${validStartVersion}.")
    }
    (start, end)
  }

  /**
   * Replay Delta transaction logs and return cdf files
   *
   * @param start The start version of cdf
   * @param end The end version of cdf
   * @param latestVersion the latest version of the delta table, which is used to validate the
   *                      starting and ending versions.
   * @param includeHistoricalMetadata if true, it need to include metadata for schema compatibility
   *                                  check. Otherwise, no need to include metadata.
   */
  def queryCDF(
    start: Long,
    end: Long,
    latestVersion: Long,
    includeHistoricalMetadata: Boolean): Seq[CDCDataSpec] = {
    if (start > latestVersion) {
      throw DeltaCDFErrors.startVersionAfterLatestVersion(start, latestVersion)
    }
    if (start > end) {
      throw DeltaCDFErrors.endBeforeStartVersionInCDF(start, end)
    }
    if (!isCDCEnabledOnTable(deltaLog.getSnapshotForVersionAsOf(start).metadataScala)) {
      throw DeltaCDFErrors.changeDataNotRecordedException(start, start, end)
    }

    val changes = deltaLog.getChanges(start, false).asScala.takeWhile(_.getVersion <= end)

    // Correct timestamp values are only available through
    // DeltaHistoryManager.getTimestampsByVersion
    val timestampsByVersion = DeltaSharingHistoryManager.getTimestampsByVersion(
      deltaLog.store,
      deltaLog.logPath,
      start,
      end + 1,
      conf
    )

    val cdcSpecs = ListBuffer[CDCDataSpec]()

    changes.foreach {versionLog =>
        val v = versionLog.getVersion
        val actions = versionLog.getActions.asScala.map(x => ConversionUtils.convertActionJ(x))
        // Check whether CDC was newly disabled in this version. (We should have already checked
        // that it's enabled for the starting version, so checking this for each version
        // incrementally is sufficient to ensure that it's enabled for the entire range.)
        val cdcDisabled = actions.exists {
          case m: Metadata => !isCDCEnabledOnTable(m)
          case _ => false
        }

        if (cdcDisabled) {
          throw DeltaCDFErrors.changeDataNotRecordedException(v, start, end)
        }

        val ts = timestampsByVersion.get(v).orNull
        val selectedActions = ListBuffer[Action]()

        // Note that the CommitInfo is *not* guaranteed to be generated in 100% of cases.
        // We are using it only for a hotfix-safe mitigation/defense-in-depth - the value
        // extracted here cannot be relied on for correctness.
        var commitInfo: Option[CommitInfo] = None
        var containsCDCAction: Boolean = false
        actions.foreach {
          case _: AddCDCFile =>
            containsCDCAction = true
          case i: CommitInfo => commitInfo = Some(i)
          case _ => // do nothing
        }

        // For UPDATE sql command, cdc actions provides the accurate commit type: update_postimage
        // and update_preimage.
        // For INSERT or DELETE, most of the times both cdc and add/remove actions will be
        // generated, either of them will result in the correct cdc data.
        // If there are CDC actions, we read them exclusively, and ignore the add/remove actions.
        if (containsCDCAction) {
          actions.foreach {
            case c: AddCDCFile =>
              selectedActions.append(c)
            case m: Metadata if (includeHistoricalMetadata && v > start) =>
              selectedActions.append(m)
            case _ => ()
          }
        } else {
          if (commitInfo.exists(shouldSkipFileActionsInCommit)) {
            // This was introduced for a hotfix, so we're mirroring the existing logic as closely
            // as possible - we skip all file actions and return metadata only.
            actions.foreach {
              case m: Metadata if (includeHistoricalMetadata && v > start) =>
                selectedActions.append(m)
              case _ => ()
            }
          } else {
            // Otherwise, we take the AddFile and RemoveFile actions with dataChange = true and
            // infer CDC from them.
            actions.foreach {
              case a: AddFile if a.dataChange =>
                selectedActions.append(a)
              case r: RemoveFile if r.dataChange =>
                selectedActions.append(r)
              case m: Metadata if (includeHistoricalMetadata && v > start) =>
                selectedActions.append(m)
              case _ => ()
            }
          }
        }
        cdcSpecs.append(CDCDataSpec(v, ts, selectedActions.toSeq))
    }

    cdcSpecs.toSeq
  }

  case class CDCDataSpec(version: Long, timestamp: Timestamp, actions: Seq[Action])

  /**
   * Function to check if file actions should be skipped for no-op merges based on
   * CommitInfo metrics.
   * MERGE will sometimes rewrite files in a way which *could* have changed data
   * (so dataChange = true) but did not actually do so (so no CDC will be produced).
   * In this case the correct CDC output is empty - we shouldn't serve it from
   * those files. This should be handled within the command, but as a hotfix-safe fix, we check
   * the metrics. If the command reported 0 rows inserted, updated, or deleted, then CDC
   * shouldn't be produced.
   */
  private def shouldSkipFileActionsInCommit(commitInfo: CommitInfo): Boolean = {
    val isMerge = commitInfo.operation == "MERGE"
    val knownToHaveNoChangedRows = {
      val metrics = commitInfo.operationMetrics.getOrElse(Map.empty)
      // Note that if any metrics are missing, this condition will be false and we won't skip.
      // Unfortunately there are no predefined constants for these metric values.
      Seq("numTargetRowsInserted", "numTargetRowsUpdated", "numTargetRowsDeleted").forall {
        metrics.get(_).contains("0")
      }
    }
    isMerge && knownToHaveNoChangedRows
  }

  /**
   * Determine if the metadata provided has cdc enabled or not.
   */
  def isCDCEnabledOnTable(metadata: Metadata): Boolean = {
    metadata.configuration.getOrElse("delta.enableChangeDataFeed", "false") == "true"
  }
}
