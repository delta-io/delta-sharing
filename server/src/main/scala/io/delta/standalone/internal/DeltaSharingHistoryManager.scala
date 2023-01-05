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
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

import io.delta.standalone.internal.actions.CommitMarker
import io.delta.standalone.internal.util.FileNames
import io.delta.storage.LogStore
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import scala.collection.JavaConverters._

object DeltaSharingHistoryManager {
  /**
   * DeltaHistoryManager.getCommits is not a public method, so we need to make local copies here.
   * When calling getCommits, the initial few timestamp values may be wrong because they are not
   * properly monotonized. getCommitsSafe uses this to update the start value
   * far behind the first timestamp they care about to get correct values.
   * TODO(https://github.com/delta-io/delta-sharing/issues/144): Cleans this up once
   *   DeltaHistoryManager.getCommits is public
   */
  private val POTENTIALLY_UNMONOTONIZED_TIMESTAMPS = 100

  // Correct timestamp values are only available through getCommits(). Commit
  // info timestamps are wrong, and file modification times are wrong because they need to be
  // monotonized first. This just performs a list (we don't read the contents of the files in
  // getCommits()) so it's not a big deal.
  private[internal] def getTimestampsByVersion(
      logStore: LogStore,
      logPath: Path,
      start: Long,
      end: Long,
      conf: Configuration): Map[Long, Timestamp] = {
    val monotonizationStart =
      Seq(start - POTENTIALLY_UNMONOTONIZED_TIMESTAMPS, 0).max
    val commits = getCommits(logStore, logPath, monotonizationStart, end, conf)

    // Note that the timestamps come from filesystem modification timestamps, so they're
    // milliseconds since epoch and we don't need to deal with timezones.
    commits.map(f => (f.version -> new Timestamp(f.timestamp))).toMap
  }

  // Convert timestamp string to Timestamp
  private[internal] def getTimestamp(paramName: String, timeStampStr: String): Timestamp = {
    try {
      new Timestamp(OffsetDateTime.parse(
        timeStampStr, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant.toEpochMilli)
    } catch {
      case e: java.time.format.DateTimeParseException =>
        throw DeltaCDFErrors.invalidTimestamp(paramName, e.getMessage)
    }
  }

  /**
   * Returns the commit version and timestamps of all commits in `[start, end)`. If `end` is not
   * specified, will return all commits that exist after `start`. Will guarantee that the commits
   * returned will have both monotonically increasing versions as well as timestamps.
   * Exposed for tests.
   */
  private def getCommits(
      logStore: LogStore,
      logPath: Path,
      start: Long,
      end: Long,
      conf: Configuration): Array[Commit] = {
    val commits = logStore
      .listFrom(FileNames.deltaFile(logPath, start), conf)
      .asScala
      .filter(f => FileNames.isDeltaFile(f.getPath))
      .map { fileStatus =>
        Commit(FileNames.deltaVersion(fileStatus.getPath), fileStatus.getModificationTime)
      }
      .takeWhile(_.version < end)

    monotonizeCommitTimestamps(commits.toArray)
  }

  /**
   * Makes sure that the commit timestamps are monotonically increasing with respect to commit
   * versions. Requires the input commits to be sorted by the commit version.
   */
  private def monotonizeCommitTimestamps[T <: CommitMarker](
      commits: Array[T]): Array[T] = {
    var i = 0
    val length = commits.length
    while (i < length - 1) {
      val prevTimestamp = commits(i).getTimestamp
      assert(commits(i).getVersion < commits(i + 1).getVersion, "Unordered commits provided.")
      if (prevTimestamp >= commits(i + 1).getTimestamp) {
        commits(i + 1) = commits(i + 1).withTimestamp(prevTimestamp + 1).asInstanceOf[T]
      }
      i += 1
    }
    commits
  }

  /** A helper class to represent the timestamp and version of a commit. */
  case class Commit(version: Long, timestamp: Long) extends CommitMarker {
    override def withTimestamp(timestamp: Long): Commit = this.copy(timestamp = timestamp)

    override def getTimestamp: Long = timestamp

    override def getVersion: Long = version
  }
}
