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
import java.util.Locale

import scala.util.Try

import org.apache.spark.internal.Logging
import org.apache.spark.network.util.{ByteUnit, JavaUtils}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

trait DeltaSharingOptionParser {
  protected def options: CaseInsensitiveMap[String]

  def toBoolean(input: String, name: String): Boolean = {
    Try(input.toBoolean).toOption.getOrElse {
      throw DeltaSharingErrors.illegalDeltaOptionException(name, input, "must be 'true' or 'false'")
    }
  }
}

trait DeltaSharingReadOptions extends DeltaSharingOptionParser {
  import DeltaSharingOptions._

  val maxFilesPerTrigger = options.get(MAX_FILES_PER_TRIGGER_OPTION).map { str =>
    Try(str.toInt).toOption.filter(_ > 0).getOrElse {
      throw DeltaSharingErrors.illegalDeltaOptionException(
        MAX_FILES_PER_TRIGGER_OPTION, str, "must be a positive integer")
    }
  }

  val maxBytesPerTrigger = options.get(MAX_BYTES_PER_TRIGGER_OPTION).map { str =>
    Try(JavaUtils.byteStringAs(str, ByteUnit.BYTE)).toOption.filter(_ > 0).getOrElse {
      throw DeltaSharingErrors.illegalDeltaOptionException(
        MAX_BYTES_PER_TRIGGER_OPTION, str, "must be a size configuration such as '10g'")
    }
  }

  val ignoreChanges = options.get(IGNORE_CHANGES_OPTION).exists(toBoolean(_, IGNORE_CHANGES_OPTION))

  val ignoreDeletes = options.get(IGNORE_DELETES_OPTION).exists(toBoolean(_, IGNORE_DELETES_OPTION))

  val readChangeFeed = options.get(CDC_READ_OPTION).exists(toBoolean(_, CDC_READ_OPTION)) ||
    options.get(CDC_READ_OPTION_LEGACY).exists(toBoolean(_, CDC_READ_OPTION_LEGACY))

  val startingVersion: Option[DeltaStartingVersion] = options.get(STARTING_VERSION_OPTION).map {
    case "latest" => StartingVersionLatest
    case str =>
      Try(str.toLong).toOption.filter(_ >= 0).map(StartingVersion).getOrElse{
        throw DeltaSharingErrors.illegalDeltaOptionException(
          STARTING_VERSION_OPTION, str, "must be greater than or equal to zero")
      }
  }

  val startingTimestamp = options.get(STARTING_TIMESTAMP_OPTION)

  private def provideOneStartingOption(): Unit = {
    if (startingTimestamp.isDefined && startingVersion.isDefined) {
      throw DeltaSharingErrors.startingVersionAndTimestampBothSetException(
        STARTING_VERSION_OPTION,
        STARTING_TIMESTAMP_OPTION)
    }
  }

  provideOneStartingOption()
}


/**
 * Options for the Delta data source.
 */
class DeltaSharingOptions(
  @transient protected[delta] val options: CaseInsensitiveMap[String])
  extends DeltaSharingReadOptions with Serializable {

  // skipping verifyOptions(options) as delta sharing client doesn't support log yet.

  def this(options: Map[String, String]) = this(CaseInsensitiveMap(options))
}

object DeltaSharingOptions extends Logging {

  val MAX_FILES_PER_TRIGGER_OPTION = "maxFilesPerTrigger"
  val MAX_FILES_PER_TRIGGER_OPTION_DEFAULT = 1000
  val MAX_BYTES_PER_TRIGGER_OPTION = "maxBytesPerTrigger"
  val IGNORE_CHANGES_OPTION = "ignoreChanges"
  val IGNORE_DELETES_OPTION = "ignoreDeletes"

  val STARTING_VERSION_OPTION = "startingVersion"
  val STARTING_TIMESTAMP_OPTION = "startingTimestamp"
  val CDC_START_VERSION = "startingVersion"
  val CDC_START_TIMESTAMP = "startingTimestamp"
  val CDC_END_VERSION = "endingVersion"
  val CDC_END_TIMESTAMP = "endingTimestamp"
  val CDC_READ_OPTION = "readChangeFeed"
  val CDC_READ_OPTION_LEGACY = "readChangeData"

  val validOptionKeys : Set[String] = Set(
    IGNORE_CHANGES_OPTION,
    IGNORE_DELETES_OPTION,
    STARTING_TIMESTAMP_OPTION,
    STARTING_VERSION_OPTION,
    CDC_READ_OPTION,
    CDC_READ_OPTION_LEGACY,
    CDC_START_TIMESTAMP,
    CDC_END_TIMESTAMP,
    CDC_START_VERSION,
    CDC_END_VERSION,
    "queryName",
    "checkpointLocation",
    "path",
    "timestampAsOf",
    "versionAsOf"
  )
}

/**
 * Definitions for the starting version of a Delta stream.
 */
sealed trait DeltaStartingVersion
case object StartingVersionLatest extends DeltaStartingVersion
case class StartingVersion(version: Long) extends DeltaStartingVersion