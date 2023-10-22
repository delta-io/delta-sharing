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
import org.apache.spark.sql.catalyst.expressions.{Cast, Literal}
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateTimeUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.TimestampType

trait DeltaSharingOptionParser {
  protected def options: CaseInsensitiveMap[String]

  def toBoolean(input: String, name: String): Boolean = {
    Try(input.toBoolean).toOption.getOrElse {
      throw DeltaSharingErrors.illegalDeltaSharingOptionException(
        name, input, "must be 'true' or 'false'")
    }
  }
}

trait DeltaSharingReadOptions extends DeltaSharingOptionParser {
  import DeltaSharingOptions._

  val maxFilesPerTrigger = options.get(MAX_FILES_PER_TRIGGER_OPTION).map { str =>
    Try(str.toInt).toOption.filter(_ > 0).getOrElse {
      throw DeltaSharingErrors.illegalDeltaSharingOptionException(
        MAX_FILES_PER_TRIGGER_OPTION, str, "must be a positive integer")
    }
  }

  val maxBytesPerTrigger = options.get(MAX_BYTES_PER_TRIGGER_OPTION).map { str =>
    Try(JavaUtils.byteStringAs(str, ByteUnit.BYTE)).toOption.filter(_ > 0).getOrElse {
      throw DeltaSharingErrors.illegalDeltaSharingOptionException(
        MAX_BYTES_PER_TRIGGER_OPTION, str, "must be a size configuration such as '10g'")
    }
  }

  val maxVersionsPerRpc: Option[Int] = options.get(MAX_VERSIONS_PER_RPC).map { str =>
    Try(str.toInt).toOption.filter(_ > 0).getOrElse {
      throw DeltaSharingErrors.illegalDeltaSharingOptionException(
        MAX_VERSIONS_PER_RPC, str, "must be a positive integer")
    }
  }

  val ignoreChanges = options.get(IGNORE_CHANGES_OPTION).exists(toBoolean(_, IGNORE_CHANGES_OPTION))

  val ignoreDeletes = options.get(IGNORE_DELETES_OPTION).exists(toBoolean(_, IGNORE_DELETES_OPTION))

  val skipChangeCommits = options.get(SKIP_CHANGE_COMMITS_OPTION)
    .exists(toBoolean(_, SKIP_CHANGE_COMMITS_OPTION))

  val readChangeFeed = options.get(CDF_READ_OPTION).exists(toBoolean(_, CDF_READ_OPTION)) ||
    options.get(CDF_READ_OPTION_LEGACY).exists(toBoolean(_, CDF_READ_OPTION_LEGACY))

  val startingVersion: Option[DeltaStartingVersion] = options.get(STARTING_VERSION_OPTION).map {
    case "latest" => StartingVersionLatest
    case str =>
      Try(str.toLong).toOption.filter(_ >= 0).map(StartingVersion).getOrElse{
        throw DeltaSharingErrors.illegalDeltaSharingOptionException(
          STARTING_VERSION_OPTION, str, "must be greater than or equal to zero")
      }
  }

  val startingTimestamp = options.get(STARTING_TIMESTAMP_OPTION).map(getFormattedTimestamp(_))

  val cdfOptions: Map[String, String] = prepareCdfOptions()

  val versionAsOf = options.get(TIME_TRAVEL_VERSION).map { str =>
    Try(str.toLong).toOption.filter(_ >= 0).getOrElse {
      throw DeltaSharingErrors.illegalDeltaSharingOptionException(
        TIME_TRAVEL_VERSION, str, "must be an integer greater than or equal to zero")
    }
  }

  val timestampAsOf = options.get(TIME_TRAVEL_TIMESTAMP).map(getFormattedTimestamp(_))

  val responseFormat = options.get(RESPONSE_FORMAT).map { str =>
    if (!(str == RESPONSE_FORMAT_PARQUET || str == RESPONSE_FORMAT_DELTA)) {
      throw DeltaSharingErrors.illegalDeltaSharingOptionException(
        RESPONSE_FORMAT, str,
        s"The user input must be one of:{$RESPONSE_FORMAT_PARQUET, $RESPONSE_FORMAT_DELTA}."
      )
    }
    str
  }.getOrElse(RESPONSE_FORMAT_PARQUET)

  def isTimeTravel: Boolean = versionAsOf.isDefined || timestampAsOf.isDefined

  // Parse the input timestamp string and TimestampType, and generate a formatted timestamp string
  // in the ISO8601 format, in the UTC timezone, such as 2022-01-01T00:00:00Z.
  // The input string is quite flexible, and can be in any timezone, examples of accepted format:
  // "2022", "2022-01-01", "2022-01-01 00:00:00" "2022-01-01T00:00:00-08:00", etc.
  private def getFormattedTimestamp(str: String): String = {
    val castResult = new Cast(
    Literal(str), TimestampType, Option(SQLConf.get.sessionLocalTimeZone)).eval()
    if (castResult == null) {
      throw DeltaSharingErrors.timestampInvalid(str)
    }
    DateTimeUtils.toJavaTimestamp(castResult.asInstanceOf[java.lang.Long]).toInstant.toString
  }

  private def prepareCdfOptions(): Map[String, String] = {
    if (readChangeFeed) {
      validCdfOptions.filter(option => options.contains(option._1)).map(option =>
        if ((option._1 == CDF_START_TIMESTAMP) || (option._1 == CDF_END_TIMESTAMP)) {
          option._1 -> getFormattedTimestamp(options.get(option._1).get)
        } else {
          option._1 -> options.get(option._1).get
        }
      )
    } else {
     Map.empty[String, String]
    }
  }

  private def validateOneStartingOption(): Unit = {
    if (startingTimestamp.isDefined && startingVersion.isDefined) {
      throw DeltaSharingErrors.versionAndTimestampBothSetException(
        STARTING_VERSION_OPTION,
        STARTING_TIMESTAMP_OPTION)
    }
  }

  private def validateOneTimeTravelOption(): Unit = {
    if (versionAsOf.isDefined && timestampAsOf.isDefined) {
      throw DeltaSharingErrors.versionAndTimestampBothSetException(
        TIME_TRAVEL_VERSION,
        TIME_TRAVEL_TIMESTAMP)
    }
  }

  validateOneStartingOption()
  validateOneTimeTravelOption()
}


/**
 * Options for the Delta Sharing data source.
 */
class DeltaSharingOptions(
  @transient protected[spark] val options: CaseInsensitiveMap[String])
  extends DeltaSharingReadOptions with Serializable {

  // skipping verifyOptions(options) as delta sharing client doesn't support log yet.

  def this(options: Map[String, String]) = this(CaseInsensitiveMap(options))
}

object DeltaSharingOptions extends Logging {

  val MAX_FILES_PER_TRIGGER_OPTION = "maxFilesPerTrigger"
  val MAX_FILES_PER_TRIGGER_OPTION_DEFAULT = 1000
  val MAX_BYTES_PER_TRIGGER_OPTION = "maxBytesPerTrigger"
  // a delta sharing specific parameter, used to sepcify the max number of versions to query in
  // one rpc in a streaming job.
  val MAX_VERSIONS_PER_RPC = "maxVersionsPerRpc"
  val MAX_VERSIONS_PER_RPC_DEFAULT = 100
  val IGNORE_CHANGES_OPTION = "ignoreChanges"
  val IGNORE_DELETES_OPTION = "ignoreDeletes"
  val SKIP_CHANGE_COMMITS_OPTION = "skipChangeCommits"

  val STARTING_VERSION_OPTION = "startingVersion"
  val STARTING_TIMESTAMP_OPTION = "startingTimestamp"
  val CDF_START_VERSION = "startingVersion"
  val CDF_START_TIMESTAMP = "startingTimestamp"
  val CDF_END_VERSION = "endingVersion"
  val CDF_END_TIMESTAMP = "endingTimestamp"
  val CDF_READ_OPTION = "readChangeFeed"
  val CDF_READ_OPTION_LEGACY = "readChangeData"

  val TIME_TRAVEL_VERSION = "versionAsOf"
  val TIME_TRAVEL_TIMESTAMP = "timestampAsOf"

  val RESPONSE_FORMAT = "responseFormat"

  val RESPONSE_FORMAT_PARQUET = "parquet"
  val RESPONSE_FORMAT_DELTA = "delta"

  val validCdfOptions = Map(
    CDF_READ_OPTION -> "",
    CDF_READ_OPTION_LEGACY -> "",
    CDF_START_TIMESTAMP -> "",
    CDF_END_TIMESTAMP -> "",
    CDF_START_VERSION -> "",
    CDF_END_VERSION -> ""
  )
}

/**
 * Definitions for the starting version of a Delta stream.
 */
sealed trait DeltaStartingVersion
case object StartingVersionLatest extends DeltaStartingVersion
case class StartingVersion(version: Long) extends DeltaStartingVersion
