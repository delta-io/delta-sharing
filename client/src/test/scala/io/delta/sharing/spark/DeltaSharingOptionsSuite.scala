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

import java.util.Locale

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.QueryTest


class DeltaSharingOptionsSuite extends SparkFunSuite {

//  import testImplicits._

  test("Default options") {
    val options = new DeltaSharingOptions(Map.empty[String, String])
    assert(options.maxFilesPerTrigger.isEmpty)
    assert(options.maxBytesPerTrigger.isEmpty)
    assert(options.maxVersionsPerRpc.isEmpty)
    assert(!options.ignoreChanges)
    assert(!options.ignoreDeletes)
    assert(!options.readChangeFeed)
    assert(options.startingVersion.isEmpty)
    assert(options.startingTimestamp.isEmpty)
  }

  test("Convert successfully") {
    var options = new DeltaSharingOptions(Map(
      "maxFilesPerTrigger" -> "11",
      "maxBytesPerTrigger" -> "12",
      "maxVersionsPerRpc" -> "15",
      "ignoreChanges" -> "true",
      "ignoreDeletes" -> "true",
      "readChangeFeed" -> "true",
      "startingVersion" -> "13",
      "versionAsOf" -> "14"
    ))
    assert(options.maxFilesPerTrigger == Some(11))
    assert(options.maxBytesPerTrigger == Some(12))
    assert(options.maxVersionsPerRpc == Some(15))
    assert(options.ignoreChanges)
    assert(options.ignoreDeletes)
    assert(options.readChangeFeed)
    assert(options.startingVersion == Some(StartingVersion(13)))
    assert(options.versionAsOf == Some(14))

    options = new DeltaSharingOptions(Map(
      "maxBytesPerTrigger" -> "12k",
      "ignoreChanges" -> "false",
      "ignoreDeletes" -> "false",
      "readChangeData" -> "true",
      "timestampAsOf" -> "2021-01-01T00:00:00-01:00"
    ))
    assert(options.maxBytesPerTrigger == Some(12288))
    assert(!options.ignoreChanges)
    assert(!options.ignoreDeletes)
    assert(options.readChangeFeed)
    assert(options.timestampAsOf == Some("2021-01-01T01:00:00Z"))

    // Non parsed options remain in the CaseInsensitiveMap
    options = new DeltaSharingOptions(Map(
      "notReservedOption" -> "random",
      "endingVersion" -> "2",
      "endingTimestamp" -> "2020-01-01"
    ))
    assert(options.options.get(DeltaSharingOptions.CDF_END_VERSION) == Some("2"))
    assert(options.options.get(DeltaSharingOptions.CDF_END_TIMESTAMP) == Some("2020-01-01"))
    assert(options.options.get("notreservedoption") == Some("random"))
  }

  test("Parse cdfOptions map successfully") {
    var options = new DeltaSharingOptions(Map(
      "readChangeFeed" -> "true",
      "startingVersion" -> "15",
      "endingTimestamp" -> "2022-01-01T00:00:00-02:00"
    ))
    assert(options.cdfOptions.size == 3)
    assert(options.cdfOptions.get(DeltaSharingOptions.CDF_READ_OPTION) == Some("true"))
    assert(options.cdfOptions.get(DeltaSharingOptions.CDF_START_VERSION) == Some("15"))
    assert(options.cdfOptions.get(DeltaSharingOptions.CDF_END_TIMESTAMP) ==
      Some("2022-01-01T02:00:00Z"))

    options = new DeltaSharingOptions(Map(
      "readChangeData" -> "true",
      "startingTimestamp" -> "2022-01-01T00:00:00-03:00",
      "endingVersion" -> "16"
    ))
    assert(options.cdfOptions.size == 3)
    assert(options.cdfOptions.get(DeltaSharingOptions.CDF_READ_OPTION_LEGACY) == Some("true"))
    assert(options.cdfOptions.get(DeltaSharingOptions.CDF_START_TIMESTAMP)
      == Some("2022-01-01T03:00:00Z"))
    assert(options.cdfOptions.get(DeltaSharingOptions.CDF_END_VERSION) == Some("16"))

    // startingTimestamp won't be considered as cdf options if readChangeFeed is not set
    options = new DeltaSharingOptions(Map(
      "startingTimestamp" -> "2022-01-01T00:00:00Z",
      "endingVersion" -> "16"
    ))
    assert(options.cdfOptions.isEmpty)
  }

  test("timestamp formatted correctly") {
    // various input string of timestamp is supported
    var options = new DeltaSharingOptions(Map(
      "readChangeData" -> "true",
      "startingTimestamp" -> "2022",
      "endingTimestamp" -> "2022-01",
      "timestampAsOf" -> "2022-01-01 00:00"
    ))
    // only checking the existence, because it's not sure in which timezone the test is executed.
    assert(options.timestampAsOf.isDefined)
    assert(options.cdfOptions.get(DeltaSharingOptions.CDF_START_TIMESTAMP).isDefined)
    assert(options.cdfOptions.get(DeltaSharingOptions.CDF_END_TIMESTAMP).isDefined)

    options = new DeltaSharingOptions(Map(
      "readChangeData" -> "true",
      "startingTimestamp" -> "2022-01-01T00:00:00",
      "endingTimestamp" -> "2022-01-01 00:00:00Z",
      "timestampAsOf" -> "2022-01-01 00:00:00+01:00"
    ))
    // only checking the existence, because it's not sure in which timezone the test is executed.
    assert(options.timestampAsOf.isDefined)
    assert(options.cdfOptions.get(DeltaSharingOptions.CDF_START_TIMESTAMP).isDefined)
    assert(options.cdfOptions.get(DeltaSharingOptions.CDF_END_TIMESTAMP).isDefined)
  }

  test("timestamp format exceptions") {
    var errorMessage = intercept[IllegalArgumentException] {
      new DeltaSharingOptions(Map("timestampAsOf" -> "202"))
    }.getMessage
    assert(errorMessage.contains("The provided timestamp (202) cannot be converted to a valid " +
      "timestamp."))

    errorMessage = intercept[IllegalArgumentException] {
      new DeltaSharingOptions(Map("timestampAsOf" -> "a"))
    }.getMessage
    assert(errorMessage.contains("The provided timestamp (a) cannot be converted to a valid " +
      "timestamp."))
  }

  test("exceptions") {
    // Boolean required
    var errorMessage = intercept[IllegalArgumentException] {
      new DeltaSharingOptions(Map("ignoreChanges" -> "1"))
    }.getMessage
    assert(errorMessage.contains(
      "Invalid value '1' for option 'ignoreChanges', must be 'true' or 'false'"))

    errorMessage = intercept[IllegalArgumentException] {
      new DeltaSharingOptions(Map("ignoreDeletes" -> "1"))
    }.getMessage
    assert(errorMessage.contains(
      "Invalid value '1' for option 'ignoreDeletes', must be 'true' or 'false'"))

    errorMessage = intercept[IllegalArgumentException] {
      new DeltaSharingOptions(Map("readChangeFeed" -> "1"))
    }.getMessage
    assert(errorMessage.contains(
      "Invalid value '1' for option 'readChangeFeed', must be 'true' or 'false'"))

    errorMessage = intercept[IllegalArgumentException] {
      new DeltaSharingOptions(Map("readChangeData" -> "1"))
    }.getMessage
    assert(errorMessage.contains(
      "Invalid value '1' for option 'readChangeData', must be 'true' or 'false'"))

    // Integer or bytes
    errorMessage = intercept[IllegalArgumentException] {
      new DeltaSharingOptions(Map("versionAsOf" -> "x3"))
    }.getMessage
    assert(errorMessage.contains("Invalid value 'x3' for option 'versionAsOf', must be an integer" +
      " greater than or equal to zero"))

    errorMessage = intercept[IllegalArgumentException] {
      new DeltaSharingOptions(Map("maxFilesPerTrigger" -> "-1"))
    }.getMessage
    assert(errorMessage.contains(
      "Invalid value '-1' for option 'maxFilesPerTrigger', must be a positive integer"))

    errorMessage = intercept[IllegalArgumentException] {
      new DeltaSharingOptions(Map("maxBytesPerTrigger" -> "2mg"))
    }.getMessage
    assert(errorMessage.contains("Invalid value '2mg' for option 'maxBytesPerTrigger', must be " +
      "a size configuration such as '10g'"))

    errorMessage = intercept[IllegalArgumentException] {
      new DeltaSharingOptions(Map("maxVersionsPerRpc" -> "-1"))
    }.getMessage
    assert(errorMessage.contains(
      "Invalid value '-1' for option 'maxVersionsPerRpc', must be a positive integer"))

    // only one of options can be set
    errorMessage = intercept[IllegalArgumentException] {
      new DeltaSharingOptions(Map("startingVersion" -> "1", "startingTimestamp" -> "2020"))
    }.getMessage
    assert(errorMessage.contains("Please either provide 'startingVersion' or 'startingTimestamp'"))

    errorMessage = intercept[IllegalArgumentException] {
      new DeltaSharingOptions(Map("versionAsOf" -> "1", "timestampAsOf" -> "2020"))
    }.getMessage
    assert(errorMessage.contains("Please either provide 'versionAsOf' or 'timestampAsOf'"))

    errorMessage = intercept[IllegalArgumentException] {
      new DeltaSharingOptions(Map("responseFormat" -> "abc"))
    }.getMessage
    assert(errorMessage.contains("The user input must be one of:{parquet, delta}."))
  }
}
