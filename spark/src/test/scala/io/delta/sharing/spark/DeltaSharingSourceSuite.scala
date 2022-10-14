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

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.connector.read.streaming.ReadMaxFiles
import org.apache.spark.sql.test.SharedSparkSession

class DeltaSharingSourceSuite extends QueryTest
  with SharedSparkSession with DeltaSharingIntegrationTest {

  import testImplicits._

  lazy val tablePath = testProfileFile.getCanonicalPath + "#share1.default.cdf_table_cdf_enabled"

  lazy val deltaLog = RemoteDeltaLog(tablePath)

  def getSource(parameters: Map[String, String]): DeltaSharingSource = {

    val options = new DeltaSharingOptions(parameters)

    DeltaSharingSource(sqlContext.sparkSession, deltaLog, options)
  }

  integrationTest("deltasharingsource - defaultLimit") {
    val source = getSource(Map.empty[String, String])

    val defaultLimit = source.getDefaultReadLimit
    assert(defaultLimit.isInstanceOf[ReadMaxFiles])
    assert(defaultLimit.asInstanceOf[ReadMaxFiles].maxFiles ==
      DeltaSharingOptions.MAX_FILES_PER_TRIGGER_OPTION_DEFAULT)
  }

  integrationTest("deltasharingsource - startingVersion 0") {
    val source = getSource(Map(
      "ignoreChanges" -> "true",
      "ignoreDeletes" -> "true",
      "startingVersion" -> "0"
    ))
    val latestOffset = source.latestOffset(null, source.getDefaultReadLimit)

    assert(latestOffset.isInstanceOf[DeltaSharingSourceOffset])
    val offset = latestOffset.asInstanceOf[DeltaSharingSourceOffset]
    assert(offset.sourceVersion == 1)
    assert(offset.reservoirId == deltaLog.snapshot(Some(0)).metadata.id)
    assert(offset.reservoirVersion == 4)
    assert(offset.index == -1)
    assert(!offset.isStartingVersion)
  }

  integrationTest("deltasharingsource - latest") {
    val source = getSource(Map(
      "ignoreChanges" -> "true",
      "ignoreDeletes" -> "true",
      "startingVersion" -> "latest"
    ))
    val latestOffset = source.latestOffset(null, source.getDefaultReadLimit)
    assert(latestOffset == null)
  }

  integrationTest("deltasharingsource - no startingVersion") {
    val source = getSource(Map(
      "ignoreChanges" -> "true",
      "ignoreDeletes" -> "true"
    ))
    val latestOffset = source.latestOffset(null, source.getDefaultReadLimit)

    assert(latestOffset.isInstanceOf[DeltaSharingSourceOffset])
    val offset = latestOffset.asInstanceOf[DeltaSharingSourceOffset]
    assert(offset.sourceVersion == 1)
    assert(offset.reservoirId == deltaLog.snapshot().metadata.id)
    assert(offset.reservoirVersion == 6)
    assert(offset.index == -1)
    assert(!offset.isStartingVersion)
  }

  integrationTest("deltasharingsource - getBatch") {
    val source = getSource(Map(
      "ignoreChanges" -> "true",
      "ignoreDeletes" -> "true"
    ))
    val endOffset = DeltaSharingSourceOffset(
      DeltaSharingSourceOffset.VERSION_1, deltaLog.snapshot().metadata.id, 3, -1, false
    )
    val df = source.getBatch(None, endOffset)
    //    Console.println(s"-----[linzhou]------[df][${df.show()}]")
    checkAnswer(df, Nil)
    // scalastyle:on println
  }
}
