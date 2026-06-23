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

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkFunSuite
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.test.SharedSparkSession

import io.delta.sharing.client.DeltaSharingProfileProvider
import io.delta.sharing.client.model.{DeltaTableFiles, Table => DeltaSharingTable}

class RemoteDeltaCDFRelationSuite extends SparkFunSuite with SharedSparkSession {

  private val table = DeltaSharingTable("fe", "fi", "fo")

  private def cdfRelation(
      client: TestDeltaSharingClient): RemoteDeltaCDFRelation = {
    val snapshot = new RemoteSnapshot(new Path("test"), client, table)
    RemoteDeltaCDFRelation(
      SparkSession.active, snapshot, client, table, Map("readChangeFeed" -> "true"))
  }

  test("wrapCDFBuildScan default implementation runs the body as-is") {
    val provider = new TestDeltaSharingProfileProvider
    // The result of the body is returned unchanged.
    assert(provider.wrapCDFBuildScan("result") === "result")
    // The body is evaluated exactly once.
    var evaluations = 0
    val result = provider.wrapCDFBuildScan {
      evaluations += 1
      42
    }
    assert(result === 42)
    assert(evaluations === 1)
  }

  // Positive case: with a pass-through wrapper, buildScan runs the full change data feed read path
  // (fetching the CDF files and assembling the add/cdc/remove file indexes into the change data
  // scan) inside the provider's wrapCDFBuildScan, and produces the change data feed scan.
  test("buildScan builds the change data feed scan within the provider's wrapCDFBuildScan") {
    val provider = new RecordingProfileProvider
    val client = new CountingCDFClient(provider)
    val relation = cdfRelation(client)

    // The relation exposes the change data feed schema: the table columns plus the CDF metadata
    // columns produced by reading change data.
    val cdfColumns = Seq("_change_type", "_commit_version", "_commit_timestamp")
    assert(cdfColumns.forall(relation.schema.fieldNames.contains))

    val rdd: RDD[Row] = relation.buildScan(relation.schema.fieldNames, Array.empty[Filter])
    assert(provider.invoked, "buildScan did not run within wrapCDFBuildScan")
    assert(client.getCDFFilesCalls >= 1, "the change data feed scan body should have run")
    assert(rdd != null)
    assert(rdd.partitions.nonEmpty, "expected a non-empty change data feed scan")
  }

  // Negative case: when the provider's wrapper aborts (throws), buildScan must propagate the
  // failure and the change data feed scan body must not run at all.
  test("buildScan does not run the change data feed scan when wrapCDFBuildScan fails") {
    val provider = new ThrowingProfileProvider
    val client = new CountingCDFClient(provider)
    val relation = cdfRelation(client)

    val ex = intercept[RuntimeException] {
      relation.buildScan(Array("_change_type"), Array.empty[Filter])
    }
    assert(ex.getMessage === ThrowingProfileProvider.Sentinel)
    assert(provider.invoked, "wrapCDFBuildScan should have been entered")
    assert(client.getCDFFilesCalls === 0, "the scan body must not run when the wrapper aborts")
  }
}

object ThrowingProfileProvider {
  val Sentinel = "wrapCDFBuildScan-aborted"
}

/** A provider whose [[wrapCDFBuildScan]] is a pass-through that records that it ran. */
class RecordingProfileProvider extends TestDeltaSharingProfileProvider {
  @volatile var invoked = false

  override def wrapCDFBuildScan[T](body: => T): T = {
    invoked = true
    body
  }
}

/** A provider whose [[wrapCDFBuildScan]] aborts before running the scan body. */
class ThrowingProfileProvider extends TestDeltaSharingProfileProvider {
  @volatile var invoked = false

  override def wrapCDFBuildScan[T](body: => T): T = {
    invoked = true
    throw new RuntimeException(ThrowingProfileProvider.Sentinel)
  }
}

/** A test client that counts how many times the change data feed scan body fetches CDF files. */
class CountingCDFClient(provider: DeltaSharingProfileProvider)
    extends TestDeltaSharingClient(profileProvider = provider) {
  @volatile var getCDFFilesCalls = 0

  override def getCDFFiles(
      table: DeltaSharingTable,
      cdfOptions: Map[String, String],
      includeHistoricalMetadata: Boolean,
      fileIdHash: Option[String]): DeltaTableFiles = {
    getCDFFilesCalls += 1
    super.getCDFFiles(table, cdfOptions, includeHistoricalMetadata, fileIdHash)
  }
}
