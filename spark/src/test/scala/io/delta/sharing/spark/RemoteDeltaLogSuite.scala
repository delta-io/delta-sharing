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

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files

import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.test.SharedSparkSession

import io.delta.sharing.spark.model.Table


class RemoteDeltaLogSuite extends SparkFunSuite with SharedSparkSession {

  test("parsePath") {
    assert(RemoteDeltaLog.parsePath("file:///foo/bar#a.b.c") == ("file:///foo/bar", "a", "b", "c"))
    assert(RemoteDeltaLog.parsePath("file:///foo/bar#bar#a.b.c") ==
      ("file:///foo/bar#bar", "a", "b", "c"))
    assert(RemoteDeltaLog.parsePath("file:///foo/bar#bar#a.b.c ") ==
      ("file:///foo/bar#bar", "a", "b", "c "))
    intercept[IllegalArgumentException] {
      RemoteDeltaLog.parsePath("file:///foo/bar")
    }
    intercept[IllegalArgumentException] {
      RemoteDeltaLog.parsePath("file:///foo/bar#a.b")
    }
    intercept[IllegalArgumentException] {
      RemoteDeltaLog.parsePath("file:///foo/bar#a.b.c.d")
    }
    intercept[IllegalArgumentException] {
      RemoteDeltaLog.parsePath("#a.b.c")
    }
    intercept[IllegalArgumentException] {
      RemoteDeltaLog.parsePath("foo#a.b.")
    }
    intercept[IllegalArgumentException] {
      RemoteDeltaLog.parsePath("foo#a.b.c.")
    }
  }

  test("RemoteSnapshot getFiles with limit") {
    val spark = SparkSession.active

    // sanity check for dummy client
    val client = new TestDeltaSharingClient()
    client.getFiles(Table("fe", "fi", "fo"), Nil, Some(2L))
    client.getFiles(Table("fe", "fi", "fo"), Nil, Some(3L))
    assert(TestDeltaSharingClient.limits === Seq(2L, 3L))
    client.clear()

    // check snapshot
    val snapshot = new RemoteSnapshot(new Path("test"), client, Table("fe", "fi", "fo"))
    snapshot.filesForScan(Nil, Some(2L), None)
    assert(TestDeltaSharingClient.limits === Seq(2L))
    client.clear()

    // check RemoteDeltaFileIndex
    val remoteDeltaLog = new RemoteDeltaLog(Table("fe", "fi", "fo"), new Path("test"), client)
    val fileIndex = {
      RemoteDeltaFileIndex(spark, remoteDeltaLog, new Path("test"), snapshot, Some(2L))
    }
    fileIndex.listFiles(Seq.empty, Seq.empty)
    assert(TestDeltaSharingClient.limits === Seq(2L))
    client.clear()
  }

  test("Limit pushdown test") {
    val testProfileFile = Files.createTempFile("delta-test", ".share").toFile
    FileUtils.writeStringToFile(testProfileFile,
      s"""{
         |  "shareCredentialsVersion": 1,
         |  "endpoint": "https://localhost:12345/delta-sharing",
         |  "bearerToken": "mock"
         |}""".stripMargin, UTF_8)

    withSQLConf("spark.delta.sharing.limitPushdown.enabled" -> "false",
      "spark.delta.sharing.client.class" -> "io.delta.sharing.spark.TestDeltaSharingClient") {
      val tablePath = testProfileFile.getCanonicalPath + "#share2.default.table2"
      withTable("delta_sharing_test") {
        sql(s"CREATE TABLE delta_sharing_test USING deltaSharing LOCATION '$tablePath'")
        sql(s"SELECT * FROM delta_sharing_test LIMIT 2").show()
        sql(s"SELECT col1, col2 FROM delta_sharing_test LIMIT 3").show()
      }
      assert(TestDeltaSharingClient.limits === Nil)
    }

    withSQLConf(
      "spark.delta.sharing.client.class" -> "io.delta.sharing.spark.TestDeltaSharingClient") {
      val tablePath = testProfileFile.getCanonicalPath + "#share2.default.table2"
      withTable("delta_sharing_test") {
        sql(s"CREATE TABLE delta_sharing_test USING deltaSharing LOCATION '$tablePath'")
        sql(s"SELECT * FROM delta_sharing_test LIMIT 2").show()
        sql(s"SELECT col1, col2 FROM delta_sharing_test LIMIT 3").show()
      }
      assert(TestDeltaSharingClient.limits === Seq(2L, 3L))
    }
  }
}
