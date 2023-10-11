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
import org.apache.spark.sql.catalyst.expressions.{AttributeReference => SqlAttributeReference, EqualTo => SqlEqualTo, Literal => SqlLiteral}
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{FloatType, IntegerType, LongType, StringType, StructField, StructType}

import io.delta.sharing.client.model.Table

class RemoteDeltaLogSuite extends SparkFunSuite with SharedSparkSession {

  test("RemoteSnapshot getFiles with limit and jsonPredicateHints") {
    val spark = SparkSession.active
    spark.sessionState.conf.setConfString("spark.delta.sharing.jsonPredicateHints.enabled", "true")

    // sanity check for dummy client
    val client = new TestDeltaSharingClient()
    client.getFiles(
      Table("fe", "fi", "fo"), Nil, Some(2L), None, None, Some("jsonPredicate1"), None
    )
    client.getFiles(
      Table("fe", "fi", "fo"), Nil, Some(3L), None, None, Some("jsonPredicate2"), None
    )
    assert(TestDeltaSharingClient.limits === Seq(2L, 3L))
    assert(TestDeltaSharingClient.jsonPredicateHints === Seq("jsonPredicate1", "jsonPredicate2"))
    client.clear()

    // check snapshot
    val snapshot = new RemoteSnapshot(new Path("test"), client, Table("fe", "fi", "fo"))
    val fileIndex = {
      val params = RemoteDeltaFileIndexParams(spark, snapshot, client.getProfileProvider)
      RemoteDeltaSnapshotFileIndex(params, Some(2L))
    }
    snapshot.filesForScan(Nil, Some(2L), Some("jsonPredicate1"), fileIndex)
    assert(TestDeltaSharingClient.limits === Seq(2L))
    assert(TestDeltaSharingClient.jsonPredicateHints === Seq("jsonPredicate1"))
    client.clear()

    // check RemoteDeltaSnapshotFileIndex

    // We will send a simple equal op as a SQL expression tree.
    val sqlEq = SqlEqualTo(
      SqlAttributeReference("id", IntegerType)(),
      SqlLiteral(23, IntegerType)
    )
    // The client should get json for jsonPredicateHints.
    val expectedJson =
      """{"op":"equal",
         |"children":[
         |  {"op":"column","name":"id","valueType":"int"},
         |  {"op":"literal","value":"23","valueType":"int"}]
         |}""".stripMargin.replaceAll("\n", "").replaceAll(" ", "")

    fileIndex.listFiles(Seq(sqlEq), Seq.empty)
    assert(TestDeltaSharingClient.limits === Seq(2L))
    assert(TestDeltaSharingClient.jsonPredicateHints.size === 1)
    val receivedJson = TestDeltaSharingClient.jsonPredicateHints(0)
    assert(receivedJson == expectedJson)

    spark.sessionState.conf.setConfString("spark.delta.sharing.jsonPredicateHints.enabled", "false")
    client.clear()
    fileIndex.listFiles(Seq(sqlEq), Seq.empty)
    assert(TestDeltaSharingClient.jsonPredicateHints.size === 0)
  }

  test("jsonPredicateV2Hints test") {
    val spark = SparkSession.active
    spark.sessionState.conf.setConfString("spark.delta.sharing.jsonPredicateHints.enabled", "true")

    val client = new TestDeltaSharingClient()
    val snapshot = new RemoteSnapshot(new Path("test"), client, Table("fe", "fi", "fo"))
    val fileIndex = {
      val params = RemoteDeltaFileIndexParams(spark, snapshot, client.getProfileProvider)
      RemoteDeltaSnapshotFileIndex(params, Some(2L))
    }

    // We will send a simple equal op as a SQL expression tree for partition filters.
    val partitionSqlEq = SqlEqualTo(
      SqlAttributeReference("id", IntegerType)(),
      SqlLiteral(23, IntegerType)
    )

    // We will send another equal op as a SQL expression tree for data filters.
    val dataSqlEq = SqlEqualTo(
      SqlAttributeReference("cost", FloatType)(),
      SqlLiteral(23.5.toFloat, FloatType)
    )

    // With V2 predicates disabled, the client should get json for partition filters only.
    val expectedJson =
      """{"op":"equal",
         |"children":[
         |  {"op":"column","name":"id","valueType":"int"},
         |  {"op":"literal","value":"23","valueType":"int"}]
         |}""".stripMargin.replaceAll("\n", "").replaceAll(" ", "")

    fileIndex.listFiles(Seq(partitionSqlEq), Seq(dataSqlEq))
    assert(TestDeltaSharingClient.jsonPredicateHints.size === 1)
    val receivedJson = TestDeltaSharingClient.jsonPredicateHints(0)
    assert(receivedJson == expectedJson)
    client.clear()

    // With V2 predicates enabled, the client should get json for partition and data filters
    // joined at the top level by an AND operation.
    val expectedJson2 =
      """{"op":"and","children":[
         |  {"op":"equal","children":[
         |    {"op":"column","name":"id","valueType":"int"},
         |    {"op":"literal","value":"23","valueType":"int"}]},
         |  {"op":"equal","children":[
         |    {"op":"column","name":"cost","valueType":"float"},
         |    {"op":"literal","value":"23.5","valueType":"float"}]}
         |]}""".stripMargin.replaceAll("\n", "").replaceAll(" ", "")
    spark.sessionState.conf.setConfString(
      "spark.delta.sharing.jsonPredicateV2Hints.enabled", "true"
    )
    fileIndex.listFiles(Seq(partitionSqlEq), Seq(dataSqlEq))
    assert(TestDeltaSharingClient.jsonPredicateHints.size === 1)
    val receivedJson2 = TestDeltaSharingClient.jsonPredicateHints(0)
    assert(receivedJson2 == expectedJson2)
    client.clear()

    // With json predicates disabled, we should not get anything.
    spark.sessionState.conf.setConfString("spark.delta.sharing.jsonPredicateHints.enabled", "false")
    spark.sessionState.conf.setConfString(
      "spark.delta.sharing.jsonPredicateV2Hints.enabled", "false"
    )
    fileIndex.listFiles(Seq(partitionSqlEq), Seq(dataSqlEq))
    assert(TestDeltaSharingClient.jsonPredicateHints.size === 0)
  }


  test("snapshot file index test") {
    val spark = SparkSession.active
    val client = new TestDeltaSharingClient()
    client.clear()
    val snapshot = new RemoteSnapshot(new Path("test"), client, Table("fe", "fi", "fo"))
    assert(snapshot.sizeInBytes == 100)
    assert(snapshot.metadata.numFiles == 2)
    assert(snapshot.schema("col1").nullable)
    assert(snapshot.schema("col2").nullable)
    assert(TestDeltaSharingClient.numMetadataCalled == 1)

    // Create an index without limits.
    val fileIndex = {
      val params = RemoteDeltaFileIndexParams(spark, snapshot, client.getProfileProvider)
      RemoteDeltaSnapshotFileIndex(params, None)
    }
    assert(fileIndex.partitionSchema.isEmpty)

    val listFilesResult = fileIndex.listFiles(Seq.empty, Seq.empty)
    assert(listFilesResult.size == 1)
    assert(listFilesResult(0).files.size == 4)
    assert(listFilesResult(0).files(0).getPath.toString == "delta-sharing:/prefix.test/f1/0")
    assert(listFilesResult(0).files(1).getPath.toString == "delta-sharing:/prefix.test/f2/0")
    assert(listFilesResult(0).files(2).getPath.toString == "delta-sharing:/prefix.test/f3/0")
    assert(listFilesResult(0).files(3).getPath.toString == "delta-sharing:/prefix.test/f4/0")
    client.clear()

    val inputFileList = fileIndex.inputFiles.toList
    assert(inputFileList.size == 4)
    assert(inputFileList(0) == "delta-sharing:/prefix.test/f1/0")
    assert(inputFileList(1) == "delta-sharing:/prefix.test/f2/0")
    assert(inputFileList(2) == "delta-sharing:/prefix.test/f3/0")
    assert(inputFileList(3) == "delta-sharing:/prefix.test/f4/0")

    // Test indices with limits.

    val fileIndexLimit1 = {
      val params = RemoteDeltaFileIndexParams(spark, snapshot, client.getProfileProvider)
      RemoteDeltaSnapshotFileIndex(params, Some(1L))
    }
    val listFilesResult1 = fileIndexLimit1.listFiles(Seq.empty, Seq.empty)
    assert(listFilesResult1.size == 1)
    assert(listFilesResult1(0).files.size == 1)
    assert(listFilesResult1(0).files(0).getPath.toString == "delta-sharing:/prefix.test/f1/0")
    client.clear()

    // The input files are never limited.
    val inputFileList1 = fileIndexLimit1.inputFiles.toList
    assert(inputFileList1.size == 4)
    assert(inputFileList1(0) == "delta-sharing:/prefix.test/f1/0")
    assert(inputFileList1(1) == "delta-sharing:/prefix.test/f2/0")
    assert(inputFileList1(2) == "delta-sharing:/prefix.test/f3/0")
    assert(inputFileList1(3) == "delta-sharing:/prefix.test/f4/0")

    val fileIndexLimit2 = {
      val params = RemoteDeltaFileIndexParams(spark, snapshot, client.getProfileProvider)
      RemoteDeltaSnapshotFileIndex(params, Some(2L))
    }
    val listFilesResult2 = fileIndexLimit2.listFiles(Seq.empty, Seq.empty)
    assert(listFilesResult2.size == 1)
    assert(listFilesResult2(0).files.size == 2)
    assert(listFilesResult2(0).files(0).getPath.toString == "delta-sharing:/prefix.test/f1/0")
    assert(listFilesResult2(0).files(1).getPath.toString == "delta-sharing:/prefix.test/f2/0")
    client.clear()
  }

  test("snapshot file index test with version") {
    // The only diff in this test with "snapshot file index test" is:
    //  the RemoteSnapshot is with versionAsOf = Some(1), and is used in client.getFiles,
    //  which will return version/timestamp for each file in TestDeltaSharingClient.getFiles.
    // The purpose of this test is to verify that the RemoteDeltaSnapshotFileIndex works well with
    // files with additional field returned from server.
    val spark = SparkSession.active
    val client = new TestDeltaSharingClient()
    client.clear()
    val snapshot = new RemoteSnapshot(
      new Path("test"),
      client,
      Table("fe", "fi", "fo"),
      versionAsOf = Some(1)
    )
    assert(snapshot.sizeInBytes == 100)
    assert(snapshot.metadata.numFiles == 2)
    assert(!snapshot.schema("col1").nullable)
    assert(!snapshot.schema("col2").nullable)
    assert(TestDeltaSharingClient.numMetadataCalled == 1)

    // Create an index without limits.
    val fileIndex = {
      val params = RemoteDeltaFileIndexParams(spark, snapshot, client.getProfileProvider)
      RemoteDeltaSnapshotFileIndex(params, None)
    }
    assert(fileIndex.partitionSchema.isEmpty)

    val listFilesResult = fileIndex.listFiles(Seq.empty, Seq.empty)
    assert(listFilesResult.size == 1)
    assert(listFilesResult(0).files.size == 4)
    assert(listFilesResult(0).files(0).getPath.toString == "delta-sharing:/prefix.test/f1/0")
    assert(listFilesResult(0).files(1).getPath.toString == "delta-sharing:/prefix.test/f2/0")
    assert(listFilesResult(0).files(2).getPath.toString == "delta-sharing:/prefix.test/f3/0")
    assert(listFilesResult(0).files(3).getPath.toString == "delta-sharing:/prefix.test/f4/0")
    client.clear()

    val inputFileList = fileIndex.inputFiles.toList
    assert(inputFileList.size == 4)
    assert(inputFileList(0) == "delta-sharing:/prefix.test/f1/0")
    assert(inputFileList(1) == "delta-sharing:/prefix.test/f2/0")
    assert(inputFileList(2) == "delta-sharing:/prefix.test/f3/0")
    assert(inputFileList(3) == "delta-sharing:/prefix.test/f4/0")

    // Test indices with limits.

    val fileIndexLimit1 = {
      val params = RemoteDeltaFileIndexParams(spark, snapshot, client.getProfileProvider)
      RemoteDeltaSnapshotFileIndex(params, Some(1L))
    }
    val listFilesResult1 = fileIndexLimit1.listFiles(Seq.empty, Seq.empty)
    assert(listFilesResult1.size == 1)
    assert(listFilesResult1(0).files.size == 1)
    assert(listFilesResult1(0).files(0).getPath.toString == "delta-sharing:/prefix.test/f1/0")
    client.clear()

    // The input files are never limited.
    val inputFileList1 = fileIndexLimit1.inputFiles.toList
    assert(inputFileList1.size == 4)
    assert(inputFileList1(0) == "delta-sharing:/prefix.test/f1/0")
    assert(inputFileList1(1) == "delta-sharing:/prefix.test/f2/0")
    assert(inputFileList1(2) == "delta-sharing:/prefix.test/f3/0")
    assert(inputFileList1(3) == "delta-sharing:/prefix.test/f4/0")

    val fileIndexLimit2 = {
      val params = RemoteDeltaFileIndexParams(spark, snapshot, client.getProfileProvider)
      RemoteDeltaSnapshotFileIndex(params, Some(2L))
    }
    val listFilesResult2 = fileIndexLimit2.listFiles(Seq.empty, Seq.empty)
    assert(listFilesResult2.size == 1)
    assert(listFilesResult2(0).files.size == 2)
    assert(listFilesResult2(0).files(0).getPath.toString == "delta-sharing:/prefix.test/f1/0")
    assert(listFilesResult2(0).files(1).getPath.toString == "delta-sharing:/prefix.test/f2/0")
    client.clear()
  }

  test("snapshot file index test with timestamp") {
    // The only diff in this test with "snapshot file index test" is:
    //  the RemoteSnapshot is with timestampAsOf = Some(xxx), and is used in client.getFiles,
    //  which will return version/timestamp for each file in TestDeltaSharingClient.getFiles.
    // The purpose of this test is to verify that the RemoteDeltaSnapshotFileIndex works well with
    // files with additional field returned from server.
    val spark = SparkSession.active
    val client = new TestDeltaSharingClient()
    client.clear()
    val snapshot = new RemoteSnapshot(
      new Path("test"),
      client,
      Table("fe", "fi", "fo"),
      // This is not parsed, just a place holder. But is used in TestDeltaSharingClient.
      timestampAsOf = Some(TestDeltaSharingClient.TESTING_TIMESTAMP)
    )
    assert(snapshot.sizeInBytes == 100)
    assert(snapshot.metadata.numFiles == 2)
    assert(snapshot.schema("col1").nullable)
    assert(!snapshot.schema("col2").nullable)
    assert(TestDeltaSharingClient.numMetadataCalled == 1)

    // Create an index without limits.
    val fileIndex = {
      val params = RemoteDeltaFileIndexParams(spark, snapshot, client.getProfileProvider)
      RemoteDeltaSnapshotFileIndex(params, None)
    }
    assert(fileIndex.partitionSchema.isEmpty)

    val listFilesResult = fileIndex.listFiles(Seq.empty, Seq.empty)
    assert(listFilesResult.size == 1)
    assert(listFilesResult(0).files.size == 4)
    assert(listFilesResult(0).files(0).getPath.toString == "delta-sharing:/prefix.test/f1/0")
    assert(listFilesResult(0).files(1).getPath.toString == "delta-sharing:/prefix.test/f2/0")
    assert(listFilesResult(0).files(2).getPath.toString == "delta-sharing:/prefix.test/f3/0")
    assert(listFilesResult(0).files(3).getPath.toString == "delta-sharing:/prefix.test/f4/0")
    client.clear()

    val inputFileList = fileIndex.inputFiles.toList
    assert(inputFileList.size == 4)
    assert(inputFileList(0) == "delta-sharing:/prefix.test/f1/0")
    assert(inputFileList(1) == "delta-sharing:/prefix.test/f2/0")
    assert(inputFileList(2) == "delta-sharing:/prefix.test/f3/0")
    assert(inputFileList(3) == "delta-sharing:/prefix.test/f4/0")

    // Test indices with limits.

    val fileIndexLimit1 = {
      val params = RemoteDeltaFileIndexParams(spark, snapshot, client.getProfileProvider)
      RemoteDeltaSnapshotFileIndex(params, Some(1L))
    }
    val listFilesResult1 = fileIndexLimit1.listFiles(Seq.empty, Seq.empty)
    assert(listFilesResult1.size == 1)
    assert(listFilesResult1(0).files.size == 1)
    assert(listFilesResult1(0).files(0).getPath.toString == "delta-sharing:/prefix.test/f1/0")
    client.clear()

    // The input files are never limited.
    val inputFileList1 = fileIndexLimit1.inputFiles.toList
    assert(inputFileList1.size == 4)
    assert(inputFileList1(0) == "delta-sharing:/prefix.test/f1/0")
    assert(inputFileList1(1) == "delta-sharing:/prefix.test/f2/0")
    assert(inputFileList1(2) == "delta-sharing:/prefix.test/f3/0")
    assert(inputFileList1(3) == "delta-sharing:/prefix.test/f4/0")

    val fileIndexLimit2 = {
      val params = RemoteDeltaFileIndexParams(spark, snapshot, client.getProfileProvider)
      RemoteDeltaSnapshotFileIndex(params, Some(2L))
    }
    val listFilesResult2 = fileIndexLimit2.listFiles(Seq.empty, Seq.empty)
    assert(listFilesResult2.size == 1)
    assert(listFilesResult2(0).files.size == 2)
    assert(listFilesResult2(0).files(0).getPath.toString == "delta-sharing:/prefix.test/f1/0")
    assert(listFilesResult2(0).files(1).getPath.toString == "delta-sharing:/prefix.test/f2/0")
    client.clear()
  }

  test("cdf file index test") {
    val spark = SparkSession.active

    val path = new Path("test")
    val table = Table("fe", "fi", "fo")
    val client = new TestDeltaSharingClient()

    val snapshot = new RemoteSnapshot(path, client, table)
    val params = RemoteDeltaFileIndexParams(spark, snapshot, client.getProfileProvider)

    val deltaTableFiles = client.getCDFFiles(table, Map.empty, false)

    val addFilesIndex = new RemoteDeltaCDFAddFileIndex(params, deltaTableFiles.addFiles)

    // For addFile actions, we expect the internal columns in the partition schema.
    val expectedAddFilePartitionSchema = StructType(Array(
      StructField("_commit_version", LongType),
      StructField("_commit_timestamp", LongType),
      StructField("_change_type", StringType)
    ))
    assert(addFilesIndex.partitionSchema == expectedAddFilePartitionSchema)

    val addListFilesResult = addFilesIndex.listFiles(Seq.empty, Seq.empty)
    assert(addListFilesResult.size == 1)
    assert(addListFilesResult(0).files.size == 1)
    assert(addListFilesResult(0).files(0).getPath.toString ==
      "delta-sharing:/prefix.test/cdf_add1/100")

    val addInputFileList = addFilesIndex.inputFiles.toList
    assert(addInputFileList.size == 1)
    assert(addInputFileList(0) == "delta-sharing:/prefix.test/cdf_add1/100")

    val cdcIndex = new RemoteDeltaCDCFileIndex(params, deltaTableFiles.cdfFiles)

    // For cdc actions, we expect the internal columns in the partition schema.
    val expectedCDCPartitionSchema = StructType(Array(
      StructField("_commit_version", LongType),
      StructField("_commit_timestamp", LongType)
    ))
    assert(cdcIndex.partitionSchema == expectedCDCPartitionSchema)

    val cdcListFilesResult = cdcIndex.listFiles(Seq.empty, Seq.empty)
    // We expect two partition dirs due to different versions.
    // The parition dirs can be returned in any order.
    // One partition has 1 file, and the other has 2 files; we use that to identity them.
    assert(cdcListFilesResult.size == 2)
    val (cdcP1, cdcP2) = if (cdcListFilesResult(0).files.size == 1) {
      (cdcListFilesResult(0), cdcListFilesResult(1))
    } else {
      (cdcListFilesResult(1), cdcListFilesResult(0))
    }
    assert(cdcP1.files.size == 1)
    assert(cdcP1.files(0).getPath.toString == "delta-sharing:/prefix.test/cdf_cdc1/200")
    assert(cdcP2.files.size == 2)
    assert(cdcP2.files(0).getPath.toString == "delta-sharing:/prefix.test/cdf_cdc2/300")
    assert(cdcP2.files(1).getPath.toString == "delta-sharing:/prefix.test/cdf_cdc3/310")

    val cdcInputFileList = cdcIndex.inputFiles.toList
    assert(cdcInputFileList.size == 3)
    assert(cdcInputFileList(0) == "delta-sharing:/prefix.test/cdf_cdc1/200")
    assert(cdcInputFileList(1) == "delta-sharing:/prefix.test/cdf_cdc2/300")
    assert(cdcInputFileList(2) == "delta-sharing:/prefix.test/cdf_cdc3/310")

    val removeFilesIndex = new RemoteDeltaCDFRemoveFileIndex(params, deltaTableFiles.removeFiles)

    // For remove actions, we expect the internal columns in the partition schema.
    val expectedRemoveFilePartitionSchema = StructType(Array(
      StructField("_commit_version", LongType),
      StructField("_commit_timestamp", LongType),
      StructField("_change_type", StringType)
    ))
    assert(removeFilesIndex.partitionSchema == expectedRemoveFilePartitionSchema)

    val removeListFilesResult = removeFilesIndex.listFiles(Seq.empty, Seq.empty)
    // We expect two partition dirs due to different timestamps.
    // Both of them have one file, and they can be returned in any order.
    assert(removeListFilesResult.size == 2)
    val p1 = removeListFilesResult(0)
    assert(p1.files.size == 1)
    val p2 = removeListFilesResult(1)
    assert(p2.files.size == 1)
    assert(
      (p1.files(0).getPath.toString == "delta-sharing:/prefix.test/cdf_rem1/400" &&
      p2.files(0).getPath.toString == "delta-sharing:/prefix.test/cdf_rem2/420") ||
      (p2.files(0).getPath.toString == "delta-sharing:/prefix.test/cdf_rem1/400" &&
      p1.files(0).getPath.toString == "delta-sharing:/prefix.test/cdf_rem2/420")
    )

    val removeInputFileList = removeFilesIndex.inputFiles.toList
    assert(removeInputFileList.size == 2)
    assert(removeInputFileList(0) == "delta-sharing:/prefix.test/cdf_rem1/400")
    assert(removeInputFileList(1) == "delta-sharing:/prefix.test/cdf_rem2/420")
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

  test("RemoteDeltaLog Initialized with metadata") {
    val path = new Path("profileFile")
    val table = Table(share = "share", schema = "schema", name = "table")
    val client = new TestDeltaSharingClient()
    client.clear()

    def checkGetMetadataCalledOnce(versionAsOf: Option[Long] = None, nullable: Boolean): Unit = {
      var deltaTableMetadata = client.getMetadata(table, versionAsOf, None)
      assert(TestDeltaSharingClient.numMetadataCalled == 1)

      val remoteDeltaLog = new RemoteDeltaLog(table, path, client, Some(deltaTableMetadata))
      val relation = remoteDeltaLog.createRelation(versionAsOf, None, Map.empty[String, String])
      val hadoopFsRelation = relation.asInstanceOf[HadoopFsRelation]
      val fileIndex = hadoopFsRelation.location.asInstanceOf[RemoteDeltaSnapshotFileIndex]

      // nullable indicates that the metadata is fetched for the correct version.
      val snapshot = fileIndex.params.snapshotAtAnalysis
      assert(snapshot.sizeInBytes == 100)
      assert(snapshot.metadata.numFiles == 2)
      assert(snapshot.schema("col1").nullable == nullable)
      assert(snapshot.schema("col2").nullable == nullable)

      assert(TestDeltaSharingClient.numMetadataCalled == 1)
    }

    checkGetMetadataCalledOnce(None, true)
    client.clear()
    checkGetMetadataCalledOnce(Some(1L), false)
  }
}
