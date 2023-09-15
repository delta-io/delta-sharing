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

package io.delta.sharing.client

import java.sql.Timestamp

import org.apache.http.HttpHeaders
import org.apache.http.client.methods.HttpGet

import io.delta.sharing.client.model._
import io.delta.sharing.client.util.UnexpectedHttpStatus

// scalastyle:off maxLineLength
class DeltaSharingRestClientDeltaSuite extends DeltaSharingIntegrationTest {

  private def getDeltaSharingClientWithDeltaResponse: DeltaSharingRestClient = {
    new DeltaSharingRestClient(
      testProfileProvider,
      sslTrustAll = true,
      responseFormat = DeltaSharingRestClient.RESPONSE_FORMAT_DELTA
    )
  }

  integrationTest("Check headers") {
    val httpRequest = new HttpGet("random_url")

    val client = new DeltaSharingRestClient(testProfileProvider)
    var h = client.prepareHeaders(httpRequest).getFirstHeader(DeltaSharingRestClient.DELTA_SHARING_CAPABILITIES_HEADER)
    // scalastyle:off caselocale
    assert(h.getValue.toLowerCase().contains(s"responseformat=${DeltaSharingRestClient.RESPONSE_FORMAT_PARQUET}"))

    val deltaClient = new DeltaSharingRestClient(
      testProfileProvider,
      responseFormat = DeltaSharingRestClient.RESPONSE_FORMAT_DELTA
    )
    h = deltaClient.prepareHeaders(httpRequest).getFirstHeader(DeltaSharingRestClient.DELTA_SHARING_CAPABILITIES_HEADER)
    // scalastyle:off caselocale
    assert(h.getValue.toLowerCase().contains(s"responseformat=${DeltaSharingRestClient.RESPONSE_FORMAT_DELTA}"))
  }

  integrationTest("getMetadata") {
    val client = getDeltaSharingClientWithDeltaResponse
    try {
      val tableMatadata =
        client.getMetadata(Table(name = "table2", schema = "default", share = "share2"))
      assert(null == tableMatadata.protocol)
      assert(null == tableMatadata.metadata)
      assert(2 == tableMatadata.lines.size)
      assert(tableMatadata.lines(0) == """{"protocol":{"deltaProtocol":{"minReaderVersion":1,"minWriterVersion":2}}}""")
      assert(tableMatadata.lines(1).contains("""{"deltaMetadata":{"id":"f8d5c169-3d01-4ca3-ad9e-7dc3355aedb2","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"eventTime\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},{\"name\":\"date\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":["date"],"configuration":{},"createdTime":1619652806049"""))
    } finally {
      client.close()
    }
  }

  integrationTest("getMetadata with configuration") {
    val client = getDeltaSharingClientWithDeltaResponse
    try {
      val tableMatadata =
        client.getMetadata(Table(name = "cdf_table_cdf_enabled", schema = "default", share = "share8"))
      assert(tableMatadata.lines(1).contains("""{"deltaMetadata":{"id":"16736144-3306-4577-807a-d3f899b77670","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"age\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"birthday\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{"delta.enableChangeDataFeed":"true"},"createdTime":1651272615011"""))
    } finally {
      client.close()
    }
  }

  private def checkDeltaTableFilesBasics(
    tableFiles: DeltaTableFiles,
    expectedVersion: Int,
    expectedNumLines: Int,
    minWriterVersion: Int = 4): Unit = {
    assert(expectedVersion == tableFiles.version)
    assert(null == tableFiles.protocol)
    assert(null == tableFiles.metadata)
    assert(Nil == tableFiles.files)
    assert(expectedNumLines == tableFiles.lines.size)
    if (expectedNumLines > 0) {
      assert(tableFiles.lines(0) == s"""{"protocol":{"deltaProtocol":{"minReaderVersion":1,"minWriterVersion":$minWriterVersion}}}""")
    }
  }

  integrationTest("getFiles") {
    def verifyTableFiles(tableFiles: DeltaTableFiles): Unit = {
      checkDeltaTableFilesBasics(tableFiles, expectedVersion = 2, expectedNumLines = 4, minWriterVersion = 2)
      assert(tableFiles.lines(1).contains("""{"deltaMetadata":{"id":"f8d5c169-3d01-4ca3-ad9e-7dc3355aedb2","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"eventTime\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},{\"name\":\"date\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":["date"],"configuration":{},"createdTime":1619652806049"""))
      assert(tableFiles.lines(2).startsWith("""{"file":{"id":"9f1a49539c5cffe1ea7f9e055d5c003c","""))
      assert(tableFiles.lines(2).contains("""deltaSingleAction":{"add":{"path":"https://delta-exchange-test.s3.us-west-2.amazonaws.com/delta-exchange-test/table2/date%3D2021-04-28/part-00000-8b0086f2-7b27-4935-ac5a-8ed6215a6640.c000.snappy.parquet?X-Amz-Algorithm="""))
      assert(tableFiles.lines(2).contains(""","partitionValues":{"date":"2021-04-28"},"size":573,"modificationTime":1619652839000,"dataChange":false,"stats":"{\"numRecords\":1,\"minValues\":{\"eventTime\":\"2021-04-28T23:33:57.955Z\"},\"maxValues\":{\"eventTime\":\"2021-04-28T23:33:57.955Z\"},\"nullCount\":{\"eventTime\":0}}""""))
      assert(tableFiles.lines(3).startsWith("""{"file":{"id":"cd2209b32f5ed5305922dd50f5908a75","""))
      assert(tableFiles.lines(3).contains(""""deltaSingleAction":{"add":{"path":"https://delta-exchange-test.s3.us-west-2.amazonaws.com/delta-exchange-test/table2/date%3D2021-04-28/part-00000-591723a8-6a27-4240-a90e-57426f4736d2.c000.snappy.parquet?X-Amz-Algorithm="""))
      assert(tableFiles.lines(3).contains(""","partitionValues":{"date":"2021-04-28"},"size":573,"modificationTime":1619652832000,"dataChange":false,"stats":"{\"numRecords\":1,\"minValues\":{\"eventTime\":\"2021-04-28T23:33:48.719Z\"},\"maxValues\":{\"eventTime\":\"2021-04-28T23:33:48.719Z\"},\"nullCount\":{\"eventTime\":0}}""""))
      // Refresh token should be returned in latest snapshot query
      assert(tableFiles.refreshToken.nonEmpty)
    }

    val client = getDeltaSharingClientWithDeltaResponse
    val table = Table(name = "table2", schema = "default", share = "share2")
    try {
      val tableFiles =
        client.getFiles(
          table,
          predicates = Nil,
          limit = None,
          versionAsOf = None,
          timestampAsOf = None,
          jsonPredicateHints = None,
          refreshToken = None)
      verifyTableFiles(tableFiles)
      val refreshedTableFiles = client.getFiles(
        table,
        predicates = Nil,
        limit = None,
        versionAsOf = None,
        timestampAsOf = None,
        jsonPredicateHints = None,
        refreshToken = tableFiles.refreshToken
      )
      verifyTableFiles(refreshedTableFiles)
    } finally {
      client.close()
    }
  }

  integrationTest("getFiles with version") {
    val client = getDeltaSharingClientWithDeltaResponse
    try {
      val tableFiles = client.getFiles(
        table = Table(name = "cdf_table_cdf_enabled", schema = "default", share = "share8"),
        predicates = Nil,
        limit = None,
        versionAsOf = Some(1L),
        timestampAsOf = None,
        jsonPredicateHints = None,
        refreshToken = None)
      checkDeltaTableFilesBasics(tableFiles, expectedVersion = 1, expectedNumLines = 5)
      assert(tableFiles.lines(1).contains("""{"deltaMetadata":{"id":"16736144-3306-4577-807a-d3f899b77670","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"age\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"birthday\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{"delta.enableChangeDataFeed":"true"},"createdTime":1651272615011"""))
      val prefix = """{"file":{"id":""""
      val version = ""","version":1,"timestamp":1651272635000,"""
      assert(tableFiles.lines(2).startsWith(prefix) && tableFiles.lines(2).contains(version))
      assert(tableFiles.lines(3).startsWith(prefix) && tableFiles.lines(3).contains(version))
      assert(tableFiles.lines(4).startsWith(prefix) && tableFiles.lines(4).contains(version))
      // Refresh token shouldn't be returned in version query
      assert(tableFiles.refreshToken.isEmpty)
    } finally {
      client.close()
    }
  }

  private def checkCdfTableCdfEnabledTableV0Metadata(line: String, version: String = "1"): Unit = {
    assert(line.contains(""""deltaMetadata":{"id":"16736144-3306-4577-807a-d3f899b77670","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"age\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"birthday\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{"delta.enableChangeDataFeed":"true"},"createdTime":1651272615011"""))
    assert(line.contains(s""""version":$version"""))
  }

  private def checkCdfTableCdfEnabledTableV1(lines: Seq[String]): Unit = {
    // VERSION 1: INSERT 3 files
    val addFilePrefix = """{"add":{"path":"https://delta-exchange-test.s3.us-west-2.amazonaws.com/delta-exchange-test/cdf_table_cdf_enabled/part-0000"""
    // VERSION 1: INSERT 3 files
    for (lineNum <- 0 to 2) {
      assert(lines(lineNum).contains(addFilePrefix))
      assert(lines(lineNum).contains(""""version":1,"timestamp":1651272635000,"expirationTimestamp":"""))
    }
  }

  private def checkCdfTableCdfEnabledTableV2ToV3(lines: Seq[String]): Unit = {
    // VERSION 2: REMOVE 1 file
    val removeFilePrefix = """{"remove":{"path":"https://delta-exchange-test.s3.us-west-2.amazonaws.com/delta-exchange-test/cdf_table_cdf_enabled/part-0000"""
    assert(lines(0).contains(removeFilePrefix))
    assert(lines(0).contains(""""version":2,"timestamp":1651272655000,"expirationTimestamp":"""))
    // VERSION 3: UPDATE 1 row: 1 remove + 1 add
    assert(lines(1).contains(removeFilePrefix))
    assert(lines(1).contains(""""version":3,"timestamp":1651272660000,"expirationTimestamp":"""))
    val addFilePrefix = """{"add":{"path":"https://delta-exchange-test.s3.us-west-2.amazonaws.com/delta-exchange-test/cdf_table_cdf_enabled/part-0000"""
    assert(lines(2).contains(addFilePrefix))
    assert(lines(2).contains(""""version":3,"timestamp":1651272660000,"expirationTimestamp":"""))
  }

  private def checkCdfTableCdfEnabledTableV5Metadata(line: String): Unit = {
    // VERSION 5: set delta.enableChangeDataFeed -> true
    assert(line.contains(""""deltaMetadata":{"id":"16736144-3306-4577-807a-d3f899b77670","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"age\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"birthday\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{"delta.enableChangeDataFeed":"true"},"createdTime":1651272615011"""))
    assert(line.contains(""""version":5,"""))
  }
  private def checkCdfTableCdfEnabledTableV4ToV5(lines: Seq[String]): Unit = {
    // VERSION 4: set delta.enableChangeDataFeed -> false
    assert(lines(0).contains(""""deltaMetadata":{"id":"16736144-3306-4577-807a-d3f899b77670","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"age\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"birthday\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{"delta.enableChangeDataFeed":"false"},"createdTime":1651272615011"""))
    assert(lines(0).contains(""""version":4,"""))
    checkCdfTableCdfEnabledTableV5Metadata(lines(1))
  }

  integrationTest("getFiles with startingVersion - success") {
    val client = getDeltaSharingClientWithDeltaResponse
    try {
      val tableFiles = client.getFiles(
        Table(name = "cdf_table_cdf_enabled", schema = "default", share = "share8"), 1L, None
      )
      checkDeltaTableFilesBasics(tableFiles, expectedVersion = 1, expectedNumLines = 10)
      checkCdfTableCdfEnabledTableV0Metadata(tableFiles.lines(1))
      checkCdfTableCdfEnabledTableV1(tableFiles.lines.slice(2, 5))
      checkCdfTableCdfEnabledTableV2ToV3(tableFiles.lines.slice(5, 8))
      checkCdfTableCdfEnabledTableV4ToV5(tableFiles.lines.slice(8, 10))
    } finally {
      client.close()
    }
  }

  integrationTest("getFiles with startingVersion/endingVersion - success 1") {
    val client = getDeltaSharingClientWithDeltaResponse
    try {
      val tableFiles = client.getFiles(
        Table(name = "cdf_table_cdf_enabled", schema = "default", share = "share8"), 1L, Some(1L)
      )
      checkDeltaTableFilesBasics(tableFiles, expectedVersion = 1, expectedNumLines = 5)
      checkCdfTableCdfEnabledTableV0Metadata(tableFiles.lines(1))
      checkCdfTableCdfEnabledTableV1(tableFiles.lines.slice(2, 5))
    } finally {
      client.close()
    }
  }

  integrationTest("getFiles with startingVersion/endingVersion - success 2") {
    val client = getDeltaSharingClientWithDeltaResponse
    try {
      val tableFiles = client.getFiles(
        Table(name = "cdf_table_cdf_enabled", schema = "default", share = "share8"), 2L, Some(3L)
      )
      checkDeltaTableFilesBasics(tableFiles, expectedVersion = 2, expectedNumLines = 5)
      checkCdfTableCdfEnabledTableV0Metadata(tableFiles.lines(1), version = "2")
      checkCdfTableCdfEnabledTableV2ToV3(tableFiles.lines.slice(2, 5))
    } finally {
      client.close()
    }
  }

  integrationTest("getFiles with startingVersion/endingVersion - success 3") {
    val client = getDeltaSharingClientWithDeltaResponse
    try {
      val tableFiles = client.getFiles(
        Table(name = "cdf_table_cdf_enabled", schema = "default", share = "share8"), 4L, Some(5L)
      )
      checkDeltaTableFilesBasics(tableFiles, expectedVersion = 4, expectedNumLines = 3)
      checkCdfTableCdfEnabledTableV4ToV5(tableFiles.lines.slice(1, 3))
    } finally {
      client.close()
    }
  }

  private def checkCdfTableCdfEnabledTableV2V3Cdc(lines: Seq[String]): Unit = {
    // VERSION 2: REMOVE 1 row
    val cdcFilePrefix = """{"cdc":{"path":"https://delta-exchange-test.s3.us-west-2.amazonaws.com/delta-exchange-test/cdf_table_cdf_enabled/_change_data/cdc-00000"""
    assert(lines(0).contains(cdcFilePrefix))
    assert(lines(0).contains(""""version":2,"timestamp":1651272655000,"expirationTimestamp":"""))
    // VERSION 3: UPDATE 1 row: 1 remove + 1 add
    assert(lines(1).contains(cdcFilePrefix))
    assert(lines(1).contains(""""version":3,"timestamp":1651272660000,"expirationTimestamp":"""))
  }

  integrationTest("getCDFFiles") {
    val client = getDeltaSharingClientWithDeltaResponse
    try {
      val cdfOptions = Map("startingVersion" -> "0", "endingVersion" -> "3")
      val tableFiles = client.getCDFFiles(
        Table(name = "cdf_table_cdf_enabled", schema = "default", share = "share8"),
        cdfOptions,
        false
      )
      checkDeltaTableFilesBasics(tableFiles, expectedVersion = 0, expectedNumLines = 7)
      checkCdfTableCdfEnabledTableV5Metadata(tableFiles.lines(1))
      checkCdfTableCdfEnabledTableV1(tableFiles.lines.slice(2, 5))
      checkCdfTableCdfEnabledTableV2V3Cdc(tableFiles.lines.slice(5, 7))
    } finally {
      client.close()
    }
  }

  private def checkNotnullToNullTableV0Metadata(line: String): Unit = {
    assert(line.contains(""""deltaMetadata":{"id":"1e2201ff-12ad-4c3b-a539-4d34e9e36680","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"nullable\":false,\"metadata\":{}}]}","partitionColumns":[],"configuration":{"delta.enableChangeDataFeed":"true"},"createdTime":1668327039667"""))
    assert(line.contains(""""version":0,"""))
  }

  private def checkNotnullToNullTableV2V3Metadata(line: String, version: String): Unit = {
    assert(line.contains(""""deltaMetadata":{"id":"1e2201ff-12ad-4c3b-a539-4d34e9e36680","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{"delta.enableChangeDataFeed":"true"},"createdTime":1668327039667"""))
    assert(line.contains(s""""version":$version"""))
  }

  private def checkNotnullToNullTableAddFiles(lines: Seq[String]): Unit = {
    // VERSION 1: INSERT 1 file
    // VERSION 3: INSERT 1 file
    val addFilePrefix = """{"add":{"path":"https://delta-exchange-test.s3.us-west-2.amazonaws.com/delta-exchange-test/streaming_notnull_to_null/part-00000-"""
    assert(lines(0).contains(addFilePrefix))
    assert(lines(1).contains(addFilePrefix))
    assert(lines(0).contains(""""version":1,"timestamp":1668327046000,"expirationTimestamp":"""))
    assert(lines(1).contains(""""version":3,"timestamp":1668327050000,"expirationTimestamp":"""))
  }

  integrationTest("getCDFFiles: more metadatas returned for includeHistoricalMetadata=true") {
    val client = getDeltaSharingClientWithDeltaResponse
    try {
      val cdfOptions = Map("startingVersion" -> "0")
      val tableFiles = client.getCDFFiles(
        Table(name = "streaming_notnull_to_null", schema = "default", share = "share8"),
        cdfOptions,
        includeHistoricalMetadata = true
      )
      checkDeltaTableFilesBasics(tableFiles, expectedVersion = 0, expectedNumLines = 5)
      checkNotnullToNullTableV0Metadata(tableFiles.lines(1))
      checkNotnullToNullTableV2V3Metadata(tableFiles.lines(3), version = "2")
      checkNotnullToNullTableAddFiles(Seq(tableFiles.lines(2), tableFiles.lines(4)))
    } finally {
      client.close()
    }
  }

  integrationTest("getCDFFiles: no additional metadatas returned for includeHistoricalMetadata=false") {
    val client = getDeltaSharingClientWithDeltaResponse
    try {
      val cdfOptions = Map("startingVersion" -> "0")
      val tableFiles = client.getCDFFiles(
        Table(name = "streaming_notnull_to_null", schema = "default", share = "share8"),
        cdfOptions,
        includeHistoricalMetadata = false
      )
      checkDeltaTableFilesBasics(tableFiles, expectedVersion = 0, expectedNumLines = 4)
      checkNotnullToNullTableV2V3Metadata(tableFiles.lines(1), version = "3")
      checkNotnullToNullTableAddFiles(tableFiles.lines.slice(2, 4))
    } finally {
      client.close()
    }
  }

  integrationTest("getCDFFiles: cdf_table_with_vacuum") {
    val client = getDeltaSharingClientWithDeltaResponse
    try {
      val cdfOptions = Map("startingVersion" -> "0")
      val tableFiles = client.getCDFFiles(
        Table(name = "cdf_table_with_vacuum", schema = "default", share = "share8"),
        cdfOptions,
        false
      )
      checkDeltaTableFilesBasics(tableFiles, expectedVersion = 0, expectedNumLines = 8)
      assert(tableFiles.lines(1).contains(""""deltaMetadata":{"id":"b960061d-dc64-4b29-8fb0-d0ddc1b29cc2","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"age\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{"delta.enableChangeDataFeed":"true"},"createdTime":1655408042120"""))
      assert(tableFiles.lines(1).contains(""""version":4"""))
      val cdcFilePrefix = """{"cdc":{"path":"https://delta-exchange-test.s3.us-west-2.amazonaws.com/delta-exchange-test/cdf_table_with_vacuum/_change_data/cdc-00000"""
      assert(tableFiles.lines(4).contains(cdcFilePrefix))
      assert(tableFiles.lines(4).contains(""""version":2,"timestamp":1655410824000,"expirationTimestamp":"""))
      assert(tableFiles.lines(7).contains(cdcFilePrefix))
      assert(tableFiles.lines(7).contains(""""version":4,"timestamp":1655410847000,"expirationTimestamp":"""))
      val addFilePrefix = """{"add":{"path":"https://delta-exchange-test.s3.us-west-2.amazonaws.com/delta-exchange-test/cdf_table_with_vacuum/part-0000"""
      for (lineNum <- 2 to 3) {
        assert(tableFiles.lines(lineNum).contains(addFilePrefix))
        assert(tableFiles.lines(lineNum).contains(""""version":1,"timestamp":1655408048000,"expirationTimestamp":"""))
      }
      for (lineNum <- 5 to 6) {
        assert(tableFiles.lines(lineNum).contains(addFilePrefix))
        assert(tableFiles.lines(lineNum).contains(""""version":3,"timestamp":1655410829000,"expirationTimestamp":"""))
      }
    } finally {
      client.close()
    }
  }
}
