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
import org.apache.http.client.methods.{HttpGet, HttpRequestBase}

import io.delta.sharing.client.model.{
  AddCDCFile,
  AddFile,
  AddFileForCDF,
  DeltaTableFiles,
  Format,
  Metadata,
  Protocol,
  RemoveFile,
  Table
}
import io.delta.sharing.client.util.UnexpectedHttpStatus

// scalastyle:off maxLineLength
class DeltaSharingRestClientSuite extends DeltaSharingIntegrationTest {

  import DeltaSharingRestClient._

  test("parsePath") {
    assert(DeltaSharingRestClient.parsePath("file:///foo/bar#a.b.c") ==
      ParsedDeltaSharingTablePath("file:///foo/bar", "a", "b", "c"))
    assert(DeltaSharingRestClient.parsePath("file:///foo/bar#bar#a.b.c") ==
      ParsedDeltaSharingTablePath("file:///foo/bar#bar", "a", "b", "c"))
    assert(DeltaSharingRestClient.parsePath("file:///foo/bar#bar#a.b.c ") ==
      ParsedDeltaSharingTablePath("file:///foo/bar#bar", "a", "b", "c "))
    intercept[IllegalArgumentException] {
      DeltaSharingRestClient.parsePath("file:///foo/bar")
    }
    intercept[IllegalArgumentException] {
      DeltaSharingRestClient.parsePath("file:///foo/bar#a.b")
    }
    intercept[IllegalArgumentException] {
      DeltaSharingRestClient.parsePath("file:///foo/bar#a.b.c.d")
    }
    intercept[IllegalArgumentException] {
      DeltaSharingRestClient.parsePath("#a.b.c")
    }
    intercept[IllegalArgumentException] {
      DeltaSharingRestClient.parsePath("foo#a.b.")
    }
    intercept[IllegalArgumentException] {
      DeltaSharingRestClient.parsePath("foo#a.b.c.")
    }
  }

  integrationTest("Check headers") {
    val httpRequest = new HttpGet("random_url")

    def checkUserAgent(request: HttpRequestBase, containsStreaming: Boolean): Unit = {
      val h = request.getFirstHeader(HttpHeaders.USER_AGENT)
      assert(h.getValue.contains(SPARK_STRUCTURED_STREAMING) == containsStreaming)
    }

    def checkDeltaSharingCapabilities(request: HttpRequestBase, expected: String): Unit = {
      val h = request.getFirstHeader(DELTA_SHARING_CAPABILITIES_HEADER)
      assert(h.getValue == expected)
    }

    var httpRequestBase = new DeltaSharingRestClient(
      testProfileProvider, forStreaming = false, readerFeatures = "willBeIgnored").prepareHeaders(httpRequest)
    checkUserAgent(httpRequestBase, false)
    checkDeltaSharingCapabilities(httpRequestBase, "responseformat=parquet")

    val readerFeatures = "deletionVectors,columnMapping,timestampNTZ"
    httpRequestBase = new DeltaSharingRestClient(
      testProfileProvider,
      forStreaming = true,
      responseFormat = RESPONSE_FORMAT_DELTA,
      readerFeatures = readerFeatures).prepareHeaders(httpRequest)
    checkUserAgent(httpRequestBase, true)
    checkDeltaSharingCapabilities(
      httpRequestBase, s"responseformat=delta;readerfeatures=$readerFeatures"
    )

    httpRequestBase = new DeltaSharingRestClient(
      testProfileProvider,
      forStreaming = true,
      responseFormat = s"$RESPONSE_FORMAT_DELTA,$RESPONSE_FORMAT_PARQUET",
      readerFeatures = readerFeatures).prepareHeaders(httpRequest)
    checkUserAgent(httpRequestBase, true)
    checkDeltaSharingCapabilities(
      httpRequestBase, s"responseformat=delta,parquet;readerfeatures=$readerFeatures"
    )
  }

  integrationTest("listAllTables") {
    val client = new DeltaSharingRestClient(testProfileProvider, sslTrustAll = true)
    try {
      val expected = Set(
        Table(name = "table1", schema = "default", share = "share1"),
        Table(name = "table2", schema = "default", share = "share2"),
        Table(name = "table3", schema = "default", share = "share1"),
        Table(name = "table4", schema = "default", share = "share3"),
        Table(name = "table5", schema = "default", share = "share3"),
        Table(name = "table7", schema = "default", share = "share1"),
        Table(name = "table8", schema = "schema1", share = "share7"),
        Table(name = "table9", schema = "schema2", share = "share7"),
        Table(name = "cdf_table_cdf_enabled", schema = "default", share = "share8"),
        Table(name = "cdf_table_with_partition", schema = "default", share = "share8"),
        Table(name = "cdf_table_with_vacuum", schema = "default", share = "share8"),
        Table(name = "cdf_table_missing_log", schema = "default", share = "share8"),
        Table(name = "streaming_table_with_optimize", schema = "default", share = "share8"),
        Table(name = "streaming_table_metadata_protocol", schema = "default", share = "share8"),
        Table(name = "streaming_notnull_to_null", schema = "default", share = "share8"),
        Table(name = "streaming_null_to_notnull", schema = "default", share = "share8"),
        Table(name = "streaming_cdf_null_to_notnull", schema = "default", share = "share8"),
        Table(name = "streaming_cdf_table", schema = "default", share = "share8"),
        Table(name = "table_reader_version_increased", schema = "default", share = "share8"),
        Table(name = "table_with_no_metadata", schema = "default", share = "share8"),
        Table(name = "table_data_loss_with_checkpoint", schema = "default", share = "share8"),
        Table(name = "table_data_loss_no_checkpoint", schema = "default", share = "share8"),
        Table(name = "test_gzip", schema = "default", share = "share4"),
        Table(name = "table_wasb", schema = "default", share = "share_azure"),
        Table(name = "table_abfs", schema = "default", share = "share_azure"),
        Table(name = "table_gcs", schema = "default", share = "share_gcp")
      )
      assert(expected == client.listAllTables().toSet)
    } finally {
      client.close()
    }
  }

  integrationTest("getTableVersion - success") {
    val client = new DeltaSharingRestClient(testProfileProvider, sslTrustAll = true)
    try {
      assert(client.getTableVersion(Table(name = "table2", schema = "default", share = "share2")) == 2)
      assert(client.getTableVersion(Table(name = "table1", schema = "default", share = "share1")) == 2)
      assert(client.getTableVersion(Table(name = "table3", schema = "default", share = "share1")) == 4)
      assert(client.getTableVersion(Table(name = "cdf_table_cdf_enabled", schema = "default", share = "share8"),
        startingTimestamp = Some("2020-01-01T00:00:00Z")) == 0)
    } finally {
      client.close()
    }
  }

  integrationTest("getTableVersion - exceptions") {
    Seq(
      DeltaSharingRestClient.RESPONSE_FORMAT_DELTA, DeltaSharingRestClient.RESPONSE_FORMAT_PARQUET
    ).foreach {
      responseFormat =>
        val client = new DeltaSharingRestClient(
          testProfileProvider,
          sslTrustAll = true,
          responseFormat = responseFormat
        )
        try {
          val errorMessage = intercept[UnexpectedHttpStatus] {
            client.getTableVersion(Table(name = "table1", schema = "default", share = "share1"),
              startingTimestamp = Some("2020-01-01T00:00:00Z"))
          }.getMessage
          assert(errorMessage.contains("400 Bad Request"))
          assert(errorMessage.contains("Reading table by version or timestamp is not supported"))
        } finally {
          client.close()
        }
    }
  }

  integrationTest("getMetadata") {
    // The response is always in parquet format, because the client allows parquet and the
    // table is a basic table.
    Seq(
      RESPONSE_FORMAT_PARQUET,
      s"${RESPONSE_FORMAT_DELTA},${RESPONSE_FORMAT_PARQUET}",
      s"${RESPONSE_FORMAT_PARQUET},${RESPONSE_FORMAT_DELTA}"
    ).foreach { responseFormat =>
      val client = new DeltaSharingRestClient(
        testProfileProvider,
        sslTrustAll = true,
        responseFormat = responseFormat
      )
      try {
        val tableMatadata =
          client.getMetadata(Table(name = "table2", schema = "default", share = "share2"))
        assert(Protocol(minReaderVersion = 1) == tableMatadata.protocol)
        val expectedMetadata = Metadata(
          id = "f8d5c169-3d01-4ca3-ad9e-7dc3355aedb2",
          format = Format(),
          schemaString = """{"type":"struct","fields":[{"name":"eventTime","type":"timestamp","nullable":true,"metadata":{}},{"name":"date","type":"date","nullable":true,"metadata":{}}]}""",
          partitionColumns = Seq("date"))
        assert(expectedMetadata == tableMatadata.metadata)
      } finally {
        client.close()
      }
    }
  }

  integrationTest("getMetadata with configuration") {
    val client = new DeltaSharingRestClient(testProfileProvider, sslTrustAll = true)
    try {
      val tableMatadata =
        client.getMetadata(Table(name = "cdf_table_cdf_enabled", schema = "default", share = "share8"))
      assert(Protocol(minReaderVersion = 1) == tableMatadata.protocol)
      val expectedMetadata = Metadata(
        id = "16736144-3306-4577-807a-d3f899b77670",
        format = Format(),
        schemaString = """{"type":"struct","fields":[{"name":"name","type":"string","nullable":true,"metadata":{}},{"name":"age","type":"integer","nullable":true,"metadata":{}},{"name":"birthday","type":"date","nullable":true,"metadata":{}}]}""",
        configuration = Map("enableChangeDataFeed" -> "true"),
        partitionColumns = Nil)
      assert(expectedMetadata == tableMatadata.metadata)
    } finally {
      client.close()
    }
  }

  integrationTest("getMetadata with parameters") {
    val client = new DeltaSharingRestClient(testProfileProvider, sslTrustAll = true)
    try {
      val metadataV0 = Metadata(
        id = "1e2201ff-12ad-4c3b-a539-4d34e9e36680",
        format = Format(),
        schemaString = """{"type":"struct","fields":[{"name":"name","type":"string","nullable":false,"metadata":{}}]}""",
        configuration = Map("enableChangeDataFeed" -> "true"),
        partitionColumns = Nil
      )
      val responseV0 = client.getMetadata(
        Table(name = "streaming_notnull_to_null", schema = "default", share = "share8"),
        versionAsOf = Some(0)
      )
      assert(Protocol(minReaderVersion = 1) == responseV0.protocol)
      assert(metadataV0 == responseV0.metadata)

      val metadataV2 = metadataV0.copy(
        schemaString = """{"type":"struct","fields":[{"name":"name","type":"string","nullable":true,"metadata":{}}]}"""
      )
      val responseV2 = client.getMetadata(
        Table(name = "streaming_notnull_to_null", schema = "default", share = "share8"),
        versionAsOf = Some(2)
      )
      assert(Protocol(minReaderVersion = 1) == responseV0.protocol)
      assert(metadataV2 == responseV2.metadata)

      val responseTimestamp = client.getMetadata(
        Table(name = "streaming_notnull_to_null", schema = "default", share = "share8"),
        timestampAsOf = Some("2022-11-13T08:10:50Z")
      )
      assert(Protocol(minReaderVersion = 1) == responseV0.protocol)
      assert(metadataV2 == responseTimestamp.metadata)
    } finally {
      client.close()
    }
  }

  integrationTest("getMetadata with parameters - exceptions") {
    val client = new DeltaSharingRestClient(testProfileProvider, sslTrustAll = true)
    var errorMessage = intercept[UnexpectedHttpStatus] {
      client.getMetadata(
        Table(name = "streaming_notnull_to_null", schema = "default", share = "share8"),
        versionAsOf = Some(6)
      )
    }.getMessage
    assert(errorMessage.contains("Cannot time travel Delta table to version 6"))

    errorMessage = intercept[UnexpectedHttpStatus] {
      client.getMetadata(
        Table(name = "streaming_notnull_to_null", schema = "default", share = "share8"),
        timestampAsOf = Some("2021-01-01T00:00:00Z")
      )
    }.getMessage
    assert(errorMessage.contains("is before the earliest version available"))
  }

  integrationTest("getFiles") {
    def verifyTableFiles(tableFiles: DeltaTableFiles): Unit = {
      assert(tableFiles.version == 2)
      assert(Protocol(minReaderVersion = 1) == tableFiles.protocol)
      val expectedMetadata = Metadata(
        id = "f8d5c169-3d01-4ca3-ad9e-7dc3355aedb2",
        format = Format(),
        schemaString =
          """{"type":"struct","fields":[{"name":"eventTime","type":"timestamp","nullable":true,"metadata":{}},{"name":"date","type":"date","nullable":true,"metadata":{}}]}""",
        partitionColumns = Seq("date")
      )
      assert(expectedMetadata == tableFiles.metadata)
      assert(tableFiles.files.size == 2)
      val expectedFiles = Seq(
        AddFile(
          url = tableFiles.files(0).url,
          expirationTimestamp = tableFiles.files(0).expirationTimestamp,
          id = "9f1a49539c5cffe1ea7f9e055d5c003c",
          partitionValues = Map("date" -> "2021-04-28"),
          size = 573,
          stats =
            """{"numRecords":1,"minValues":{"eventTime":"2021-04-28T23:33:57.955Z"},"maxValues":{"eventTime":"2021-04-28T23:33:57.955Z"},"nullCount":{"eventTime":0}}"""
        ),
        AddFile(
          url = tableFiles.files(1).url,
          expirationTimestamp = tableFiles.files(1).expirationTimestamp,
          id = "cd2209b32f5ed5305922dd50f5908a75",
          partitionValues = Map("date" -> "2021-04-28"),
          size = 573,
          stats =
            """{"numRecords":1,"minValues":{"eventTime":"2021-04-28T23:33:48.719Z"},"maxValues":{"eventTime":"2021-04-28T23:33:48.719Z"},"nullCount":{"eventTime":0}}"""
        )
      )
      assert(expectedFiles == tableFiles.files.toList)
      assert(tableFiles.files(0).expirationTimestamp > System.currentTimeMillis())
      // Refresh token should be returned in latest snapshot query
      assert(tableFiles.refreshToken.nonEmpty)
      assert(tableFiles.refreshToken.get.nonEmpty)
    }

    // The response is always in parquet format, because the client allows parquet and the
    // table is a basic table.
    Seq(
      RESPONSE_FORMAT_PARQUET,
      s"${RESPONSE_FORMAT_DELTA},${RESPONSE_FORMAT_PARQUET}",
      s"${RESPONSE_FORMAT_PARQUET},${RESPONSE_FORMAT_DELTA}"
    ).foreach { responseFormat =>
      Seq(true, false).foreach { paginationEnabled =>
        val client = new DeltaSharingRestClient(
          testProfileProvider,
          sslTrustAll = true,
          queryTablePaginationEnabled = paginationEnabled,
          maxFilesPerReq = 1,
          responseFormat = responseFormat
        )
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
              refreshToken = None
            )
          verifyTableFiles(tableFiles)
          val refreshedTableFiles =
            client.getFiles(
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
    }
  }

  integrationTest("getFiles with version") {
    val client = new DeltaSharingRestClient(testProfileProvider, sslTrustAll = true)
    try {
      val tableFiles = client.getFiles(
        table = Table(name = "cdf_table_cdf_enabled", schema = "default", share = "share8"),
        predicates = Nil,
        limit = None,
        versionAsOf = Some(1L),
        timestampAsOf = None,
        jsonPredicateHints = None,
        refreshToken = None
      )
      assert(tableFiles.version == 1)
      assert(tableFiles.files.size == 3)
      val expectedFiles = Seq(
        AddFile(
          url = tableFiles.files(0).url,
          expirationTimestamp = tableFiles.files(0).expirationTimestamp,
          id = "60d0cf57f3e4367db154aa2c36152a1f",
          partitionValues = Map.empty,
          size = 1030,
          version = 1,
          timestamp = 1651272635000L,
          stats = """{"numRecords":1,"minValues":{"name":"1","age":1,"birthday":"2020-01-01"},"maxValues":{"name":"1","age":1,"birthday":"2020-01-01"},"nullCount":{"name":0,"age":0,"birthday":0}}"""
        ),
        AddFile(
          url = tableFiles.files(1).url,
          expirationTimestamp = tableFiles.files(1).expirationTimestamp,
          id = "d7ed708546dd70fdff9191b3e3d6448b",
          partitionValues = Map.empty,
          size = 1030,
          version = 1,
          timestamp = 1651272635000L,
          stats = """{"numRecords":1,"minValues":{"name":"3","age":3,"birthday":"2020-01-01"},"maxValues":{"name":"3","age":3,"birthday":"2020-01-01"},"nullCount":{"name":0,"age":0,"birthday":0}}"""
        ),
        AddFile(
          url = tableFiles.files(2).url,
          expirationTimestamp = tableFiles.files(2).expirationTimestamp,
          id = "a6dc5694a4ebcc9a067b19c348526ad6",
          partitionValues = Map.empty,
          size = 1030,
          version = 1,
          timestamp = 1651272635000L,
          stats = """{"numRecords":1,"minValues":{"name":"2","age":2,"birthday":"2020-01-01"},"maxValues":{"name":"2","age":2,"birthday":"2020-01-01"},"nullCount":{"name":0,"age":0,"birthday":0}}"""
        )
      )
      assert(expectedFiles == tableFiles.files.toList)
      assert(tableFiles.files(0).expirationTimestamp > System.currentTimeMillis())
      // Refresh token shouldn't be returned in version query
      assert(tableFiles.refreshToken.isEmpty)
    } finally {
      client.close()
    }
  }

  integrationTest("getFiles with version exception") {
    Seq(
      DeltaSharingRestClient.RESPONSE_FORMAT_DELTA, DeltaSharingRestClient.RESPONSE_FORMAT_PARQUET
    ).foreach {
      responseFormat =>
        val client = new DeltaSharingRestClient(
          testProfileProvider,
          sslTrustAll = true,
          responseFormat = responseFormat
        )
        try {
          val errorMessage = intercept[UnexpectedHttpStatus] {
            client.getFiles(
              table = Table(name = "table1", schema = "default", share = "share1"),
              predicates = Nil,
              limit = None,
              versionAsOf = Some(1L),
              timestampAsOf = None,
              jsonPredicateHints = None,
              refreshToken = None
            )
          }.getMessage
          assert(errorMessage.contains("Reading table by version or timestamp is not supported because history sharing is not enabled on table: share1.default.table1"))
        } finally {
          client.close()
        }
    }
  }

  integrationTest("getFiles with timestamp parsed, but too early") {
    Seq(
      DeltaSharingRestClient.RESPONSE_FORMAT_DELTA, DeltaSharingRestClient.RESPONSE_FORMAT_PARQUET
    ).foreach {
      responseFormat =>
        val client = new DeltaSharingRestClient(
          testProfileProvider,
          sslTrustAll = true,
          responseFormat = responseFormat
        )
        try {
          // This is to test that timestamp is correctly passed to the server and parsed.
          // The error message is expected as we are using a timestamp much smaller than the earliest
          // version of the table.
          // Because with undecided timezone, the timestamp string can be mapped to different versions
          val errorMessage = intercept[UnexpectedHttpStatus] {
            client.getFiles(
              table = Table(name = "cdf_table_cdf_enabled", schema = "default", share = "share8"),
              predicates = Nil,
              limit = None,
              versionAsOf = None,
              timestampAsOf = Some("2000-01-01T00:00:00Z"),
              jsonPredicateHints = None,
              refreshToken = None
            )
          }.getMessage
          assert(errorMessage.contains("The provided timestamp"))
        } finally {
          client.close()
        }
    }
  }

  integrationTest("getFiles with timestamp not supported on table1") {
    Seq(
      DeltaSharingRestClient.RESPONSE_FORMAT_DELTA, DeltaSharingRestClient.RESPONSE_FORMAT_PARQUET
    ).foreach {
      responseFormat =>
        val client = new DeltaSharingRestClient(
          testProfileProvider,
          sslTrustAll = true,
          responseFormat = responseFormat
        )
        try {
          val errorMessage = intercept[UnexpectedHttpStatus] {
            client.getFiles(
              table = Table(name = "table1", schema = "default", share = "share1"),
              predicates = Nil,
              limit = None,
              versionAsOf = None,
              timestampAsOf = Some("abc"),
              jsonPredicateHints = None,
              refreshToken = None
            )
          }.getMessage
          assert(errorMessage.contains("Reading table by version or timestamp is not supported because history sharing is not enabled on table: share1.default.table1"))
        } finally {
          client.close()
        }
    }
  }

  integrationTest("getFiles with startingVersion - success") {
    Seq(true, false).foreach { paginationEnabled =>
      val client = new DeltaSharingRestClient(
        testProfileProvider,
        sslTrustAll = true,
        queryTablePaginationEnabled = paginationEnabled,
        maxFilesPerReq = 1
      )
      try {
        val tableFiles = client.getFiles(
          Table(name = "cdf_table_cdf_enabled", schema = "default", share = "share8"),
          1L,
          None
        )
        assert(tableFiles.version == 1)
        assert(tableFiles.addFiles.size == 4)
        val expectedAddFiles = Seq(
          AddFileForCDF(
            url = tableFiles.addFiles(0).url,
            expirationTimestamp = tableFiles.addFiles(0).expirationTimestamp,
            id = "60d0cf57f3e4367db154aa2c36152a1f",
            partitionValues = Map.empty,
            size = 1030,
            stats =
              """{"numRecords":1,"minValues":{"name":"1","age":1,"birthday":"2020-01-01"},"maxValues":{"name":"1","age":1,"birthday":"2020-01-01"},"nullCount":{"name":0,"age":0,"birthday":0}}""",
            version = 1,
            timestamp = 1651272635000L
          ),
          AddFileForCDF(
            url = tableFiles.addFiles(1).url,
            expirationTimestamp = tableFiles.addFiles(1).expirationTimestamp,
            id = "a6dc5694a4ebcc9a067b19c348526ad6",
            partitionValues = Map.empty,
            size = 1030,
            stats =
              """{"numRecords":1,"minValues":{"name":"2","age":2,"birthday":"2020-01-01"},"maxValues":{"name":"2","age":2,"birthday":"2020-01-01"},"nullCount":{"name":0,"age":0,"birthday":0}}""",
            version = 1,
            timestamp = 1651272635000L
          ),
          AddFileForCDF(
            url = tableFiles.addFiles(2).url,
            expirationTimestamp = tableFiles.addFiles(2).expirationTimestamp,
            id = "d7ed708546dd70fdff9191b3e3d6448b",
            partitionValues = Map.empty,
            size = 1030,
            stats =
              """{"numRecords":1,"minValues":{"name":"3","age":3,"birthday":"2020-01-01"},"maxValues":{"name":"3","age":3,"birthday":"2020-01-01"},"nullCount":{"name":0,"age":0,"birthday":0}}""",
            version = 1,
            timestamp = 1651272635000L
          ),
          AddFileForCDF(
            url = tableFiles.addFiles(3).url,
            expirationTimestamp = tableFiles.addFiles(3).expirationTimestamp,
            id = "b875623be22c1fa1dfdeb0480fae6117",
            partitionValues = Map.empty,
            size = 1247,
            stats =
              """{"numRecords":1,"minValues":{"name":"2","age":2,"birthday":"2020-02-02"},"maxValues":{"name":"2","age":2,"birthday":"2020-02-02"},"nullCount":{"name":0,"age":0,"birthday":0,"_change_type":1}}""",
            version = 3,
            timestamp = 1651272660000L
          )
        )
        assert(expectedAddFiles == tableFiles.addFiles.toList)
        assert(tableFiles.addFiles(0).expirationTimestamp > System.currentTimeMillis())

        assert(tableFiles.removeFiles.size == 2)
        val expectedRemoveFiles = Seq(
          RemoveFile(
            url = tableFiles.removeFiles(0).url,
            expirationTimestamp = tableFiles.removeFiles(0).expirationTimestamp,
            id = "d7ed708546dd70fdff9191b3e3d6448b",
            partitionValues = Map.empty,
            size = 1030,
            version = 2,
            timestamp = 1651272655000L
          ),
          RemoveFile(
            url = tableFiles.removeFiles(1).url,
            expirationTimestamp = tableFiles.removeFiles(1).expirationTimestamp,
            id = "a6dc5694a4ebcc9a067b19c348526ad6",
            partitionValues = Map.empty,
            size = 1030,
            version = 3,
            timestamp = 1651272660000L
          )
        )
        assert(expectedRemoveFiles == tableFiles.removeFiles.toList)
        assert(tableFiles.removeFiles(0).expirationTimestamp > System.currentTimeMillis())

        assert(tableFiles.additionalMetadatas.size == 2)
        val v4Metadata = Metadata(
          id = "16736144-3306-4577-807a-d3f899b77670",
          format = Format(),
          schemaString =
            """{"type":"struct","fields":[{"name":"name","type":"string","nullable":true,"metadata":{}},{"name":"age","type":"integer","nullable":true,"metadata":{}},{"name":"birthday","type":"date","nullable":true,"metadata":{}}]}""",
          configuration = Map.empty,
          partitionColumns = Nil,
          version = 4
        )
        assert(v4Metadata == tableFiles.additionalMetadatas(0))

        val v5Metadata = v4Metadata.copy(
          configuration = Map("enableChangeDataFeed" -> "true"),
          version = 5
        )
        assert(v5Metadata == tableFiles.additionalMetadatas(1))
      } finally {
        client.close()
      }
    }
  }

  integrationTest("getFiles with startingVersion/endingVersion - success 1") {
    val client = new DeltaSharingRestClient(testProfileProvider, sslTrustAll = true)
    try {
      val tableFiles = client.getFiles(
        Table(name = "cdf_table_cdf_enabled", schema = "default", share = "share8"), 1L, Some(1L)
      )
      assert(tableFiles.version == 1)
      assert(tableFiles.addFiles.size == 3)
      val expectedAddFiles = Seq(
        AddFileForCDF(
          url = tableFiles.addFiles(0).url,
          expirationTimestamp = tableFiles.addFiles(0).expirationTimestamp,
          id = "60d0cf57f3e4367db154aa2c36152a1f",
          partitionValues = Map.empty,
          size = 1030,
          stats = """{"numRecords":1,"minValues":{"name":"1","age":1,"birthday":"2020-01-01"},"maxValues":{"name":"1","age":1,"birthday":"2020-01-01"},"nullCount":{"name":0,"age":0,"birthday":0}}""",
          version = 1,
          timestamp = 1651272635000L
        ),
        AddFileForCDF(
          url = tableFiles.addFiles(1).url,
          expirationTimestamp = tableFiles.addFiles(1).expirationTimestamp,
          id = "a6dc5694a4ebcc9a067b19c348526ad6",
          partitionValues = Map.empty,
          size = 1030,
          stats = """{"numRecords":1,"minValues":{"name":"2","age":2,"birthday":"2020-01-01"},"maxValues":{"name":"2","age":2,"birthday":"2020-01-01"},"nullCount":{"name":0,"age":0,"birthday":0}}""",
          version = 1,
          timestamp = 1651272635000L
        ),
        AddFileForCDF(
          url = tableFiles.addFiles(2).url,
          expirationTimestamp = tableFiles.addFiles(2).expirationTimestamp,
          id = "d7ed708546dd70fdff9191b3e3d6448b",
          partitionValues = Map.empty,
          size = 1030,
          stats = """{"numRecords":1,"minValues":{"name":"3","age":3,"birthday":"2020-01-01"},"maxValues":{"name":"3","age":3,"birthday":"2020-01-01"},"nullCount":{"name":0,"age":0,"birthday":0}}""",
          version = 1,
          timestamp = 1651272635000L
        )
      )
      assert(expectedAddFiles == tableFiles.addFiles.toList)
    } finally {
      client.close()
    }
  }

  integrationTest("getFiles with startingVersion/endingVersion - success 2") {
    val client = new DeltaSharingRestClient(testProfileProvider, sslTrustAll = true)
    try {
      val tableFiles = client.getFiles(
        Table(name = "cdf_table_cdf_enabled", schema = "default", share = "share8"), 2L, Some(3L)
      )
      assert(tableFiles.version == 2)
      assert(tableFiles.addFiles.size == 1)
      val expectedAddFiles = Seq(
        AddFileForCDF(
          url = tableFiles.addFiles(0).url,
          expirationTimestamp = tableFiles.addFiles(0).expirationTimestamp,
          id = "b875623be22c1fa1dfdeb0480fae6117",
          partitionValues = Map.empty,
          size = 1247,
          stats = """{"numRecords":1,"minValues":{"name":"2","age":2,"birthday":"2020-02-02"},"maxValues":{"name":"2","age":2,"birthday":"2020-02-02"},"nullCount":{"name":0,"age":0,"birthday":0,"_change_type":1}}""",
          version = 3,
          timestamp = 1651272660000L
        )
      )
      assert(expectedAddFiles == tableFiles.addFiles.toList)

      assert(tableFiles.removeFiles.size == 2)
      val expectedRemoveFiles = Seq(
        RemoveFile(
          url = tableFiles.removeFiles(0).url,
          expirationTimestamp = tableFiles.removeFiles(0).expirationTimestamp,
          id = "d7ed708546dd70fdff9191b3e3d6448b",
          partitionValues = Map.empty,
          size = 1030,
          version = 2,
          timestamp = 1651272655000L
        ),
        RemoveFile(
          url = tableFiles.removeFiles(1).url,
          expirationTimestamp = tableFiles.removeFiles(1).expirationTimestamp,
          id = "a6dc5694a4ebcc9a067b19c348526ad6",
          partitionValues = Map.empty,
          size = 1030,
          version = 3,
          timestamp = 1651272660000L
        )
      )
      assert(expectedRemoveFiles == tableFiles.removeFiles.toList)
    } finally {
      client.close()
    }
  }

  integrationTest("getFiles with startingVersion/endingVersion - success 3") {
    val client = new DeltaSharingRestClient(testProfileProvider, sslTrustAll = true)
    try {
      val tableFiles = client.getFiles(
        Table(name = "cdf_table_cdf_enabled", schema = "default", share = "share8"), 4L, Some(5L)
      )
      assert(tableFiles.version == 4)
      assert(tableFiles.addFiles.size == 0)
      assert(tableFiles.removeFiles.size == 0)

      assert(tableFiles.additionalMetadatas.size == 1)
      val v5Metadata = Metadata(
        id = "16736144-3306-4577-807a-d3f899b77670",
        format = Format(),
        schemaString = """{"type":"struct","fields":[{"name":"name","type":"string","nullable":true,"metadata":{}},{"name":"age","type":"integer","nullable":true,"metadata":{}},{"name":"birthday","type":"date","nullable":true,"metadata":{}}]}""",
        configuration = Map("enableChangeDataFeed" -> "true"),
        partitionColumns = Nil,
        version = 5)
      assert(v5Metadata == tableFiles.additionalMetadatas(0))
    } finally {
      client.close()
    }
  }

  integrationTest("getFiles with startingVersion/endingVersion - exception") {
    Seq(
      DeltaSharingRestClient.RESPONSE_FORMAT_DELTA, DeltaSharingRestClient.RESPONSE_FORMAT_PARQUET
    ).foreach {
      responseFormat =>
        val client = new DeltaSharingRestClient(
          testProfileProvider,
          sslTrustAll = true,
          responseFormat = responseFormat
        )
        try {
          var errorMessage = intercept[UnexpectedHttpStatus] {
            client.getFiles(
              Table(name = "table1", schema = "default", share = "share1"),
              1,
              None
            )
          }.getMessage
          assert(errorMessage.contains("Reading table by version or timestamp is not supported because history sharing is not enabled on table: share1.default.table1"))

          errorMessage = intercept[UnexpectedHttpStatus] {
            client.getFiles(
              Table(name = "cdf_table_cdf_enabled", schema = "default", share = "share8"),
              -1,
              None
            )
          }.getMessage
          assert(errorMessage.contains("startingVersion cannot be negative"))

          errorMessage = intercept[UnexpectedHttpStatus] {
            client.getFiles(
              Table(name = "cdf_table_cdf_enabled", schema = "default", share = "share8"),
              2,
              Some(1)
            )
          }.getMessage
          assert(errorMessage.contains("startingVersion(2) must be smaller than or equal to " +
            "endingVersion(1)"))
        } finally {
          client.close()
        }
    }
  }

  integrationTest("getCDFFiles") {
    // The response is always in parquet format, because the client allows parquet and the
    // table is a basic table.
    Seq(
      RESPONSE_FORMAT_PARQUET,
      s"${RESPONSE_FORMAT_DELTA},${RESPONSE_FORMAT_PARQUET}",
      s"${RESPONSE_FORMAT_PARQUET},${RESPONSE_FORMAT_DELTA}"
    ).foreach { responseFormat =>
      Seq(true, false).foreach { paginationEnabled =>
        val client = new DeltaSharingRestClient(
          testProfileProvider,
          sslTrustAll = true,
          queryTablePaginationEnabled = paginationEnabled,
          maxFilesPerReq = 1
        )
        try {
          val cdfOptions = Map("startingVersion" -> "0", "endingVersion" -> "3")
          val tableFiles = client.getCDFFiles(
            Table(name = "cdf_table_cdf_enabled", schema = "default", share = "share8"),
            cdfOptions,
            false
          )
          assert(tableFiles.version == 0)
          assert(Protocol(minReaderVersion = 1) == tableFiles.protocol)
          val expectedMetadata = Metadata(
            id = "16736144-3306-4577-807a-d3f899b77670",
            format = Format(),
            schemaString =
              """{"type":"struct","fields":[{"name":"name","type":"string","nullable":true,"metadata":{}},{"name":"age","type":"integer","nullable":true,"metadata":{}},{"name":"birthday","type":"date","nullable":true,"metadata":{}}]}""",
            configuration = Map("enableChangeDataFeed" -> "true"),
            partitionColumns = Nil,
            version = 5
          )
          assert(expectedMetadata == tableFiles.metadata)
          assert(tableFiles.cdfFiles.size == 2)
          val expectedCdfFiles = Seq(
            AddCDCFile(
              url = tableFiles.cdfFiles(0).url,
              expirationTimestamp = tableFiles.cdfFiles(0).expirationTimestamp,
              id = "6521ba910108d4b54d27beaa9fc2373f",
              partitionValues = Map.empty,
              size = 1301,
              version = 2,
              timestamp = 1651272655000L
            ),
            AddCDCFile(
              url = tableFiles.cdfFiles(1).url,
              expirationTimestamp = tableFiles.cdfFiles(1).expirationTimestamp,
              id = "2508998dce55bd726369e53761c4bc3f",
              partitionValues = Map.empty,
              size = 1416,
              version = 3,
              timestamp = 1651272660000L
            )
          )
          assert(expectedCdfFiles == tableFiles.cdfFiles.toList)
          assert(tableFiles.cdfFiles(1).expirationTimestamp > System.currentTimeMillis())

          assert(tableFiles.addFiles.size == 3)
          val expectedAddFiles = Seq(
            AddFileForCDF(
              url = tableFiles.addFiles(0).url,
              expirationTimestamp = tableFiles.addFiles(0).expirationTimestamp,
              id = "60d0cf57f3e4367db154aa2c36152a1f",
              partitionValues = Map.empty,
              size = 1030,
              stats =
                """{"numRecords":1,"minValues":{"name":"1","age":1,"birthday":"2020-01-01"},"maxValues":{"name":"1","age":1,"birthday":"2020-01-01"},"nullCount":{"name":0,"age":0,"birthday":0}}""",
              version = 1,
              timestamp = 1651272635000L
            ),
            AddFileForCDF(
              url = tableFiles.addFiles(1).url,
              expirationTimestamp = tableFiles.addFiles(1).expirationTimestamp,
              id = "a6dc5694a4ebcc9a067b19c348526ad6",
              partitionValues = Map.empty,
              size = 1030,
              stats =
                """{"numRecords":1,"minValues":{"name":"2","age":2,"birthday":"2020-01-01"},"maxValues":{"name":"2","age":2,"birthday":"2020-01-01"},"nullCount":{"name":0,"age":0,"birthday":0}}""",
              version = 1,
              timestamp = 1651272635000L
            ),
            AddFileForCDF(
              url = tableFiles.addFiles(2).url,
              expirationTimestamp = tableFiles.addFiles(2).expirationTimestamp,
              id = "d7ed708546dd70fdff9191b3e3d6448b",
              partitionValues = Map.empty,
              size = 1030,
              stats =
                """{"numRecords":1,"minValues":{"name":"3","age":3,"birthday":"2020-01-01"},"maxValues":{"name":"3","age":3,"birthday":"2020-01-01"},"nullCount":{"name":0,"age":0,"birthday":0}}""",
              version = 1,
              timestamp = 1651272635000L
            )
          )
          assert(expectedAddFiles == tableFiles.addFiles.toList)
          assert(tableFiles.addFiles(0).expirationTimestamp > System.currentTimeMillis())
        } finally {
          client.close()
        }
      }
    }
  }

  integrationTest("getCDFFiles - more metadatas returned for includeHistoricalMetadata=true") {
    val client = new DeltaSharingRestClient(
      testProfileProvider,
      sslTrustAll = true
    )
    try {
      val cdfOptions = Map("startingVersion" -> "0")
      val tableFiles = client.getCDFFiles(
        Table(name = "streaming_notnull_to_null", schema = "default", share = "share8"),
        cdfOptions,
        includeHistoricalMetadata = true
      )
      assert(tableFiles.version == 0)
      assert(Protocol(minReaderVersion = 1) == tableFiles.protocol)
      val expectedMetadata = Metadata(
        id = "1e2201ff-12ad-4c3b-a539-4d34e9e36680",
        format = Format(),
        schemaString = """{"type":"struct","fields":[{"name":"name","type":"string","nullable":false,"metadata":{}}]}""",
        configuration = Map("enableChangeDataFeed" -> "true"),
        partitionColumns = Nil,
        version = 0)
      assert(expectedMetadata == tableFiles.metadata)

      assert(tableFiles.addFiles.size == 2)
      assert(tableFiles.cdfFiles.size == 0)
      assert(tableFiles.removeFiles.size == 0)
      assert(tableFiles.additionalMetadatas.size == 1)

    } finally {
      client.close()
    }
  }

  integrationTest("getCDFFiles - no additional metadatas returned for includeHistoricalMetadata=false") {
    val client = new DeltaSharingRestClient(
      testProfileProvider,
      sslTrustAll = true
    )
    try {
      val cdfOptions = Map("startingVersion" -> "0")
      val tableFiles = client.getCDFFiles(
        Table(name = "streaming_notnull_to_null", schema = "default", share = "share8"),
        cdfOptions,
        includeHistoricalMetadata = false
      )
      assert(tableFiles.version == 0)
      assert(Protocol(minReaderVersion = 1) == tableFiles.protocol)
      val expectedMetadata = Metadata(
        id = "1e2201ff-12ad-4c3b-a539-4d34e9e36680",
        format = Format(),
        schemaString = """{"type":"struct","fields":[{"name":"name","type":"string","nullable":true,"metadata":{}}]}""",
        configuration = Map("enableChangeDataFeed" -> "true"),
        partitionColumns = Nil,
        version = 3)
      assert(expectedMetadata == tableFiles.metadata)

      assert(tableFiles.addFiles.size == 2)
      assert(tableFiles.cdfFiles.size == 0)
      assert(tableFiles.removeFiles.size == 0)
      assert(tableFiles.additionalMetadatas.size == 0)

    } finally {
      client.close()
    }
  }

  integrationTest("getCDFFiles: cdf_table_with_vacuum") {
    val client = new DeltaSharingRestClient(testProfileProvider, sslTrustAll = true)
    try {
      val cdfOptions = Map("startingVersion" -> "0")
      val tableFiles = client.getCDFFiles(
        Table(name = "cdf_table_with_vacuum", schema = "default", share = "share8"),
        cdfOptions,
        false
      )
      assert(tableFiles.version == 0)
      assert(Protocol(minReaderVersion = 1) == tableFiles.protocol)
      assert(tableFiles.addFiles.size == 4)
      assert(tableFiles.cdfFiles.size == 2)
      assert(tableFiles.removeFiles.size == 0)
    } finally {
      client.close()
    }
  }

  integrationTest("getCDFFiles: cdf_table_missing_log") {
    Seq(
      DeltaSharingRestClient.RESPONSE_FORMAT_DELTA, DeltaSharingRestClient.RESPONSE_FORMAT_PARQUET
    ).foreach {
      responseFormat =>
        val client = new DeltaSharingRestClient(
          testProfileProvider,
          sslTrustAll = true,
          responseFormat = responseFormat
        )
        try {
          val errorMessage = intercept[UnexpectedHttpStatus] {
            val cdfOptions = Map("startingVersion" -> "1")
            client.getCDFFiles(
              Table(name = "cdf_table_missing_log", schema = "default", share = "share8"),
              cdfOptions,
              false
            )
          }.getMessage
          assert(errorMessage.contains("""400 Bad Request {"errorCode":"RESOURCE_DOES_NOT_EXIST""""))
          assert(errorMessage.contains("table files missing"))
        } finally {
          client.close()
        }
    }
  }

  integrationTest("getCDFFiles with startingTimestamp") {
    Seq(
      DeltaSharingRestClient.RESPONSE_FORMAT_DELTA, DeltaSharingRestClient.RESPONSE_FORMAT_PARQUET
    ).foreach {
      responseFormat =>
        val client = new DeltaSharingRestClient(
          testProfileProvider,
          sslTrustAll = true,
          responseFormat = responseFormat
        )
        try {
          // This is to test that timestamp is correctly passed to the server and parsed.
          // The error message is expected as we are using a timestamp much smaller than the earliest
          // version of the table.
          val cdfOptions = Map("startingTimestamp" -> "2000-01-01T00:00:00Z")
          val errorMessage = intercept[UnexpectedHttpStatus] {
            val tableFiles = client.getCDFFiles(
              Table(name = "cdf_table_cdf_enabled", schema = "default", share = "share8"),
              cdfOptions,
              false
            )
          }.getMessage
          assert(errorMessage.contains("Please use a timestamp greater"))
        } finally {
          client.close()
        }
    }
  }

  integrationTest("getCDFFiles with endingTimestamp") {
    Seq(
      DeltaSharingRestClient.RESPONSE_FORMAT_DELTA, DeltaSharingRestClient.RESPONSE_FORMAT_PARQUET
    ).foreach {
      responseFormat =>
        val client = new DeltaSharingRestClient(
          testProfileProvider,
          sslTrustAll = true,
          responseFormat = responseFormat
        )
        try {
          // This is to test that timestamp is correctly passed to the server and parsed.
          // The error message is expected as we are using a timestamp much larger than the latest
          // version of the table.
          val cdfOptions = Map("startingVersion" -> "0", "endingTimestamp" -> "2100-01-01T00:00:00Z")
          val errorMessage = intercept[UnexpectedHttpStatus] {
            val tableFiles = client.getCDFFiles(
              Table(name = "cdf_table_cdf_enabled", schema = "default", share = "share8"),
              cdfOptions,
              false
            )
          }.getMessage
          assert(errorMessage.contains("Please use a timestamp less than or equal to"))
        } finally {
          client.close()
        }
    }
  }

  integrationTest("getCDFFiles_exceptions") {
    Seq(
      DeltaSharingRestClient.RESPONSE_FORMAT_DELTA, DeltaSharingRestClient.RESPONSE_FORMAT_PARQUET
    ).foreach {
      responseFormat =>
        val client = new DeltaSharingRestClient(
          testProfileProvider,
          sslTrustAll = true,
          responseFormat = responseFormat
        )
        try {
          val cdfOptions = Map("startingVersion" -> "0")
          val errorMessage = intercept[UnexpectedHttpStatus] {
            client.getCDFFiles(
              Table(name = "table1", schema = "default", share = "share1"),
              cdfOptions,
              false
            )
          }.getMessage
          assert(errorMessage.contains("cdf is not enabled on table share1.default.table1"))
        } finally {
          client.close()
        }
    }
  }
}
