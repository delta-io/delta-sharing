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
import org.apache.http.client.methods.{HttpGet, HttpPost, HttpRequestBase}

import io.delta.sharing.client.model.{
  AddCDCFile,
  AddFile,
  AddFileForCDF,
  AwsTempCredentials,
  AzureUserDelegationSas,
  Credentials,
  DeltaTableFiles,
  EndStreamAction,
  Format,
  GcpOauthToken,
  Metadata,
  Protocol,
  RemoveFile,
  Table,
  TemporaryCredentials
}
import io.delta.sharing.client.util.ConfUtils
import io.delta.sharing.client.util.JsonUtils
import io.delta.sharing.client.util.UnexpectedHttpStatus
import io.delta.sharing.spark.{DeltaSharingServerException, MissingEndStreamActionException}

// scalastyle:off maxLineLength
class DeltaSharingRestClientSuite extends DeltaSharingIntegrationTest {

  import DeltaSharingRestClient._

  private var spark: org.apache.spark.sql.SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = org.apache.spark.sql.SparkSession.builder()
      .master("local[1]")
      .appName("DeltaSharingRestClientSuite")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    try {
      if (spark != null) {
        spark.stop()
        spark = null
      }
    } finally {
      super.afterAll()
    }
  }

  test("parsePath") {
    val emptyShareCredentialsOptions: Map[String, String] = Map.empty
    val testShareCredentialsOptions: Map[String, String] = Map("key" -> "value")

    assert(
      DeltaSharingRestClient.parsePath("file:///foo/bar#a.b.c", emptyShareCredentialsOptions) ==
      ParsedDeltaSharingTablePath("file:///foo/bar", "a", "b", "c"))
    assert(DeltaSharingRestClient.parsePath("file:///foo/bar#bar#a.b.c", emptyShareCredentialsOptions) ==
      ParsedDeltaSharingTablePath("file:///foo/bar#bar", "a", "b", "c"))
    assert(DeltaSharingRestClient.parsePath("file:///foo/bar#bar#a.b.c ", emptyShareCredentialsOptions) ==
      ParsedDeltaSharingTablePath("file:///foo/bar#bar", "a", "b", "c "))
    assert(DeltaSharingRestClient.parsePath("a.b.c", testShareCredentialsOptions) ==
      ParsedDeltaSharingTablePath("", "a", "b", "c"))
    intercept[IllegalArgumentException] {
      DeltaSharingRestClient.parsePath("file:///foo/barr#a.b.c ", testShareCredentialsOptions)
    }
    intercept[IllegalArgumentException] {
      DeltaSharingRestClient.parsePath("file:///foo/bar", emptyShareCredentialsOptions)
    }
    intercept[IllegalArgumentException] {
      DeltaSharingRestClient.parsePath("file:///foo/bar#a.b", emptyShareCredentialsOptions)
    }
    intercept[IllegalArgumentException] {
      DeltaSharingRestClient.parsePath("file:///foo/bar#a.b.c.d", emptyShareCredentialsOptions)
    }
    intercept[IllegalArgumentException] {
      DeltaSharingRestClient.parsePath("#a.b.c", emptyShareCredentialsOptions)
    }
    intercept[IllegalArgumentException] {
      DeltaSharingRestClient.parsePath("foo#a.b.", emptyShareCredentialsOptions)
    }
    intercept[IllegalArgumentException] {
      DeltaSharingRestClient.parsePath("foo#a.b.c.", emptyShareCredentialsOptions)
    }
  }

  test("parsePath with optionsProfileProvider disabled") {
    spark.conf.set(ConfUtils.OPTIONS_PROFILE_PROVIDER_ENABLED_CONF, "false")

    val emptyShareCredentialsOptions: Map[String, String] = Map.empty
    val testShareCredentialsOptions: Map[String, String] = Map("key" -> "value")

    // Should work fine with profile file format when options are empty
    assert(
      DeltaSharingRestClient.parsePath("file:///foo/bar#a.b.c", emptyShareCredentialsOptions) ==
      ParsedDeltaSharingTablePath("file:///foo/bar", "a", "b", "c"))

    // Should fail when shareCredentialsOptions is non-empty
    val e1 = intercept[IllegalArgumentException] {
      DeltaSharingRestClient.parsePath("a.b.c", testShareCredentialsOptions)
    }
    assert(e1.getMessage.contains("DeltaSharingOptionsProfileProvider is disabled"))
    assert(e1.getMessage.contains(ConfUtils.OPTIONS_PROFILE_PROVIDER_ENABLED_CONF))

    // Should fail when path has no profile file (shapeIndex < 0) even with empty options
    val e2 = intercept[IllegalArgumentException] {
      DeltaSharingRestClient.parsePath("a.b.c", emptyShareCredentialsOptions)
    }
    assert(e2.getMessage.contains("you must provide a profile file path"))
    assert(e2.getMessage.contains("profile_file#share.schema.table"))

    // Reset config for other tests
    spark.conf.set(ConfUtils.OPTIONS_PROFILE_PROVIDER_ENABLED_CONF, "true")
  }

  test("DeltaSharingRestClient.apply with optionsProfileProvider disabled") {
    spark.conf.set(ConfUtils.OPTIONS_PROFILE_PROVIDER_ENABLED_CONF, "false")

    val testShareCredentialsOptions: Map[String, String] = Map(
      "shareCredentialsVersion" -> "1",
      "endpoint" -> "https://example.com",
      "bearerToken" -> "test-token"
    )

    // Should fail when trying to create client with shareCredentialsOptions when flag is disabled
    val e = intercept[IllegalArgumentException] {
      DeltaSharingRestClient("", testShareCredentialsOptions)
    }
    assert(e.getMessage.contains("DeltaSharingOptionsProfileProvider is disabled"))
    assert(e.getMessage.contains(ConfUtils.OPTIONS_PROFILE_PROVIDER_ENABLED_CONF))

    // Reset config for other tests
    spark.conf.set(ConfUtils.OPTIONS_PROFILE_PROVIDER_ENABLED_CONF, "true")
  }

  integrationTest("Check headers") {
    val httpRequest = new HttpGet("random_url")

    def checkUserAgent(request: HttpRequestBase, containsStreaming: Boolean): Unit = {
      val h = request.getFirstHeader(HttpHeaders.USER_AGENT).getValue
      if (containsStreaming) {
        assert(h.contains(SPARK_STRUCTURED_STREAMING) == containsStreaming)
      } else {
        assert(!h.contains(SPARK_STRUCTURED_STREAMING))
        assert(h.contains("Delta-Sharing-Spark"))
      }

      assert(h.contains(" QueryId-"))
      assert(h.contains(" Hadoop/"))
      assert(h.contains(" Linux/"))
      assert(h.contains(" java/"))
    }

    def getEndStreamActionHeader(endStreamActionEnabled: Boolean): String = {
      if (endStreamActionEnabled) {
        s";$DELTA_SHARING_INCLUDE_END_STREAM_ACTION=true"
      } else {
        ""
      }
    }

    def checkDeltaSharingCapabilities(
        request: HttpRequestBase,
        responseFormat: String,
        readerFeatures: String,
        endStreamActionEnabled: Boolean): Unit = {
      val expected = s"${RESPONSE_FORMAT}=$responseFormat$readerFeatures" +
        getEndStreamActionHeader(endStreamActionEnabled)
      val h = request.getFirstHeader(DELTA_SHARING_CAPABILITIES_HEADER)
      assert(h.getValue == expected)
    }

    Seq(
      (true, true),
      (true, false),
      (false, true),
      (false, false)
    ).foreach { case (forStreaming, endStreamActionEnabled) =>
      var client = new DeltaSharingRestClient(
        testProfileProvider,
        forStreaming = forStreaming,
        endStreamActionEnabled = endStreamActionEnabled,
        readerFeatures = "willBeIgnored")
        .prepareHeaders(httpRequest, setIncludeEndStreamAction = endStreamActionEnabled)
      checkUserAgent(client, forStreaming)
      checkDeltaSharingCapabilities(client, "parquet", "", endStreamActionEnabled)

      val readerFeatures = "deletionVectors,columnMapping,timestampNTZ"
      client = new DeltaSharingRestClient(
        testProfileProvider,
        forStreaming = forStreaming,
        endStreamActionEnabled = endStreamActionEnabled,
        responseFormat = RESPONSE_FORMAT_DELTA,
        readerFeatures = readerFeatures)
        .prepareHeaders(httpRequest, setIncludeEndStreamAction = endStreamActionEnabled)
      checkUserAgent(client, forStreaming)
      checkDeltaSharingCapabilities(
        client, "delta", s";$READER_FEATURES=$readerFeatures", endStreamActionEnabled
      )

      client = new DeltaSharingRestClient(
        testProfileProvider,
        forStreaming = forStreaming,
        endStreamActionEnabled = endStreamActionEnabled,
        responseFormat = s"$RESPONSE_FORMAT_DELTA,$RESPONSE_FORMAT_PARQUET",
        readerFeatures = readerFeatures)
        .prepareHeaders(httpRequest, setIncludeEndStreamAction = endStreamActionEnabled)
      checkUserAgent(client, forStreaming)
      checkDeltaSharingCapabilities(
        client, s"delta,parquet", s";$READER_FEATURES=$readerFeatures", endStreamActionEnabled
      )
    }
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
        Table(name = "table_gcs", schema = "default", share = "share_gcp"),
        Table(name = "table_with_cm_name", schema = "default", share = "share8"),
        Table(name = "table_with_cm_id", schema = "default", share = "share8"),
        Table(name = "deletion_vectors_with_dvs_dv_property_on", schema = "default", share = "share8"),
        Table(name = "dv_and_cm_table", schema = "default", share = "share8"),
        Table(name = "timestampntz_cdf_table", schema = "default", share = "share8")
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
    assert(errorMessage.contains("Cannot load table version 6"))

    errorMessage = intercept[UnexpectedHttpStatus] {
      client.getMetadata(
        Table(name = "streaming_notnull_to_null", schema = "default", share = "share8"),
        timestampAsOf = Some("2021-01-01T00:00:00Z")
      )
    }.getMessage
    assert(errorMessage.contains("is before the earliest available version "))
  }

  integrationTest("getFiles using async api error handling") {
    val client = new DeltaSharingRestClient(
      testProfileProvider,
      sslTrustAll = true,
      queryTablePaginationEnabled = true,
      maxFilesPerReq = 1,
      responseFormat = RESPONSE_FORMAT_PARQUET,
      enableAsyncQuery = true
    )
    val table = Table(name = "tableWithAsyncQueryError", schema = "default", share = "share1")

    val errorMessage = intercept[UnexpectedHttpStatus] {
      client.getFiles(
        table,
        predicates = Nil,
        limit = None,
        versionAsOf = None,
        timestampAsOf = None,
        jsonPredicateHints = None,
        refreshToken = None
      )
    }.getMessage

    assert(errorMessage.contains("expected error"))
  }

  integrationTest("getFiles using async api") {
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
    }

    Seq(false, true).foreach { paginationEnabled => {
      val client = new DeltaSharingRestClient(
        testProfileProvider,
        sslTrustAll = true,
        queryTablePaginationEnabled = paginationEnabled,
        maxFilesPerReq = 1,
        responseFormat = RESPONSE_FORMAT_PARQUET,
        enableAsyncQuery = true
      )
      val table = Table(name = "table2", schema = "default", share = "share2")
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
    }
    }
  }

  integrationTest("getFiles with sync api") {
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
      Seq(true, false).foreach { paginationEnabled => {
        val client = new DeltaSharingRestClient(
          testProfileProvider,
          sslTrustAll = true,
          queryTablePaginationEnabled = paginationEnabled,
          maxFilesPerReq = 1,
          responseFormat = responseFormat,
          enableAsyncQuery = false
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

          if (tableFiles.refreshToken.isDefined) {
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
          }
        } finally {
          client.close()
        }
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
          timestamp = null,
          stats = """{"numRecords":1,"minValues":{"name":"1","age":1,"birthday":"2020-01-01"},"maxValues":{"name":"1","age":1,"birthday":"2020-01-01"},"nullCount":{"name":0,"age":0,"birthday":0}}"""
        ),
        AddFile(
          url = tableFiles.files(1).url,
          expirationTimestamp = tableFiles.files(1).expirationTimestamp,
          id = "a6dc5694a4ebcc9a067b19c348526ad6",
          partitionValues = Map.empty,
          size = 1030,
          version = 1,
          timestamp = null,
          stats = """{"numRecords":1,"minValues":{"name":"2","age":2,"birthday":"2020-01-01"},"maxValues":{"name":"2","age":2,"birthday":"2020-01-01"},"nullCount":{"name":0,"age":0,"birthday":0}}"""
        ),
        AddFile(
          url = tableFiles.files(2).url,
          expirationTimestamp = tableFiles.files(2).expirationTimestamp,
          id = "d7ed708546dd70fdff9191b3e3d6448b",
          partitionValues = Map.empty,
          size = 1030,
          version = 1,
          timestamp = null,
          stats = """{"numRecords":1,"minValues":{"name":"3","age":3,"birthday":"2020-01-01"},"maxValues":{"name":"3","age":3,"birthday":"2020-01-01"},"nullCount":{"name":0,"age":0,"birthday":0}}"""
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
          assert(errorMessage.contains("""400 Bad Request for query"""))
          assert(errorMessage.contains("""{"errorCode":"RESOURCE_DOES_NOT_EXIST""""))
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

  val fakeAddFileStr = JsonUtils.toJson(AddFile(
    url = "https://unused-url",
    expirationTimestamp = 12345,
    id = "random-id",
    partitionValues = Map.empty,
    size = 123,
    stats = """{"numRecords":1,"minValues":{"age":"1"},"maxValues":{"age":"3"},"nullCount":{"age":0}}"""
  ).wrap)
  val fakeEndStreamActionStr = JsonUtils.toJson(EndStreamAction(
    refreshToken = "random-refresh",
    nextPageToken = "random-next",
    minUrlExpirationTimestamp = 0
  ).wrap)
  val fakeEndStreamActionErrorStr = JsonUtils.toJson(EndStreamAction(
    refreshToken = null,
    nextPageToken = null,
    minUrlExpirationTimestamp = null,
    errorMessage = "BAD REQUEST: Error Occurred During Streaming"
  ).wrap)
  val fakeEndStreamActionStrWithErrorMsgAndCode = JsonUtils.toJson(EndStreamAction(
      refreshToken = null,
      nextPageToken = null,
      minUrlExpirationTimestamp = null,
      errorMessage = "BAD REQUEST: Error Occurred During Streaming",
      httpStatusErrorCode = 400
    ).wrap)

  test("checkEndStreamAction succeeded") {
    // DELTA_SHARING_INCLUDE_END_STREAM_ACTION=true
    // Succeeded because the responded header is true, and EndStreamAction exists
    checkEndStreamAction(
      Some(s"$DELTA_SHARING_INCLUDE_END_STREAM_ACTION=true"),
      Map(DELTA_SHARING_INCLUDE_END_STREAM_ACTION -> "true"),
      Seq(fakeAddFileStr, fakeEndStreamActionStr),
      "random-query-id"
    )

    // DELTA_SHARING_INCLUDE_END_STREAM_ACTION=false
    // Succeeded even though the last line is not EndStreamAction
    checkEndStreamAction(
      Some(s"$DELTA_SHARING_INCLUDE_END_STREAM_ACTION=false"),
      Map(DELTA_SHARING_INCLUDE_END_STREAM_ACTION -> "false"),
      Seq(fakeAddFileStr),
      "random-query-id"
    )
    // Succeeded even though the lines are empty.
    checkEndStreamAction(
      Some(s"$DELTA_SHARING_INCLUDE_END_STREAM_ACTION=false"),
      Map(DELTA_SHARING_INCLUDE_END_STREAM_ACTION -> "false"),
      Seq.empty,
      "random-query-id"
    )
    // Succeeded even though the last line cannot be parsed.
    checkEndStreamAction(
      Some(s"$DELTA_SHARING_INCLUDE_END_STREAM_ACTION=false"),
      Map(DELTA_SHARING_INCLUDE_END_STREAM_ACTION -> "false"),
      Seq("random-string-server-error"),
      "random-query-id"
    )

    // DELTA_SHARING_INCLUDE_END_STREAM_ACTION not in header
    // Succeeded even though the last line is not EndStreamAction
    checkEndStreamAction(None, Map.empty, Seq(fakeAddFileStr), "random-query-id")
    // Succeeded even though the lines are empty.
    checkEndStreamAction(None, Map.empty, Seq.empty, "random-query-id")
    // Succeeded even though the last line cannot be parsed.
    checkEndStreamAction(None, Map.empty, Seq("random-string-server-error"), "random-query-id")
  }

  test("checkEndStreamAction failed") {
    def checkErrorMessage(
        e: MissingEndStreamActionException,
        additionalErrorMsg: String): Unit = {
      val commonErrorMsg = "Client sets includeendstreamaction=true random-query-id, " +
      "server responded with the header set to true(Some(includeendstreamaction=true)) and"
      assert(e.getMessage.contains(commonErrorMsg))

      assert(e.getMessage.contains(additionalErrorMsg))
    }

    // Failed because the last line is not EndStreamAction.
    var e = intercept[MissingEndStreamActionException] {
      checkEndStreamAction(
        Some(s"$DELTA_SHARING_INCLUDE_END_STREAM_ACTION=true"),
        Map(DELTA_SHARING_INCLUDE_END_STREAM_ACTION -> "true"),
        Seq(fakeAddFileStr),
        "random-query-id"
      )
    }
    checkErrorMessage(e, s"and 1 lines, and last line as [$fakeAddFileStr].")

    // Failed because the last line cannot be parsed.
    e = intercept[MissingEndStreamActionException] {
      checkEndStreamAction(
        Some(s"$DELTA_SHARING_INCLUDE_END_STREAM_ACTION=true"),
        Map(DELTA_SHARING_INCLUDE_END_STREAM_ACTION -> "true"),
        Seq("random-string-server-error"),
        "random-query-id"
      )
    }
    checkErrorMessage(e, s"and 1 lines, and last line as [random-string-server-error].")

    // Failed because the responded lines are empty.
    e = intercept[MissingEndStreamActionException] {
      checkEndStreamAction(
        Some(s"$DELTA_SHARING_INCLUDE_END_STREAM_ACTION=true"),
        Map(DELTA_SHARING_INCLUDE_END_STREAM_ACTION -> "true"),
        Seq.empty,
        "random-query-id"
      )
    }
    checkErrorMessage(e, s"and 0 lines, and last line as [Empty_Seq_in_checkEndStreamAction].")
  }

  test("checkEndStreamAction with error message throws streaming error") {
    def checkErrorMessage(
        e: DeltaSharingServerException,
        additionalErrorMsg: String,
        errorCodeOpt: Option[Int] = None): Unit = {
      val commonErrorMsg = s"Server Exception[${errorCodeOpt.getOrElse("")}]"
      assert(e.getMessage.contains(commonErrorMsg))
      // null/non-existent httpStatusErrorCode in endStreamAction json string defaults to e.statusCodeOpt=None
      assert(e.statusCodeOpt == errorCodeOpt)

      assert(e.getMessage.contains(additionalErrorMsg))
    }

    // checkEndStreamAction throws error if the last line is EndStreamAction with error message
    var e = intercept[DeltaSharingServerException] {
      checkEndStreamAction(
        Some(s"$DELTA_SHARING_INCLUDE_END_STREAM_ACTION=true"),
        Map(DELTA_SHARING_INCLUDE_END_STREAM_ACTION -> "true"),
        Seq(fakeAddFileStr, fakeEndStreamActionErrorStr),
        "random-query-id"
      )
    }
    checkErrorMessage(e, "BAD REQUEST: Error Occurred During Streaming")

    // checkEndStreamAction throws error if the last line is EndStreamAction with error message
    e = intercept[DeltaSharingServerException] {
      checkEndStreamAction(
        Some(s"$DELTA_SHARING_INCLUDE_END_STREAM_ACTION=true"),
        Map(DELTA_SHARING_INCLUDE_END_STREAM_ACTION -> "true"),
        Seq(fakeAddFileStr, fakeEndStreamActionStrWithErrorMsgAndCode),
        "random-query-id"
      )
    }
    checkErrorMessage(e, "BAD REQUEST: Error Occurred During Streaming", Some(400))

    // checkEndStreamAction throws error if the only line is EndStreamAction with error message & errorCode
    e = intercept[DeltaSharingServerException] {
      checkEndStreamAction(
        Some(s"$DELTA_SHARING_INCLUDE_END_STREAM_ACTION=true"),
        Map(DELTA_SHARING_INCLUDE_END_STREAM_ACTION -> "true"),
        Seq(fakeEndStreamActionErrorStr),
        "random-query-id"
      )
    }
    checkErrorMessage(e, "BAD REQUEST: Error Occurred During Streaming")
  }

  class TestProfileProvider(isMSTQuery: Boolean = false) extends DeltaSharingProfileProvider {
    override def getProfile: DeltaSharingProfile = DeltaSharingProfile(
      shareCredentialsVersion = Some(1),
      endpoint = "http://localhost:12345",
      bearerToken = "test-bearer-token"
    )
    override def isMSTQuery(): Boolean = isMSTQuery
  }

  test("generateTemporaryTableCredential - parse responses") {
    val testCases = Seq(
      (
        """{"credentials":{"location":"s3://some/path/to/table","awsTempCredentials":{"accessKeyId":"some-access-key-id","secretAccessKey":"some-secret-access-key","sessionToken":"some-session-token"},"expirationTime":1}}""",
        TemporaryCredentials(
          credentials = Credentials(
            location = "s3://some/path/to/table",
            awsTempCredentials = AwsTempCredentials(
              accessKeyId = "some-access-key-id",
              secretAccessKey = "some-secret-access-key",
              sessionToken = "some-session-token"
            ),
            expirationTime = 1L
          )
        )
      ),
      (
        """{"credentials":{"location":"abfss://my-container@mystorage.dfs.core.windows.net/some/path/to/table","azureUserDelegationSas":{"sasToken":"some-sas-token"},"expirationTime":1}}""",
        TemporaryCredentials(
          credentials = Credentials(
            location = "abfss://my-container@mystorage.dfs.core.windows.net/some/path/to/table",
            azureUserDelegationSas = AzureUserDelegationSas(
              sasToken = "some-sas-token"
            ),
            expirationTime = 1L
          )
        )
      ),
      (
        """{"credentials":{"location":"gs://my-bucket/some/path/to/table","gcpOauthToken":{"oauthToken":"some-oauth-token"},"expirationTime":1}}""",
        TemporaryCredentials(
          credentials = Credentials(
            location = "gs://my-bucket/some/path/to/table",
            gcpOauthToken = GcpOauthToken(
              oauthToken = "some-oauth-token"
            ),
            expirationTime = 1L
          )
        )
      )
    )

    testCases.foreach { case (jsonResponse, expectedCredentials) =>
      // Create a client that overrides getResponse to return a mocked response
      val client = new DeltaSharingRestClient(
        profileProvider = new TestProfileProvider,
        sslTrustAll = true
      ) {
        override private[client] def getResponse(
          httpRequest: HttpRequestBase,
          allowNoContent: Boolean = false,
          fetchAsOneString: Boolean = false,
          setIncludeEndStreamAction: Boolean = false
        ): (Option[Long], Map[String, String], Seq[String]) = {
          // Return a mock response with the test JSON
          (
            None,
            Map.empty,
            Seq(jsonResponse)
          )
        }
      }

      try {
        val table = Table(name = "test_table", schema = "test_schema", share = "test_share")
        val result = client.generateTemporaryTableCredential(table)

        // Verify the result matches the expected credentials
        assert(result == expectedCredentials)
        assert(result.credentials.location == expectedCredentials.credentials.location)
        assert(result.credentials.expirationTime == expectedCredentials.credentials.expirationTime)

        // Verify AWS credentials if present
        if (expectedCredentials.credentials.awsTempCredentials != null) {
          assert(result.credentials.awsTempCredentials != null)
          assert(result.credentials.awsTempCredentials.accessKeyId ==
            expectedCredentials.credentials.awsTempCredentials.accessKeyId)
          assert(result.credentials.awsTempCredentials.secretAccessKey ==
            expectedCredentials.credentials.awsTempCredentials.secretAccessKey)
          assert(result.credentials.awsTempCredentials.sessionToken ==
            expectedCredentials.credentials.awsTempCredentials.sessionToken)
        }

        // Verify Azure credentials if present
        if (expectedCredentials.credentials.azureUserDelegationSas != null) {
          assert(result.credentials.azureUserDelegationSas != null)
          assert(result.credentials.azureUserDelegationSas.sasToken ==
            expectedCredentials.credentials.azureUserDelegationSas.sasToken)
        }

        // Verify GCP credentials if present
        if (expectedCredentials.credentials.gcpOauthToken != null) {
          assert(result.credentials.gcpOauthToken != null)
          assert(result.credentials.gcpOauthToken.oauthToken ==
            expectedCredentials.credentials.gcpOauthToken.oauthToken)
        }
      } finally {
        client.close()
      }
    }
  }

  test("generateTemporaryTableCredential - with location parameter") {
    var capturedRequest: Option[HttpRequestBase] = None

    val client = new DeltaSharingRestClient(
      profileProvider = new TestProfileProvider,
      sslTrustAll = true
    ) {
      override private[client] def getResponse(
        httpRequest: HttpRequestBase,
        allowNoContent: Boolean = false,
        fetchAsOneString: Boolean = false,
        setIncludeEndStreamAction: Boolean = false
      ): (Option[Long], Map[String, String], Seq[String]) = {
        capturedRequest = Some(httpRequest)
        // Return a mock response
        (
          None,
          Map.empty,
          Seq("""{"credentials":{"location":"s3://custom/location/path","awsTempCredentials":{"accessKeyId":"test-key","secretAccessKey":"test-secret","sessionToken":"test-token"},"expirationTime":1}}""")
        )
      }
    }

    try {
      val table = Table(name = "test_table", schema = "test_schema", share = "test_share")
      val customLocation = "s3://custom/location/path"

      // Test with location parameter
      val result = client.generateTemporaryTableCredential(table, Some(customLocation))

      // Verify the request was captured
      assert(capturedRequest.isDefined)
      val httpRequest = capturedRequest.get

      // Verify it's a POST request
      assert(httpRequest.isInstanceOf[HttpPost])
      val httpPost = httpRequest.asInstanceOf[HttpPost]

      // Verify the request has the correct content-type header
      assert(httpPost.getFirstHeader("Content-type").getValue == "application/json")

      // Verify the request body contains the location
      val entity = httpPost.getEntity
      assert(entity != null)
      val content = scala.io.Source.fromInputStream(entity.getContent).mkString
      assert(content.contains(customLocation))
      assert(content.contains("\"location\""))

      // Verify the response is parsed correctly
      assert(result.credentials.location == customLocation)
    } finally {
      client.close()
    }

    // Test without location parameter (location = None)
    capturedRequest = None
    val client2 = new DeltaSharingRestClient(
      profileProvider = new TestProfileProvider,
      sslTrustAll = true
    ) {
      override private[client] def getResponse(
        httpRequest: HttpRequestBase,
        allowNoContent: Boolean = false,
        fetchAsOneString: Boolean = false,
        setIncludeEndStreamAction: Boolean = false
      ): (Option[Long], Map[String, String], Seq[String]) = {
        capturedRequest = Some(httpRequest)
        (
          None,
          Map.empty,
          Seq("""{"credentials":{"location":"s3://default/path","awsTempCredentials":{"accessKeyId":"test-key","secretAccessKey":"test-secret","sessionToken":"test-token"},"expirationTime":1}}""")
        )
      }
    }

    try {
      val table = Table(name = "test_table", schema = "test_schema", share = "test_share")

      // Test without location parameter
      val result = client2.generateTemporaryTableCredential(table, None)

      // Verify the request was captured
      assert(capturedRequest.isDefined)
      val httpRequest = capturedRequest.get

      // Verify it's a POST request
      assert(httpRequest.isInstanceOf[HttpPost])
      val httpPost = httpRequest.asInstanceOf[HttpPost]

      // Verify the request has no entity (no body) when location is None
      val entity = httpPost.getEntity
      assert(entity == null || {
        val content = scala.io.Source.fromInputStream(entity.getContent).mkString
        content.isEmpty
      })
    } finally {
      client2.close()
    }
  }

  test("generateTemporaryTableCredential - error on not one-line response") {
    // Test with more than 1 line in response
    val client = new DeltaSharingRestClient(
      profileProvider = new TestProfileProvider,
      sslTrustAll = true
    ) {
      override private[client] def getResponse(
        httpRequest: HttpRequestBase,
        allowNoContent: Boolean = false,
        fetchAsOneString: Boolean = false,
        setIncludeEndStreamAction: Boolean = false
      ): (Option[Long], Map[String, String], Seq[String]) = {
        // Return a mock response with multiple lines (invalid for this endpoint)
        (
          None,
          Map.empty,
          Seq(
            """{"credentials":{"location":"s3://some/path/to/table","awsTempCredentials":{"accessKeyId":"some-access-key-id","secretAccessKey":"some-secret-access-key","sessionToken":"some-session-token"},"expirationTime":1}}""",
            """{"extraLine":"this should not be here"}"""
          )
        )
      }
    }

    try {
      val table = Table(name = "test_table", schema = "test_schema", share = "test_share")
      val exception = intercept[IllegalStateException] {
        client.generateTemporaryTableCredential(table)
      }
      assert(exception.getMessage.contains("Unexpected response"))
      assert(exception.getMessage.contains("response="))
    } finally {
      client.close()
    }

    // Test with 0 lines in response
    val client2 = new DeltaSharingRestClient(
      profileProvider = new TestProfileProvider,
      sslTrustAll = true
    ) {
      override private[client] def getResponse(
        httpRequest: HttpRequestBase,
        allowNoContent: Boolean = false,
        fetchAsOneString: Boolean = false,
        setIncludeEndStreamAction: Boolean = false
      ): (Option[Long], Map[String, String], Seq[String]) = {
        // Return a mock response with no lines (invalid)
        (
          None,
          Map.empty,
          Seq.empty
        )
      }
    }

    try {
      val table = Table(name = "test_table", schema = "test_schema", share = "test_share")
      val exception = intercept[IllegalStateException] {
        client2.generateTemporaryTableCredential(table)
      }
      assert(exception.getMessage.contains("Unexpected response"))
    } finally {
      client2.close()
    }
  }

  test("generateTemporaryTableCredential - malformed response handling") {
    // Test with malformed JSON
    val malformedJsonClient = new DeltaSharingRestClient(
      profileProvider = new TestProfileProvider,
      sslTrustAll = true
    ) {
      override private[client] def getResponse(
        httpRequest: HttpRequestBase,
        allowNoContent: Boolean = false,
        fetchAsOneString: Boolean = false,
        setIncludeEndStreamAction: Boolean = false
      ): (Option[Long], Map[String, String], Seq[String]) = {
        (
          None,
          Map.empty,
          Seq("""{"credentials":{"location":"s3://path","invalid-json""")
        )
      }
    }

    try {
      val table = Table(name = "test_table", schema = "test_schema", share = "test_share")
      val exception = intercept[Exception] {
        malformedJsonClient.generateTemporaryTableCredential(table)
      }
      // Should throw a JSON parsing exception
      assert(exception != null)
    } finally {
      malformedJsonClient.close()
    }

    // Test with missing required fields (no credentials types set)
    val missingFieldsClient = new DeltaSharingRestClient(
      profileProvider = new TestProfileProvider,
      sslTrustAll = true
    ) {
      override private[client] def getResponse(
        httpRequest: HttpRequestBase,
        allowNoContent: Boolean = false,
        fetchAsOneString: Boolean = false,
        setIncludeEndStreamAction: Boolean = false
      ): (Option[Long], Map[String, String], Seq[String]) = {
        (
          None,
          Map.empty,
          Seq("""{"credentials":{"location":"s3://path","expirationTime":1}}""")
        )
      }
    }

    try {
      val table = Table(name = "test_table", schema = "test_schema", share = "test_share")
      // This should throw an exception because no credential types are set
      val exception = intercept[IllegalStateException] {
        missingFieldsClient.generateTemporaryTableCredential(table)
      }
      assert(exception.getMessage.contains("No valid credentials found in response"))
      assert(exception.getMessage.contains("awsTempCredentials, azureUserDelegationSas, or gcpOauthToken"))
    } finally {
      missingFieldsClient.close()
    }

    // Test with null credentials
    val nullCredentialsClient = new DeltaSharingRestClient(
      profileProvider = new TestProfileProvider,
      sslTrustAll = true
    ) {
      override private[client] def getResponse(
        httpRequest: HttpRequestBase,
        allowNoContent: Boolean = false,
        fetchAsOneString: Boolean = false,
        setIncludeEndStreamAction: Boolean = false
      ): (Option[Long], Map[String, String], Seq[String]) = {
        (
          None,
          Map.empty,
          Seq("""{"credentials":null}""")
        )
      }
    }

    try {
      val table = Table(name = "test_table", schema = "test_schema", share = "test_share")
      // This should throw an exception because credentials object is null
      val exception = intercept[IllegalStateException] {
        nullCredentialsClient.generateTemporaryTableCredential(table)
      }
      assert(exception.getMessage.contains("Credentials object is null"))
    } finally {
      nullCredentialsClient.close()
    }

    // Test with completely invalid JSON structure
    val invalidStructureClient = new DeltaSharingRestClient(
      profileProvider = new TestProfileProvider,
      sslTrustAll = true
    ) {
      override private[client] def getResponse(
        httpRequest: HttpRequestBase,
        allowNoContent: Boolean = false,
        fetchAsOneString: Boolean = false,
        setIncludeEndStreamAction: Boolean = false
      ): (Option[Long], Map[String, String], Seq[String]) = {
        (
          None,
          Map.empty,
          Seq("""["this", "is", "an", "array"]""")
        )
      }
    }

    try {
      val table = Table(name = "test_table", schema = "test_schema", share = "test_share")
      val exception = intercept[Exception] {
        invalidStructureClient.generateTemporaryTableCredential(table)
      }
      assert(exception != null)
    } finally {
      invalidStructureClient.close()
    }

    // Test with empty JSON object
    val emptyJsonClient = new DeltaSharingRestClient(
      profileProvider = new TestProfileProvider,
      sslTrustAll = true
    ) {
      override private[client] def getResponse(
        httpRequest: HttpRequestBase,
        allowNoContent: Boolean = false,
        fetchAsOneString: Boolean = false,
        setIncludeEndStreamAction: Boolean = false
      ): (Option[Long], Map[String, String], Seq[String]) = {
        (
          None,
          Map.empty,
          Seq("""{}""")
        )
      }
    }

    try {
      val table = Table(name = "test_table", schema = "test_schema", share = "test_share")
      // This should throw an exception because credentials object is missing
      val exception = intercept[IllegalStateException] {
        emptyJsonClient.generateTemporaryTableCredential(table)
      }
      assert(exception.getMessage.contains("Credentials object is null"))
    } finally {
      emptyJsonClient.close()
    }
  }

  test("version mismatch check - getMetadata with mocked response") {
    Seq(true, false).foreach { isMSTQueryFlag =>
      val requestedVersion = 5L
      val returnedVersion = 10L

      // Create a client that overrides getNDJson to return a mocked response
      val client = new DeltaSharingRestClient(
        profileProvider = new TestProfileProvider(isMSTQueryFlag)
      ) {
        override def getNDJson(
            target: String,
            requireVersion: Boolean,
            setIncludeEndStreamAction: Boolean
        ): ParsedDeltaSharingResponse = {
          // Return a mock response with a different version than requested
          ParsedDeltaSharingResponse(
            version = returnedVersion,
            respondedFormat = RESPONSE_FORMAT_PARQUET,
            lines = Seq(
              """{"protocol":{"minReaderVersion":1}}""",
              """{"metaData":{"id":"test-id","format":{"provider":"parquet"},"schemaString":"{\"type\":\"struct\",\"fields\":[]}","partitionColumns":[]}}"""
            ),
            capabilitiesMap = Map.empty
          )
        }
      }

      try {
        val table = Table(name = "test_table", schema = "test_schema", share = "test_share")

        if (isMSTQueryFlag) {
          // For MST queries, should throw exception when versions don't match
          val exception = intercept[IllegalArgumentException] {
            client.getMetadata(table, versionAsOf = Some(requestedVersion), timestampAsOf = None)
          }
          assert(exception.getMessage.contains(
            s"The returned table version $returnedVersion does not match the requested versionAsOf $requestedVersion"
          ))
          assert(exception.getMessage.contains("in getMetadata"))
        } else {
          // For non-MST queries, should succeed and return the server's version
          val metadata = client.getMetadata(table, versionAsOf = Some(requestedVersion), timestampAsOf = None)
          assert(metadata.version == returnedVersion)
        }
      } finally {
        client.close()
      }
    }
  }

  test("version mismatch check - getFiles with mocked response") {
    // Test delta format where version validation is controlled by isMSTQuery
    Seq(true, false).foreach { isMSTQueryFlag =>
      val requestedVersion = 15L
      val returnedVersion = 20L

      // Create a client that overrides getNDJsonPost to return a mocked delta format response
      val client = new DeltaSharingRestClient(
        profileProvider = new TestProfileProvider(isMSTQueryFlag),
        responseFormat = RESPONSE_FORMAT_DELTA
      ) {
        override def getNDJsonPost[T: Manifest](
            target: String,
            data: T,
            setIncludeEndStreamAction: Boolean
        ): ParsedDeltaSharingResponse = {
          // Return a mock delta format response with a different version than requested
          ParsedDeltaSharingResponse(
            version = returnedVersion,
            respondedFormat = RESPONSE_FORMAT_DELTA,
            lines = Seq(
              """{"protocol":{"minReaderVersion":1}}""",
              """{"metaData":{"id":"test-id","format":{"provider":"parquet"},"schemaString":"{\"type\":\"struct\",\"fields\":[]}","partitionColumns":[]}}"""
            ),
            capabilitiesMap = Map.empty
          )
        }
      }

      try {
        val table = Table(name = "test_table", schema = "test_schema", share = "test_share")

        if (isMSTQueryFlag) {
          // For MST queries, should throw exception when versions don't match
          val exception = intercept[IllegalArgumentException] {
            client.getFiles(table, Nil, None, Some(requestedVersion), None, None, None)
          }
          assert(exception.getMessage.contains(
            s"The returned table version $returnedVersion does not match the requested versionAsOf $requestedVersion"
          ))
          assert(exception.getMessage.contains("in getFiles"))
        } else {
          // For non-MST queries, should succeed and return the server's version
          val files = client.getFiles(table, Nil, None, Some(requestedVersion), None, None, None)
          assert(files.version == returnedVersion)
        }
      } finally {
        client.close()
      }
    }
  }

}
