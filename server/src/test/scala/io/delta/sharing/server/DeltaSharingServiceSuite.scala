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

package io.delta.sharing.server

import java.io.IOException
import java.net.{URL, URLEncoder}
import java.nio.charset.StandardCharsets.UTF_8
import java.security.cert.X509Certificate
import java.sql.Timestamp
import javax.net.ssl._

import scala.collection.mutable.ArrayBuffer

import com.linecorp.armeria.server.Server
import io.delta.standalone.internal.{DeltaResponseProtocol, DeltaResponseSingleAction}
import io.delta.standalone.internal.DeltaSharedTable.{RESPONSE_FORMAT_DELTA, RESPONSE_FORMAT_PARQUET}
import org.apache.commons.io.IOUtils
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import scalapb.json4s.JsonFormat

import io.delta.sharing.server.config.ServerConfig
import io.delta.sharing.server.model._
import io.delta.sharing.server.protocol._
import io.delta.sharing.server.util.JsonUtils

// scalastyle:off maxLineLength
class DeltaSharingServiceSuite extends FunSuite with BeforeAndAfterAll {

  def shouldRunIntegrationTest: Boolean = {
    sys.env.get("AWS_ACCESS_KEY_ID").exists(_.length > 0) &&
      sys.env.get("AZURE_TEST_ACCOUNT_KEY").exists(_.length > 0) &&
      sys.env.get("GOOGLE_APPLICATION_CREDENTIALS").exists(_.length > 0)
  }

  private var serverConfig: ServerConfig = _
  private var server: Server = _

  /**
   * Disable the ssl verification for Java's HTTP client because our local test server doesn't have
   * CA-signed certificate.
   */
  private def allowUntrustedServer(): Unit = {
    val trustAllCerts = Array[TrustManager](new X509TrustManager {
      override def getAcceptedIssuers(): Array[X509Certificate] = null

      override def checkClientTrusted(certs: Array[X509Certificate], authType: String) = {}

      override def checkServerTrusted(certs: Array[X509Certificate], authType: String) = {}
    })
    val sc = SSLContext.getInstance("SSL")
    sc.init(null, trustAllCerts, new java.security.SecureRandom())
    HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory())
    val allHostsValid = new HostnameVerifier {
      override def verify(s: String, sslSession: SSLSession): Boolean = true
    }
    HttpsURLConnection.setDefaultHostnameVerifier(allHostsValid)
  }

  private def verifyPreSignedUrl(url: String, expectedLength: Int): Unit = {
    // We should be able to read from the url
    assert(IOUtils.toByteArray(new URL(url)).size == expectedLength)

    // Modifying the file to access a different path should fail. This ensures the url is scoped
    // down to the specific file.
    val urlForDifferentObject = url.replaceAll("\\.parquet", ".orc")
    assert(url != urlForDifferentObject)
    val e = intercept[IOException] {
      IOUtils.toByteArray(new URL(urlForDifferentObject))
    }
    assert(e.getMessage.contains("Server returned HTTP response code: 403")) // 403 Forbidden
  }

  def requestPath(path: String): String = {
    s"https://${serverConfig.getHost}:${serverConfig.getPort}${serverConfig.getEndpoint}$path"
  }

  override def beforeAll() {
    if (shouldRunIntegrationTest) {
      allowUntrustedServer()
      val serverConfigPath = TestResource.setupTestTables().getCanonicalPath
      serverConfig = ServerConfig.load(serverConfigPath)
      serverConfig.evaluateJsonPredicateHints = true
      serverConfig.evaluateJsonPredicateHintsV2 = true
      server = DeltaSharingService.start(serverConfig)
    }
  }

  override def afterAll() {
    if (server != null) {
      server.stop().get()
    }
  }

  def readJson(
      url: String,
      expectedTableVersion: Option[Long] = None,
      asyncQuery: String = "false"): String = {
    readHttpContent(
      url,
      None,
      None,
      RESPONSE_FORMAT_PARQUET,
      expectedTableVersion,
      "application/json; charset=utf-8",
      asyncQuery = asyncQuery
    )
  }

  def readNDJson(
    url: String,
    method: Option[String] = None,
    data: Option[String] = None,
    expectedTableVersion: Option[Long] = None,
    responseFormat: String = RESPONSE_FORMAT_PARQUET,
    asyncQuery: String = "false"): String = {
    readHttpContent(
      url,
      method,
      data,
      responseFormat,
      expectedTableVersion,
      "application/x-ndjson; charset=utf-8",
      asyncQuery = asyncQuery
    )
  }

  def readHttpContent(
    url: String,
    method: Option[String],
    data: Option[String] = None,
    responseFormat: String,
    expectedTableVersion: Option[Long] = None,
    expectedContentType: String,
    asyncQuery: String = "true"): String = {
    val connection = new URL(url).openConnection().asInstanceOf[HttpsURLConnection]
    connection.setRequestProperty("Authorization", s"Bearer ${TestResource.testAuthorizationToken}")
    var deltaSharingCapabilities = s"asyncquery=$asyncQuery"
    if (responseFormat == RESPONSE_FORMAT_DELTA) {
      deltaSharingCapabilities += s";responseFormat=$responseFormat"
    }
    connection.setRequestProperty("delta-sharing-capabilities", deltaSharingCapabilities)

    method.foreach(connection.setRequestMethod)
    data.foreach { d =>
      connection.setDoOutput(true)
      connection.setRequestProperty("Content-Type", "application/json; charset=utf8")
      val output = connection.getOutputStream()
      try {
        output.write(d.getBytes(UTF_8))
      } finally {
        output.close()
      }
    }
    val input = connection.getInputStream()
    val content = try {
      IOUtils.toString(input)
    } finally {
      input.close()
    }
    val contentType = connection.getHeaderField("Content-Type")
    assert(
      expectedContentType == contentType,
      s"Incorrect content type: $contentType. Error: $content")
    if (expectedTableVersion.isDefined) {
      val responseCapabilities = connection.getHeaderField("delta-sharing-capabilities")
      assert(responseCapabilities == s"responseformat=$responseFormat",
        s"Incorrect response format: $responseCapabilities")
    }
    val deltaTableVersion = connection.getHeaderField("Delta-Table-Version")
    expectedTableVersion.foreach { v =>
      assert(v.toString == deltaTableVersion)
    }
    content
  }

  def integrationTest(testName: String)(func: => Unit): Unit = {
    test(testName) {
      assume(shouldRunIntegrationTest)
      func
    }
  }

  test("getCdfOptionsMap") {
    intercept[IllegalArgumentException] {
      DeltaSharingService.getCdfOptionsMap(None, None, None, None)
    }.getMessage.contains("No startingVersion or startingTimestamp provided for CDF read")

    intercept[IllegalArgumentException] {
      DeltaSharingService.getCdfOptionsMap(None, None, None, Some("endingTimestamp"))
    }.getMessage.contains("No startingVersion or startingTimestamp provided for CDF read")

    intercept[IllegalArgumentException] {
      DeltaSharingService.getCdfOptionsMap(Some("startingV"), None, Some("startingT"), None)
    }.getMessage.contains("Multiple starting arguments provided for CDF read")

    intercept[IllegalArgumentException] {
      DeltaSharingService.getCdfOptionsMap(Some("startV"), Some("endV"), None, Some("endT"))
    }.getMessage.contains("Multiple ending arguments provided for CDF read")

    intercept[IllegalArgumentException] {
      DeltaSharingService.getCdfOptionsMap(Some("startV"), Some("3"), None, None)
    }.getMessage.contains("startingVersion is not a valid number")

    intercept[IllegalArgumentException] {
      DeltaSharingService.getCdfOptionsMap(Some("2"), Some("endV"), None, None)
    }.getMessage.contains("endingVersion is not a valid number")
  }

  integrationTest("401 Unauthorized Error: incorrect token") {
    val url = requestPath("/shares")
    val connection = new URL(url).openConnection().asInstanceOf[HttpsURLConnection]
    connection.setRequestProperty("Authorization", s"Bearer incorrect-token")
    val e = intercept[IOException] {
      connection.getInputStream()
    }
    assert(e.getMessage.contains("Server returned HTTP response code: 401"))
  }

  integrationTest("401 Unauthorized Error: no token") {
    val url = requestPath("/shares")
    val connection = new URL(url).openConnection().asInstanceOf[HttpsURLConnection]
    val e = intercept[IOException] {
      connection.getInputStream()
    }
    assert(e.getMessage.contains("Server returned HTTP response code: 401"))
  }

  integrationTest("/shares") {
    val response = readJson(requestPath("/shares"))
    val expected = ListSharesResponse(
      Vector(
        Share().withName("share1"),
        Share().withName("share2"),
        Share().withName("share3"),
        Share().withName("share4"),
        Share().withName("share5"),
        Share().withName("share6"),
        Share().withName("share7"),
        Share().withName("share_azure"),
        Share().withName("share_gcp"),
        Share().withName("share8")
      )
    )
    assert(expected == JsonFormat.fromJsonString[ListSharesResponse](response))
  }

  integrationTest("/shares: maxResults") {
    var response =
      JsonFormat.fromJsonString[ListSharesResponse](readJson(requestPath("/shares?maxResults=1")))
    val shares = ArrayBuffer[Share]()
    shares ++= response.items
    while (response.nextPageToken.nonEmpty) {
      response = JsonFormat.fromJsonString[ListSharesResponse](readJson(requestPath(s"/shares?pageToken=${response.nextPageToken.get}&maxResults=1")))
      shares ++= response.items
    }
    val expected = Seq(
      Share().withName("share1"),
      Share().withName("share2"),
      Share().withName("share3"),
      Share().withName("share4"),
      Share().withName("share5"),
      Share().withName("share6"),
      Share().withName("share7"),
      Share().withName("share_azure"),
      Share().withName("share_gcp"),
      Share().withName("share8")
    )
    assert(expected == shares)
  }

  integrationTest("/shares/{share}") {
    val response = readJson(requestPath("/shares/share1"))
    val expected = GetShareResponse(Some(Share().withName("share1")))
    assert(expected == JsonFormat.fromJsonString[GetShareResponse](response))
  }

  integrationTest("/shares/{share}/schemas") {
    val response = readJson(requestPath("/shares/share1/schemas"))
    val expected = ListSchemasResponse(
      Schema().withName("default").withShare("share1") :: Nil)
    assert(expected == JsonFormat.fromJsonString[ListSchemasResponse](response))
  }

  integrationTest("/shares/{share}/schemas/{schema}/tables") {
    val response = readJson(requestPath("/shares/share1/schemas/default/tables"))
    val expected = ListTablesResponse(
      Table(
        name = Some("table1"),
        schema = Some("default"),
        share = Some("share1"),
        id = Some("00000000-0000-0000-0000-000000000001")
      )::
        Table(
          name = Some("table3"),
          schema = Some("default"),
          share = Some("share1"),
          id = Some("00000000-0000-0000-0000-000000000003")
        )::
        Table(
          name = Some("table7"),
          schema = Some("default"),
          share = Some("share1"),
          id = Some("00000000-0000-0000-0000-000000000007")
        )::Nil
    )
    assert(expected == JsonFormat.fromJsonString[ListTablesResponse](response))
  }

  integrationTest("/shares/{share}/schemas/{schema}/tables: maxResults") {
    var response = JsonFormat.fromJsonString[ListTablesResponse](readJson(requestPath("/shares/share1/schemas/default/tables?maxResults=1")))
    val tables = ArrayBuffer[Table]()
    tables ++= response.items
    while (response.nextPageToken.nonEmpty) {
      response = JsonFormat.fromJsonString[ListTablesResponse](readJson(requestPath(s"/shares/share1/schemas/default/tables?pageToken=${response.nextPageToken.get}&maxResults=1")))
      tables ++= response.items
    }
    val expected =
      Table(
        name = Some("table1"),
        schema = Some("default"),
        share = Some("share1"),
        id = Some("00000000-0000-0000-0000-000000000001")
      )::
        Table(
          name = Some("table3"),
          schema = Some("default"),
          share = Some("share1"),
          id = Some("00000000-0000-0000-0000-000000000003")
        )::
        Table(
          name = Some("table7"),
          schema = Some("default"),
          share = Some("share1"),
          id = Some("00000000-0000-0000-0000-000000000007")
        )::Nil
    assert(expected == tables)
  }

  integrationTest("/shares/{share}/all-tables") {
    val response = readJson(requestPath("/shares/share7/all-tables"))
    val expected = ListAllTablesResponse(
      Table(
        name = Some("table8"),
        schema = Some("schema1"),
        share = Some("share7"),
        id = Some("00000000-0000-0000-0000-000000000008")
      )::
        Table(
          name = Some("table9"),
          schema = Some("schema2"),
          share = Some("share7"),
          id = Some("00000000-0000-0000-0000-000000000009")
        )::Nil
    )
    assert(expected == JsonFormat.fromJsonString[ListAllTablesResponse](response))
  }

  integrationTest("/shares/{share}/all-tables: maxResults") {
    var response = JsonFormat.fromJsonString[ListAllTablesResponse](readJson(requestPath("/shares/share7/all-tables?maxResults=1")))
    val tables = ArrayBuffer[Table]()
    tables ++= response.items
    while (response.nextPageToken.nonEmpty) {
      response = JsonFormat.fromJsonString[ListAllTablesResponse](readJson(requestPath(s"/shares/share7/all-tables?pageToken=${response.nextPageToken.get}&maxResults=1")))
      tables ++= response.items
    }
    val expected =
      Table(
        name = Some("table8"),
        schema = Some("schema1"),
        share = Some("share7"),
        id = Some("00000000-0000-0000-0000-000000000008")
      )::
        Table(
          name = Some("table9"),
          schema = Some("schema2"),
          share = Some("share7"),
          id = Some("00000000-0000-0000-0000-000000000009")
        )::Nil
    assert(expected == tables)
  }


  integrationTest("table1 - head - /shares/{share}/schemas/{schema}/tables/{table}") {
    // getTableVersion succeeds without parameters
    var url = requestPath("/shares/share1/schemas/default/tables/table1")
    var connection = new URL(url).openConnection().asInstanceOf[HttpsURLConnection]
    connection.setRequestMethod("HEAD")
    connection.setRequestProperty("Authorization", s"Bearer ${TestResource.testAuthorizationToken}")
    var input = connection.getInputStream()
    try {
      IOUtils.toString(input)
    } finally {
      input.close()
    }
    var deltaTableVersion = connection.getHeaderField("Delta-Table-Version")
    assert(deltaTableVersion == "2")

    // getTableVersion succeeds with timestamp before version 0
    url = requestPath("/shares/share8/schemas/default/tables/cdf_table_cdf_enabled?startingTimestamp=2000-01-01T00:00:00Z")
    connection = new URL(url).openConnection().asInstanceOf[HttpsURLConnection]
    connection.setRequestMethod("HEAD")
    connection.setRequestProperty("Authorization", s"Bearer ${TestResource.testAuthorizationToken}")
    input = connection.getInputStream()
    try {
      IOUtils.toString(input)
    } finally {
      input.close()
    }
    deltaTableVersion = connection.getHeaderField("Delta-Table-Version")
    assert(deltaTableVersion == "0")

    // getTableVersion succeeds with timestamp after creation
    url = requestPath("/shares/share8/schemas/default/tables/cdf_table_cdf_enabled?startingTimestamp=2022-04-29T22:51:12Z")
    connection = new URL(url).openConnection().asInstanceOf[HttpsURLConnection]
    connection.setRequestMethod("HEAD")
    connection.setRequestProperty("Authorization", s"Bearer ${TestResource.testAuthorizationToken}")
    input = connection.getInputStream()
    try {
      IOUtils.toString(input)
    } finally {
      input.close()
    }
    deltaTableVersion = connection.getHeaderField("Delta-Table-Version")
    assert(deltaTableVersion == "5")
  }

  integrationTest("table1 - get - /shares/{share}/schemas/{schema}/tables/{table}/version") {
    // getTableVersion succeeds without parameters
    var url = requestPath("/shares/share1/schemas/default/tables/table1/version")
    var connection = new URL(url).openConnection().asInstanceOf[HttpsURLConnection]
    connection.setRequestMethod("GET")
    connection.setRequestProperty("Authorization", s"Bearer ${TestResource.testAuthorizationToken}")
    var input = connection.getInputStream()
    try {
      IOUtils.toString(input)
    } finally {
      input.close()
    }
    var deltaTableVersion = connection.getHeaderField("Delta-Table-Version")
    assert(deltaTableVersion == "2")

    // getTableVersion succeeds with parameters
    url = requestPath("/shares/share8/schemas/default/tables/cdf_table_cdf_enabled/version?startingTimestamp=2000-01-01T00:00:00Z")
    connection = new URL(url).openConnection().asInstanceOf[HttpsURLConnection]
    connection.setRequestMethod("GET")
    connection.setRequestProperty("Authorization", s"Bearer ${TestResource.testAuthorizationToken}")
    input = connection.getInputStream()
    try {
      IOUtils.toString(input)
    } finally {
      input.close()
    }
    deltaTableVersion = connection.getHeaderField("Delta-Table-Version")
    assert(deltaTableVersion == "0")
  }

  integrationTest("getTableVersion - head exceptions") {
    // timestamp can be any string here, it's resolved in DeltaSharedTableLoader
    assertHttpError(
      url = requestPath("/shares/share2/schemas/default/tables/table2?startingTimestamp=abc"),
      method = "HEAD",
      data = None,
      expectedErrorCode = 400,
      expectedErrorMessage = "Reading table by version or timestamp is not supported because "
    )

    // invalid startingTimestamp format
    assertHttpError(
      url = requestPath(
        "/shares/share8/schemas/default/tables/cdf_table_cdf_enabled?startingTimestamp=abc"
      ),
      method = "HEAD",
      data = None,
      expectedErrorCode = 400,
      expectedErrorMessage = "Invalid startingTimestamp"
    )

    // timestamp after the latest version
    assertHttpError(
      url = requestPath("/shares/share8/schemas/default/tables/cdf_table_cdf_enabled?startingTimestamp=9999-01-01T00:00:00Z"),
      method = "HEAD",
      data = None,
      expectedErrorCode = 400,
      expectedErrorMessage = "The provided timestamp (9999-01-01 00:00:00.0) is after the latest version available"
    )
  }

  integrationTest("getTableVersion - get exceptions") {
    // timestamp can be any string here, it's resolved in DeltaSharedTableLoader
    assertHttpError(
      url = requestPath("/shares/share2/schemas/default/tables/table2/version?startingTimestamp=abc"),
      method = "GET",
      data = None,
      expectedErrorCode = 400,
      expectedErrorMessage = "Reading table by version or timestamp is not supported because "
    )

    // invalid startingTimestamp format
    assertHttpError(
      url = requestPath(
        "/shares/share8/schemas/default/tables/cdf_table_cdf_enabled/version?startingTimestamp=abc"
      ),
      method = "GET",
      data = None,
      expectedErrorCode = 400,
      expectedErrorMessage = "Invalid startingTimestamp"
    )

    // timestamp after the latest version
    assertHttpError(
      url = requestPath("/shares/share8/schemas/default/tables/cdf_table_cdf_enabled/version?startingTimestamp=9999-01-01T00:00:00-08:00"),
      method = "GET",
      data = None,
      expectedErrorCode = 400,
      expectedErrorMessage = "The provided timestamp "
    )
  }

  integrationTest("table1 - non partitioned - /shares/{share}/schemas/{schema}/tables/{table}/metadata") {
    Seq(
      RESPONSE_FORMAT_PARQUET,
      RESPONSE_FORMAT_DELTA,
      s"$RESPONSE_FORMAT_DELTA,$RESPONSE_FORMAT_PARQUET",
      s"$RESPONSE_FORMAT_PARQUET,$RESPONSE_FORMAT_DELTA"
    ).foreach { responseFormat =>
      val respondedFormat = if (responseFormat == RESPONSE_FORMAT_DELTA) {
        RESPONSE_FORMAT_DELTA
      } else {
        RESPONSE_FORMAT_PARQUET
      }
      val response = readNDJson(requestPath(s"/shares/share1/schemas/default/tables/table1/metadata"), responseFormat = respondedFormat, expectedTableVersion = Some(2))
      val Array(protocol, metadata) = response.split("\n")
      if (responseFormat == RESPONSE_FORMAT_DELTA) {
        val responseProtocol = JsonUtils.fromJson[DeltaResponseSingleAction](protocol).protocol
        assert(responseProtocol.deltaProtocol.minReaderVersion == 1)
        val responseMetadata = JsonUtils.fromJson[DeltaResponseSingleAction](metadata).metaData
        assert(responseMetadata.deltaMetadata.id == "ed96aa41-1d81-4b7f-8fb5-846878b4b0cf")
        assert(responseMetadata.deltaMetadata.schemaString == """{"type":"struct","fields":[{"name":"eventTime","type":"timestamp","nullable":true,"metadata":{}},{"name":"date","type":"date","nullable":true,"metadata":{}}]}""")
      } else {
        val expectedProtocol = Protocol(minReaderVersion = 1).wrap
        assert(expectedProtocol == JsonUtils.fromJson[SingleAction](protocol))
        val expectedMetadata = Metadata(
          id = "ed96aa41-1d81-4b7f-8fb5-846878b4b0cf",
          format = Format(),
          schemaString = """{"type":"struct","fields":[{"name":"eventTime","type":"timestamp","nullable":true,"metadata":{}},{"name":"date","type":"date","nullable":true,"metadata":{}}]}""",
          partitionColumns = Nil).wrap
        assert(expectedMetadata == JsonUtils.fromJson[SingleAction](metadata))
      }
    }
  }

  integrationTest("table1 - non partitioned - /shares/{share}/schemas/{schema}/tables/{table}/query") {
    Seq(
      RESPONSE_FORMAT_PARQUET,
      RESPONSE_FORMAT_DELTA,
      s"$RESPONSE_FORMAT_DELTA,$RESPONSE_FORMAT_PARQUET",
      s"$RESPONSE_FORMAT_PARQUET,$RESPONSE_FORMAT_DELTA"
    ).foreach { responseFormat =>
      val respondedFormat = if (responseFormat == RESPONSE_FORMAT_DELTA) {
        RESPONSE_FORMAT_DELTA
      } else {
        RESPONSE_FORMAT_PARQUET
      }
      val p =
        s"""
          |{
          |  "predicateHints": [
          |    "date = CAST('2021-04-28' AS DATE)"
          |  ]
          |}
          |""".stripMargin
      val response = readNDJson(requestPath("/shares/share1/schemas/default/tables/table1/query"), Some("POST"), Some(p), Some(2), respondedFormat)
      val lines = response.split("\n")
      val protocol = lines(0)
      val metadata = lines(1)
      if (responseFormat == RESPONSE_FORMAT_DELTA) {
        val responseProtocol = JsonUtils.fromJson[DeltaResponseSingleAction](protocol).protocol
        assert(responseProtocol.deltaProtocol.minReaderVersion == 1)

        // unable to construct the delta action because the cases classes like AddFile/Metadata
        // are private to io.delta.standalone.internal.
        // So we only verify a couple important fields.
        val responseMetadata = JsonUtils.fromJson[DeltaResponseSingleAction](metadata).metaData
        assert(responseMetadata.deltaMetadata.id == "ed96aa41-1d81-4b7f-8fb5-846878b4b0cf")

        val actualFiles = lines.drop(2).map(f => JsonUtils.fromJson[DeltaResponseSingleAction](f).file)
        assert(actualFiles(0).id == "061cb3683a467066995f8cdaabd8667d")
        assert(actualFiles(0).deltaSingleAction.add != null)
        assert(actualFiles(1).id == "e268cbf70dbaa6143e7e9fa3e2d3b00e")
        assert(actualFiles(1).deltaSingleAction.add != null)
        assert(actualFiles.count(_.expirationTimestamp > System.currentTimeMillis()) == 2)
        verifyPreSignedUrl(actualFiles(0).deltaSingleAction.add.path, 781)
        verifyPreSignedUrl(actualFiles(1).deltaSingleAction.add.path, 781)
      } else {
        val expectedProtocol = Protocol(minReaderVersion = 1).wrap
        assert(expectedProtocol == JsonUtils.fromJson[SingleAction](protocol))
        val expectedMetadata = Metadata(
          id = "ed96aa41-1d81-4b7f-8fb5-846878b4b0cf",
          format = Format(),
          schemaString = """{"type":"struct","fields":[{"name":"eventTime","type":"timestamp","nullable":true,"metadata":{}},{"name":"date","type":"date","nullable":true,"metadata":{}}]}""",
          partitionColumns = Nil).wrap
        assert(expectedMetadata == JsonUtils.fromJson[SingleAction](metadata))
        val files = lines.drop(2)
        val actualFiles = files.map(f => JsonUtils.fromJson[SingleAction](f).file)
        assert(actualFiles.size == 2)
        val expectedFiles = Seq(
          AddFile(
            url = actualFiles(0).url,
            expirationTimestamp = actualFiles(0).expirationTimestamp,
            id = "061cb3683a467066995f8cdaabd8667d",
            partitionValues = Map.empty,
            size = 781,
            stats = """{"numRecords":1,"minValues":{"eventTime":"2021-04-28T06:32:22.421Z","date":"2021-04-28"},"maxValues":{"eventTime":"2021-04-28T06:32:22.421Z","date":"2021-04-28"},"nullCount":{"eventTime":0,"date":0}}"""
          ),
          AddFile(
            url = actualFiles(1).url,
            expirationTimestamp = actualFiles(1).expirationTimestamp,
            id = "e268cbf70dbaa6143e7e9fa3e2d3b00e",
            partitionValues = Map.empty,
            size = 781,
            stats = """{"numRecords":1,"minValues":{"eventTime":"2021-04-28T06:32:02.070Z","date":"2021-04-28"},"maxValues":{"eventTime":"2021-04-28T06:32:02.070Z","date":"2021-04-28"},"nullCount":{"eventTime":0,"date":0}}"""
          )
        )
        assert(actualFiles.count(_.expirationTimestamp != null) == 2)
        assert(expectedFiles == actualFiles.toList)
        verifyPreSignedUrl(actualFiles(0).url, 781)
        verifyPreSignedUrl(actualFiles(1).url, 781)
      }
    }
  }

  integrationTest("table1 async query without idempotency_key") {
    Seq(RESPONSE_FORMAT_PARQUET, RESPONSE_FORMAT_DELTA).foreach { responseFormat =>
      assertHttpError(
        requestPath("/shares/share1/schemas/default/tables/table1/query"),
        method = "POST",
        data = Some("""{"maxFiles": 1}"""),
        expectedErrorCode = 400,
        expectedErrorMessage = "idempotency_key is required for async query",
        headers = Map("delta-sharing-capabilities" -> s"asyncquery=true;responseFormat=$responseFormat")
      )
    }
  }

  integrationTest("table1 async query") {
    Seq(RESPONSE_FORMAT_PARQUET, RESPONSE_FORMAT_DELTA).foreach { responseFormat =>
      val response = readNDJson(
        requestPath("/shares/share1/schemas/default/tables/table1/query"),
        Some("POST"),
        Some("""{"idempotency_key": "random id"}"""),
        expectedTableVersion = None,
        responseFormat,
        asyncQuery = "true"
      )

      assert(JsonUtils.fromJson[SingleAction](response).queryStatus != null)
    }
  }

  integrationTest("table1 async query get status") {
    Seq(RESPONSE_FORMAT_PARQUET, RESPONSE_FORMAT_DELTA).foreach { responseFormat =>
      val response = readNDJson(
        requestPath("/shares/share1/schemas/default/tables/table1/queries/1234"),
        method = Some("POST"),
        Some("""{"maxFiles": 1}"""),
        expectedTableVersion = None,
        responseFormat,
        asyncQuery = "true"
      )

      var lines = response.split("\n")
      assert(lines.length == 4)

      print(s"response: $response")
    }
  }

  integrationTest("table1 - non partitioned - paginated query") {
    Seq(RESPONSE_FORMAT_PARQUET, RESPONSE_FORMAT_DELTA).foreach { responseFormat =>
      var response = readNDJson(
        requestPath("/shares/share1/schemas/default/tables/table1/query"),
        Some("POST"),
        Some("""{"maxFiles": 1}"""),
        Some(2),
        responseFormat
      )
      var lines = response.split("\n")
      assert(lines.length == 4)
      val protocol = lines(0)
      val metadata = lines(1)
      val files = ArrayBuffer[String]()
      files.append(lines(2))
      var endAction = JsonUtils.fromJson[SingleAction](lines(3)).endStreamAction
      assert(endAction.minUrlExpirationTimestamp != null)
      var numPages = 1
      while (endAction.nextPageToken != null) {
        numPages += 1
        response = readNDJson(
          requestPath("/shares/share1/schemas/default/tables/table1/query"),
          Some("POST"),
          Some(s"""{"maxFiles": 1, "pageToken": "${endAction.nextPageToken}"}"""),
          Some(2),
          responseFormat
        )
        lines = response.split("\n")
        assert(lines.length == 4)
        assert(protocol == lines(0))
        assert(metadata == lines(1))
        files.append(lines(2))
        endAction = JsonUtils.fromJson[SingleAction](lines(3)).endStreamAction
        assert(endAction.minUrlExpirationTimestamp != null)
      }
      assert(numPages == 2)

      if (responseFormat == RESPONSE_FORMAT_DELTA) {
        val responseProtocol = JsonUtils.fromJson[DeltaResponseSingleAction](protocol).protocol
        assert(responseProtocol.deltaProtocol.minReaderVersion == 1)

        // unable to construct the delta action because the cases classes like AddFile/Metadata
        // are private to io.delta.standalone.internal.
        // So we only verify a couple important fields.
        val responseMetadata = JsonUtils.fromJson[DeltaResponseSingleAction](metadata).metaData
        assert(responseMetadata.deltaMetadata.id == "ed96aa41-1d81-4b7f-8fb5-846878b4b0cf")

        val actualFiles = files.map(f => JsonUtils.fromJson[DeltaResponseSingleAction](f).file)
        assert(actualFiles(0).id == "061cb3683a467066995f8cdaabd8667d")
        assert(actualFiles(0).deltaSingleAction.add != null)
        assert(actualFiles(1).id == "e268cbf70dbaa6143e7e9fa3e2d3b00e")
        assert(actualFiles(1).deltaSingleAction.add != null)
        assert(actualFiles.count(_.expirationTimestamp > System.currentTimeMillis()) == 2)
        verifyPreSignedUrl(actualFiles(0).deltaSingleAction.add.path, 781)
        verifyPreSignedUrl(actualFiles(1).deltaSingleAction.add.path, 781)
      } else {
        val expectedProtocol = Protocol(minReaderVersion = 1).wrap
        assert(expectedProtocol == JsonUtils.fromJson[SingleAction](protocol))
        val expectedMetadata = Metadata(
          id = "ed96aa41-1d81-4b7f-8fb5-846878b4b0cf",
          format = Format(),
          schemaString =
            """{"type":"struct","fields":[{"name":"eventTime","type":"timestamp","nullable":true,"metadata":{}},{"name":"date","type":"date","nullable":true,"metadata":{}}]}""",
          partitionColumns = Nil
        ).wrap
        assert(expectedMetadata == JsonUtils.fromJson[SingleAction](metadata))
        val actualFiles = files.map(f => JsonUtils.fromJson[SingleAction](f).file)
        val expectedFiles = Seq(
          AddFile(
            url = actualFiles(0).url,
            expirationTimestamp = actualFiles(0).expirationTimestamp,
            id = "061cb3683a467066995f8cdaabd8667d",
            partitionValues = Map.empty,
            size = 781,
            stats =
              """{"numRecords":1,"minValues":{"eventTime":"2021-04-28T06:32:22.421Z","date":"2021-04-28"},"maxValues":{"eventTime":"2021-04-28T06:32:22.421Z","date":"2021-04-28"},"nullCount":{"eventTime":0,"date":0}}"""
          ),
          AddFile(
            url = actualFiles(1).url,
            expirationTimestamp = actualFiles(1).expirationTimestamp,
            id = "e268cbf70dbaa6143e7e9fa3e2d3b00e",
            partitionValues = Map.empty,
            size = 781,
            stats =
              """{"numRecords":1,"minValues":{"eventTime":"2021-04-28T06:32:02.070Z","date":"2021-04-28"},"maxValues":{"eventTime":"2021-04-28T06:32:02.070Z","date":"2021-04-28"},"nullCount":{"eventTime":0,"date":0}}"""
          )
        )
        assert(actualFiles.count(_.expirationTimestamp != null) == 2)
        assert(expectedFiles == actualFiles.toList)
        verifyPreSignedUrl(actualFiles(0).url, 781)
        verifyPreSignedUrl(actualFiles(1).url, 781)
      }
    }
  }

  integrationTest("refresh query returns the same set of files as initial query") {
    val initialResponse = readNDJson(
      requestPath("/shares/share1/schemas/default/tables/table1/query"),
      Some("POST"),
      Some("""{"includeRefreshToken": true}"""),
      Some(2)
    ).split("\n")
    assert(initialResponse.length == 5)
    val endAction = JsonUtils.fromJson[SingleAction](initialResponse.last).endStreamAction
    assert(endAction.refreshToken != null)

    val refreshResponse = readNDJson(
      requestPath("/shares/share1/schemas/default/tables/table1/query"),
      Some("POST"),
      Some(s"""{"includeRefreshToken": true, "refreshToken": "${endAction.refreshToken}"}"""),
      Some(2)
    ).split("\n")
    assert(refreshResponse.length == 5)
    // protocol
    assert(initialResponse(0) == refreshResponse(0))
    // metadata
    assert(initialResponse(1) == refreshResponse(1))
    // files
    val initialFiles = initialResponse.slice(2, 4).map(f => JsonUtils.fromJson[SingleAction](f).file)
    val refreshedFiles = refreshResponse.slice(2, 4).map(f => JsonUtils.fromJson[SingleAction](f).file)
    assert(initialFiles.map(_.id) sameElements refreshedFiles.map(_.id))
  }

  integrationTest("refresh query - exception") {
    // invalid refresh token
    assertHttpError(
      url = requestPath("/shares/share1/schemas/default/tables/table1/query"),
      method = "POST",
      data = Some("""{"includeRefreshToken": true, "refreshToken": "foo"}"""),
      expectedErrorCode = 400,
      expectedErrorMessage = "Error decoding refresh token"
    )

    // invalid query parameters
    assertHttpError(
      url = requestPath("/shares/share8/schemas/default/tables/streaming_notnull_to_null/query"),
      method = "POST",
      data = Some("""{"version": 1, "includeRefreshToken": true}"""),
      expectedErrorCode = 400,
      expectedErrorMessage = "includeRefreshToken cannot be used when querying a specific version"
    )
    assertHttpError(
      url = requestPath("/shares/share1/schemas/default/tables/table1/query"),
      method = "POST",
      data = Some("""{"pageToken": "foo", "includeRefreshToken": true}"""),
      expectedErrorCode = 400,
      expectedErrorMessage = "includeRefreshToken must be used in the first page request"
    )
    assertHttpError(
      url = requestPath("/shares/share8/schemas/default/tables/streaming_notnull_to_null/query"),
      method = "POST",
      data = Some("""{"startingVersion": 1, "refreshToken": "foo"}"""),
      expectedErrorCode = 400,
      expectedErrorMessage = "refreshToken cannot be used when querying a specific version"
    )
    assertHttpError(
      url = requestPath("/shares/share1/schemas/default/tables/table1/query"),
      method = "POST",
      data = Some("""{"pageToken": "foo", "refreshToken": "foo"}"""),
      expectedErrorCode = 400,
      expectedErrorMessage = "refreshToken must be used in the first page request"
    )

    var response = readNDJson(
      requestPath("/shares/share1/schemas/default/tables/table1/query"),
      Some("POST"),
      Some("""{"includeRefreshToken": true}"""),
      Some(2)
    )
    var lines = response.split("\n")
    assert(lines.length == 5)
    var endAction = JsonUtils.fromJson[SingleAction](lines.last).endStreamAction
    assert(endAction.refreshToken != null)
    assertHttpError(
      url = requestPath("/shares/share2/schemas/default/tables/table2/query"),
      method = "POST",
      data = Some(s"""{"includeRefreshToken": true, "refreshToken": "${endAction.refreshToken}"}"""),
      expectedErrorCode = 400,
      expectedErrorMessage = "The table specified in the refresh token does not match the table being queried"
    )

    // refresh token expired
    val updatedServerConfig = serverConfig.copy(refreshTokenTtlMs = 0)
    server.stop().get()
    server = DeltaSharingService.start(updatedServerConfig)
    response = readNDJson(
      requestPath("/shares/share1/schemas/default/tables/table1/query"),
      Some("POST"),
      Some("""{"includeRefreshToken": true}"""),
      Some(2)
    )
    lines = response.split("\n")
    assert(lines.length == 5)
    endAction = JsonUtils.fromJson[SingleAction](lines.last).endStreamAction
    assert(endAction.refreshToken != null)

    assertHttpError(
      url = requestPath("/shares/share1/schemas/default/tables/table1/query"),
      method = "POST",
      data = Some(s"""{"includeRefreshToken": true, "refreshToken": "${endAction.refreshToken}"}"""),
      expectedErrorCode = 400,
      expectedErrorMessage = "The refresh token has expired"
    )

    server.stop().get()
    server = DeltaSharingService.start(serverConfig)
  }

  integrationTest("table2 - partitioned - /shares/{share}/schemas/{schema}/tables/{table}/metadata") {
    val response = readNDJson(requestPath("/shares/share2/schemas/default/tables/table2/metadata"), expectedTableVersion = Some(2))
    val Array(protocol, metadata) = response.split("\n")
    val expectedProtocol = Protocol(minReaderVersion = 1).wrap
    assert(expectedProtocol == JsonUtils.fromJson[SingleAction](protocol))
    val expectedMetadata = Metadata(
      id = "f8d5c169-3d01-4ca3-ad9e-7dc3355aedb2",
      format = Format(),
      schemaString = """{"type":"struct","fields":[{"name":"eventTime","type":"timestamp","nullable":true,"metadata":{}},{"name":"date","type":"date","nullable":true,"metadata":{}}]}""",
      partitionColumns = Seq("date")).wrap
    assert(expectedMetadata == JsonUtils.fromJson[SingleAction](metadata))
  }

  integrationTest("table_with_no_metadata - metadata missing") {
    assertHttpError(
      url = requestPath("/shares/share8/schemas/default/tables/table_with_no_metadata/version"),
      method = "GET",
      data = None,
      expectedErrorCode = 500,
      expectedErrorMessage = ""
    )
  }

  integrationTest("table2 - version 1 : historyShared is false") {
    assertHttpError(
      url = requestPath("/shares/share2/schemas/default/tables/table2/query"),
      method = "POST",
      data = Some("""{"version": 1}"""),
      expectedErrorCode = 400,
      expectedErrorMessage = "Reading table by version or timestamp is not supported because history sharing is not enabled on table: share2.default.table2"
    )
  }

  integrationTest("table2 - timestamp not supported: historyShared is false") {
    // timestamp can be any string here, it's resolved in DeltaSharedTableLoader
    assertHttpError(
      url = requestPath("/shares/share2/schemas/default/tables/table2/query"),
      method = "POST",
      data = Some("""{"timestamp": "abc"}"""),
      expectedErrorCode = 400,
      expectedErrorMessage = "Reading table by version or timestamp is not supported because history sharing is not enabled on table: share2.default.table2"
    )
  }

  integrationTest("table2 - time travel metadata not supported: historyShared is false") {
    // timestamp can be any string here, it's resolved in DeltaSharedTableLoader
    assertHttpError(
      url = requestPath("/shares/share2/schemas/default/tables/table2/metadata?version=1"),
      method = "GET",
      data = None,
      expectedErrorCode = 400,
      expectedErrorMessage = "Reading table by version or timestamp is not supported because history sharing is not enabled on table: share2.default.table2"
    )
  }

  integrationTest("table2 - partitioned - /shares/{share}/schemas/{schema}/tables/{table}/query") {
    val p =
      """
        |{
        |  "predicateHints": [
        |    "date = CAST('2021-04-28' AS DATE)"
        |  ],
        |  "limitHint": 123
        |}
        |""".stripMargin
    val response = readNDJson(requestPath("/shares/share2/schemas/default/tables/table2/query"), Some("POST"), Some(p), Some(2))
    val lines = response.split("\n")
    val protocol = lines(0)
    val metadata = lines(1)
    val expectedProtocol = Protocol(minReaderVersion = 1).wrap
    assert(expectedProtocol == JsonUtils.fromJson[SingleAction](protocol))
    val expectedMetadata = Metadata(
      id = "f8d5c169-3d01-4ca3-ad9e-7dc3355aedb2",
      format = Format(),
      schemaString = """{"type":"struct","fields":[{"name":"eventTime","type":"timestamp","nullable":true,"metadata":{}},{"name":"date","type":"date","nullable":true,"metadata":{}}]}""",
      partitionColumns = Seq("date")).wrap
    assert(expectedMetadata == JsonUtils.fromJson[SingleAction](metadata))
    val files = lines.drop(2)
    val actualFiles = files.map(f => JsonUtils.fromJson[SingleAction](f).file)
    assert(actualFiles.size == 2)
    val expectedFiles = Seq(
      AddFile(
        url = actualFiles(0).url,
        expirationTimestamp = actualFiles(0).expirationTimestamp,
        id = "9f1a49539c5cffe1ea7f9e055d5c003c",
        partitionValues = Map("date" -> "2021-04-28"),
        size = 573,
        stats = """{"numRecords":1,"minValues":{"eventTime":"2021-04-28T23:33:57.955Z"},"maxValues":{"eventTime":"2021-04-28T23:33:57.955Z"},"nullCount":{"eventTime":0}}"""
      ),
      AddFile(
        url = actualFiles(1).url,
        expirationTimestamp = actualFiles(1).expirationTimestamp,
        id = "cd2209b32f5ed5305922dd50f5908a75",
        partitionValues = Map("date" -> "2021-04-28"),
        size = 573,
        stats = """{"numRecords":1,"minValues":{"eventTime":"2021-04-28T23:33:48.719Z"},"maxValues":{"eventTime":"2021-04-28T23:33:48.719Z"},"nullCount":{"eventTime":0}}"""
      )
    )
    assert(actualFiles.count(_.expirationTimestamp != null) == 2)
    assert(expectedFiles == actualFiles.toList)
    verifyPreSignedUrl(actualFiles(0).url, 573)
    verifyPreSignedUrl(actualFiles(1).url, 573)
  }

  integrationTest("table2 - partitioned - paginated query") {
    var body =
      """
        |{
        |  "predicateHints": [
        |    "date = CAST('2021-04-28' AS DATE)"
        |  ],
        |  "maxFiles": 1
        |}
        |""".stripMargin
    var response = readNDJson(
      requestPath("/shares/share2/schemas/default/tables/table2/query"),
      Some("POST"),
      Some(body),
      Some(2)
    )
    var lines = response.split("\n")
    assert(lines.length == 4)
    val protocol = lines(0)
    val metadata = lines(1)
    val files = ArrayBuffer[String]()
    files.append(lines(2))
    var endAction = JsonUtils.fromJson[SingleAction](lines(3)).endStreamAction
    assert(endAction.minUrlExpirationTimestamp != null)
    var numPages = 1
    while (endAction.nextPageToken != null) {
      numPages += 1
      body =
        s"""
           |{
           |  "predicateHints": [
           |    "date = CAST('2021-04-28' AS DATE)"
           |  ],
           |  "maxFiles": 1,
           |  "pageToken": "${endAction.nextPageToken}"
           |}
           |""".stripMargin
      response = readNDJson(
        requestPath("/shares/share2/schemas/default/tables/table2/query"),
        Some("POST"),
        Some(body),
        Some(2)
      )
      lines = response.split("\n")
      assert(lines.length == 4)
      assert(protocol == lines(0))
      assert(metadata == lines(1))
      files.append(lines(2))
      endAction = JsonUtils.fromJson[SingleAction](lines(3)).endStreamAction
      assert(endAction.minUrlExpirationTimestamp != null)
    }
    assert(numPages == 2)

    val expectedProtocol = Protocol(minReaderVersion = 1).wrap
    assert(expectedProtocol == JsonUtils.fromJson[SingleAction](protocol))
    val expectedMetadata = Metadata(
      id = "f8d5c169-3d01-4ca3-ad9e-7dc3355aedb2",
      format = Format(),
      schemaString =
        """{"type":"struct","fields":[{"name":"eventTime","type":"timestamp","nullable":true,"metadata":{}},{"name":"date","type":"date","nullable":true,"metadata":{}}]}""",
      partitionColumns = Seq("date")
    ).wrap
    assert(expectedMetadata == JsonUtils.fromJson[SingleAction](metadata))
    val actualFiles = files.map(f => JsonUtils.fromJson[SingleAction](f).file)
    val expectedFiles = Seq(
      AddFile(
        url = actualFiles(0).url,
        expirationTimestamp = actualFiles(0).expirationTimestamp,
        id = "9f1a49539c5cffe1ea7f9e055d5c003c",
        partitionValues = Map("date" -> "2021-04-28"),
        size = 573,
        stats =
          """{"numRecords":1,"minValues":{"eventTime":"2021-04-28T23:33:57.955Z"},"maxValues":{"eventTime":"2021-04-28T23:33:57.955Z"},"nullCount":{"eventTime":0}}"""
      ),
      AddFile(
        url = actualFiles(1).url,
        expirationTimestamp = actualFiles(1).expirationTimestamp,
        id = "cd2209b32f5ed5305922dd50f5908a75",
        partitionValues = Map("date" -> "2021-04-28"),
        size = 573,
        stats =
          """{"numRecords":1,"minValues":{"eventTime":"2021-04-28T23:33:48.719Z"},"maxValues":{"eventTime":"2021-04-28T23:33:48.719Z"},"nullCount":{"eventTime":0}}"""
      )
    )
    assert(actualFiles.count(_.expirationTimestamp != null) == 2)
    assert(expectedFiles == actualFiles.toList)
    verifyPreSignedUrl(actualFiles(0).url, 573)
    verifyPreSignedUrl(actualFiles(1).url, 573)
  }

  integrationTest("jsonPredicateTest") {
    // A test function that applies specified predicate hints on cdf_table_with_partition
    // table which has two files with dates (2020-01-01, 2020-02-02)
    //
    // The param "expectedDatesSorted" specifies which of the two files we expect for the
    // specified hints.
    def testPredicateHints(hints: String, expectedDatesSorted: Seq[String]): Unit = {
      val jsonPredicateStr = JsonUtils.toJson(Map("jsonPredicateHints" -> hints))
      val response = readNDJson(
        requestPath("/shares/share8/schemas/default/tables/cdf_table_with_partition/query"),
        Some("POST"),
        Some(jsonPredicateStr),
        Some(3)
      )

      // Sort the files and match the expected dates.
      // The date string should be present in the corresponding file name.
      val lines = response.split("\n").toSeq
      val files = lines.drop(2).map(f => JsonUtils.fromJson[SingleAction](f).file.url).sorted
      assert(files.size == expectedDatesSorted.size)
      for (i <- 0 to expectedDatesSorted.size - 1) {
        assert(files(i).contains(expectedDatesSorted(i)))
      }
    }
    testPredicateHints("", Seq("2020-01-01", "2020-02-02"))

    val hints1 =
      """{"op":"and","children":[
           |  {"op":"not","children":[
           |    {"op":"isNull","children":[
           |      {"op":"column","name":"birthday","valueType":"date"}]}]},
           |  {"op":"equal","children":[
           |    {"op":"column","name":"birthday","valueType":"date"},
           |    {"op":"literal","value":"2020-01-01","valueType":"date"}]}
           |]}""".stripMargin.replaceAll("\n", "").replaceAll(" ", "")
    testPredicateHints(hints1, Seq("2020-01-01"))

    val hints2 =
      """{"op":"and","children":[
           |  {"op":"not","children":[
           |    {"op":"isNull","children":[
           |      {"op":"column","name":"birthday","valueType":"date"}]}]},
           |  {"op":"greaterThan","children":[
           |    {"op":"column","name":"birthday","valueType":"date"},
           |    {"op":"literal","value":"2020-01-01","valueType":"date"}]}
           |]}""".stripMargin.replaceAll("\n", "").replaceAll(" ", "")
    testPredicateHints(hints2, Seq("2020-02-02"))

    val hints3 =
      """{"op":"and","children":[
           |  {"op":"lessThan","children":[
           |    {"op":"column","name":"birthday","valueType":"date"},
           |    {"op":"literal","value":"2020-02-03","valueType":"date"}]},
           |  {"op":"greaterThanOrEqual","children":[
           |    {"op":"column","name":"birthday","valueType":"date"},
           |    {"op":"literal","value":"2020-01-01","valueType":"date"}]}
           |]}""".stripMargin.replaceAll("\n", "").replaceAll(" ", "")
    testPredicateHints(hints3, Seq("2020-01-01", "2020-02-02"))

    // hints on stats column age.
    val hints4 =
      """{"op":"and","children":[
           |  {"op":"not","children":[
           |    {"op":"isNull","children":[
           |      {"op":"column","name":"birthday","valueType":"date"}]}]},
           |  {"op":"equal","children":[
           |    {"op":"column","name":"age","valueType":"int"},
           |    {"op":"literal","value":"2","valueType":"int"}]}
           |]}""".stripMargin.replaceAll("\n", "").replaceAll(" ", "")
    testPredicateHints(hints4, Seq("2020-02-02"))

    // hints on stats column age.
    val hints5 =
      """{"op":"and","children":[
           |  {"op":"not","children":[
           |    {"op":"isNull","children":[
           |      {"op":"column","name":"birthday","valueType":"date"}]}]},
           |  {"op":"lessThan","children":[
           |    {"op":"column","name":"age","valueType":"int"},
           |    {"op":"literal","value":"3","valueType":"int"}]}
           |]}""".stripMargin.replaceAll("\n", "").replaceAll(" ", "")
    testPredicateHints(hints5, Seq("2020-01-01", "2020-02-02"))

    // hints on stats and partition columns.
    val hints6 =
      """{"op":"and","children":[
           |  {"op":"lessThan","children":[
           |    {"op":"column","name":"birthday","valueType":"date"},
           |    {"op":"literal","value":"2020-02-03","valueType":"date"}]},
           |  {"op":"greaterThanOrEqual","children":[
           |    {"op":"column","name":"birthday","valueType":"date"},
           |    {"op":"literal","value":"2020-01-01","valueType":"date"}]},
           |  {"op":"lessThan","children":[
           |    {"op":"column","name":"age","valueType":"int"},
           |    {"op":"literal","value":"2","valueType":"int"}]}
           |]}""".stripMargin.replaceAll("\n", "").replaceAll(" ", "")
    testPredicateHints(hints6, Seq("2020-01-01"))
  }

  integrationTest("paginated query with jsonPredicates") {
    // cdf_table_with_partition has two files with dates (2020-01-01, 2020-02-02)
    val hints =
      """{"op":"and","children":[
        |  {"op":"not","children":[
        |    {"op":"isNull","children":[
        |      {"op":"column","name":"birthday","valueType":"date"}]}]},
        |  {"op":"equal","children":[
        |    {"op":"column","name":"birthday","valueType":"date"},
        |    {"op":"literal","value":"2020-01-01","valueType":"date"}]}
        |]}""".stripMargin.replaceAll("\n", "").replaceAll(" ", "")
    val response = readNDJson(
      requestPath("/shares/share8/schemas/default/tables/cdf_table_with_partition/query"),
      Some("POST"),
      Some(JsonUtils.toJson(Map("jsonPredicateHints" -> hints, "maxFiles" -> 1))),
      Some(3)
    )
    val actions = response.split("\n").map(JsonUtils.fromJson[SingleAction](_))
    assert(actions.length == 4)
    val expectedProtocol = Protocol(minReaderVersion = 1)
    assert(expectedProtocol == actions(0).protocol)
    val expectedMetadata = Metadata(
      id = "e21eb083-6976-4159-90f2-ad88d06b7c7f",
      format = Format(),
      schemaString =
        """{"type":"struct","fields":[{"name":"name","type":"string","nullable":true,"metadata":{}},{"name":"age","type":"integer","nullable":true,"metadata":{}},{"name":"birthday","type":"date","nullable":true,"metadata":{}}]}""",
      configuration = Map("enableChangeDataFeed" -> "true"),
      partitionColumns = Seq("birthday")
    )
    assert(expectedMetadata == actions(1).metaData)
    val actualFile = actions(2).file
    val expectedAddFile = AddFile(
      url = actualFile.url,
      expirationTimestamp = actualFile.expirationTimestamp,
      id = "a04d61f17541fac1f9b5df5b8d26fff8",
      partitionValues = Map("birthday" -> "2020-01-01"),
      size = 791,
      stats =
        """{"numRecords":1,"minValues":{"name":"1","age":1},"maxValues":{"name":"1","age":1},"nullCount":{"name":0,"age":0}}"""
    )
    assert(expectedAddFile == actualFile)
    val endAction = actions(3).endStreamAction
    assert(endAction.nextPageToken == null)
    assert(endAction.minUrlExpirationTimestamp == actualFile.expirationTimestamp)
  }

  integrationTest("paginated query - exceptions") {
    // invalid page token
    assertHttpError(
      url = requestPath("/shares/share1/schemas/default/tables/table1/query"),
      method = "POST",
      data = Some("""{"pageToken": "randomPageToken"}"""),
      expectedErrorCode = 400,
      expectedErrorMessage = "Error decoding the page token"
    )

    // invalid query parameters
    var response = readNDJson(
      requestPath("/shares/share1/schemas/default/tables/table1/query"),
      Some("POST"),
      Some("""{"maxFiles": 1}"""),
      Some(2)
    )
    var lines = response.split("\n")
    assert(lines.length == 4)
    var endAction = JsonUtils.fromJson[SingleAction](lines(3)).endStreamAction
    assert(endAction.nextPageToken != null)

    assertHttpError(
      url = requestPath("/shares/share2/schemas/default/tables/table2/query"),
      method = "POST",
      data = Some(s"""{"pageToken": "${endAction.nextPageToken}"}"""),
      expectedErrorCode = 400,
      expectedErrorMessage = "The table specified in the page token does not match the table being queried"
    )
    assertHttpError(
      url = requestPath("/shares/share1/schemas/default/tables/table1/query"),
      method = "POST",
      data = Some(s"""{"limitHint": 123, "pageToken": "${endAction.nextPageToken}"}"""),
      expectedErrorCode = 400,
      expectedErrorMessage = "Query parameter mismatch detected for the next page query"
    )

    // page token expired
    val updatedServerConfig = serverConfig.copy(queryTablePageTokenTtlMs = 0)
    server.stop().get()
    server = DeltaSharingService.start(updatedServerConfig)
    response = readNDJson(
      requestPath("/shares/share1/schemas/default/tables/table1/query"),
      Some("POST"),
      Some("""{"maxFiles": 1}"""),
      Some(2)
    )
    lines = response.split("\n")
    assert(lines.length == 4)
    endAction = JsonUtils.fromJson[SingleAction](lines(3)).endStreamAction
    assert(endAction.nextPageToken != null)

    assertHttpError(
      url = requestPath("/shares/share1/schemas/default/tables/table1/query"),
      method = "POST",
      data = Some(s"""{"pageToken": "${endAction.nextPageToken}"}"""),
      expectedErrorCode = 400,
      expectedErrorMessage = "The page token has expired"
    )

    server.stop().get()
    server = DeltaSharingService.start(serverConfig)
  }

  integrationTest("table3 - different data file schemas - /shares/{share}/schemas/{schema}/tables/{table}/metadata") {
    val response = readNDJson(requestPath("/shares/share1/schemas/default/tables/table3/metadata"), expectedTableVersion = Some(4))
    val Array(protocol, metadata) = response.split("\n")
    val expectedProtocol = Protocol(minReaderVersion = 1).wrap
    assert(expectedProtocol == JsonUtils.fromJson[SingleAction](protocol))
    val expectedMetadata = Metadata(
      id = "7ba6d727-a578-4234-a138-953f790b427c",
      format = Format(),
      schemaString = """{"type":"struct","fields":[{"name":"eventTime","type":"timestamp","nullable":true,"metadata":{}},{"name":"date","type":"date","nullable":true,"metadata":{}},{"name":"type","type":"string","nullable":true,"metadata":{}}]}""",
      partitionColumns = Seq("date")).wrap
    assert(expectedMetadata == JsonUtils.fromJson[SingleAction](metadata))
  }

  integrationTest("table3 - different data file schemas - /shares/{share}/schemas/{schema}/tables/{table}/query") {
    val response = readNDJson(requestPath("/shares/share1/schemas/default/tables/table3/query"), Some("POST"), Some("{}"), Some(4))
    val lines = response.split("\n")
    val protocol = lines(0)
    val metadata = lines(1)
    val expectedProtocol = Protocol(minReaderVersion = 1).wrap
    assert(expectedProtocol == JsonUtils.fromJson[SingleAction](protocol))
    val expectedMetadata = Metadata(
      id = "7ba6d727-a578-4234-a138-953f790b427c",
      format = Format(),
      schemaString = """{"type":"struct","fields":[{"name":"eventTime","type":"timestamp","nullable":true,"metadata":{}},{"name":"date","type":"date","nullable":true,"metadata":{}},{"name":"type","type":"string","nullable":true,"metadata":{}}]}""",
      partitionColumns = Seq("date")).wrap
    assert(expectedMetadata == JsonUtils.fromJson[SingleAction](metadata))
    val files = lines.drop(2)
    val actualFiles = files.map(f => JsonUtils.fromJson[SingleAction](f).file)
    assert(actualFiles.size == 3)
    val expectedFiles = Seq(
      AddFile(
        url = actualFiles(0).url,
        expirationTimestamp = actualFiles(0).expirationTimestamp,
        id = "db213271abffec6fd6c7fc2aad9d4b3f",
        partitionValues = Map("date" -> "2021-04-28"),
        size = 778,
        stats = """{"numRecords":1,"minValues":{"eventTime":"2021-04-28T23:36:51.945Z","type":"bar"},"maxValues":{"eventTime":"2021-04-28T23:36:51.945Z","type":"bar"},"nullCount":{"eventTime":0,"type":0}}"""
      ),
      AddFile(
        url = actualFiles(1).url,
        expirationTimestamp = actualFiles(1).expirationTimestamp,
        id = "f1f8be229d8b18eb6d6a34255f2d7089",
        partitionValues = Map("date" -> "2021-04-28"),
        size = 778,
        stats = """{"numRecords":1,"minValues":{"eventTime":"2021-04-28T23:36:47.599Z","type":"foo"},"maxValues":{"eventTime":"2021-04-28T23:36:47.599Z","type":"foo"},"nullCount":{"eventTime":0,"type":0}}"""
      ),
      AddFile(
        url = actualFiles(2).url,
        expirationTimestamp = actualFiles(2).expirationTimestamp,
        id = "a892a55d770ee70b34ffb2ebf7dc2fd0",
        partitionValues = Map("date" -> "2021-04-28"),
        size = 573,
        stats = """{"numRecords":1,"minValues":{"eventTime":"2021-04-28T23:35:53.156Z"},"maxValues":{"eventTime":"2021-04-28T23:35:53.156Z"},"nullCount":{"eventTime":0}}"""
      )
    )
    assert(actualFiles.count(_.expirationTimestamp != null) == 3)
    assert(expectedFiles == actualFiles.toList)
    verifyPreSignedUrl(actualFiles(0).url, 778)
    verifyPreSignedUrl(actualFiles(1).url, 778)
    verifyPreSignedUrl(actualFiles(2).url, 573)
  }

  integrationTest("case insensitive") {
    val response = readNDJson(requestPath("/shares/sHare1/schemas/deFault/tables/taBle3/metadata"), expectedTableVersion = Some(4))
    val Array(protocol, metadata) = response.split("\n")
    val expectedProtocol = Protocol(minReaderVersion = 1).wrap
    assert(expectedProtocol == JsonUtils.fromJson[SingleAction](protocol))
    val expectedMetadata = Metadata(
      id = "7ba6d727-a578-4234-a138-953f790b427c",
      format = Format(),
      schemaString = """{"type":"struct","fields":[{"name":"eventTime","type":"timestamp","nullable":true,"metadata":{}},{"name":"date","type":"date","nullable":true,"metadata":{}},{"name":"type","type":"string","nullable":true,"metadata":{}}]}""",
      partitionColumns = Seq("date")).wrap
    assert(expectedMetadata == JsonUtils.fromJson[SingleAction](metadata))
  }

  integrationTest("cdf_table_cdf_enabled - /shares/{share}/schemas/{schema}/tables/{table}/metadata") {
    val response = readNDJson(requestPath("/shares/share8/schemas/default/tables/cdf_table_cdf_enabled/metadata"), expectedTableVersion = Some(5))
    val Array(protocol, metadata) = response.split("\n")
    val expectedProtocol = Protocol(minReaderVersion = 1).wrap
    assert(expectedProtocol == JsonUtils.fromJson[SingleAction](protocol))
    val expectedMetadata = Metadata(
      id = "16736144-3306-4577-807a-d3f899b77670",
      format = Format(),
      schemaString = """{"type":"struct","fields":[{"name":"name","type":"string","nullable":true,"metadata":{}},{"name":"age","type":"integer","nullable":true,"metadata":{}},{"name":"birthday","type":"date","nullable":true,"metadata":{}}]}""",
      configuration = Map("enableChangeDataFeed" -> "true"),
      partitionColumns = Nil).wrap
    assert(expectedMetadata == JsonUtils.fromJson[SingleAction](metadata))
  }

  integrationTest("cdf_table_cdf_enabled - version 1 - /shares/{share}/schemas/{schema}/tables/{table}/query") {
    val p =
      """
        |{
        | "version": 1
        |}
        |""".stripMargin
    val response = readNDJson(requestPath("/shares/share8/schemas/default/tables/cdf_table_cdf_enabled/query"), Some("POST"), Some(p), Some(1))
    val lines = response.split("\n")
    val protocol = lines(0)
    val metadata = lines(1)
    val expectedProtocol = Protocol(minReaderVersion = 1).wrap
    assert(expectedProtocol == JsonUtils.fromJson[SingleAction](protocol))
    val expectedMetadata = Metadata(
      id = "16736144-3306-4577-807a-d3f899b77670",
      format = Format(),
      schemaString = """{"type":"struct","fields":[{"name":"name","type":"string","nullable":true,"metadata":{}},{"name":"age","type":"integer","nullable":true,"metadata":{}},{"name":"birthday","type":"date","nullable":true,"metadata":{}}]}""",
      configuration = Map("enableChangeDataFeed" -> "true"),
      partitionColumns = Nil).wrap
    assert(expectedMetadata == JsonUtils.fromJson[SingleAction](metadata))
    val files = lines.drop(2)
    val actualFiles = files.map(f => JsonUtils.fromJson[SingleAction](f).file)
    assert(actualFiles.size == 3)
    val expectedFiles = Seq(
      AddFile(
        url = actualFiles(0).url,
        expirationTimestamp = actualFiles(0).expirationTimestamp,
        id = "60d0cf57f3e4367db154aa2c36152a1f",
        partitionValues = Map.empty,
        size = 1030,
        stats = """{"numRecords":1,"minValues":{"name":"1","age":1,"birthday":"2020-01-01"},"maxValues":{"name":"1","age":1,"birthday":"2020-01-01"},"nullCount":{"name":0,"age":0,"birthday":0}}""",
        version = 1,
        timestamp = 1651272635000L
      ),
      AddFile(
        url = actualFiles(1).url,
        expirationTimestamp = actualFiles(1).expirationTimestamp,
        id = "d7ed708546dd70fdff9191b3e3d6448b",
        partitionValues = Map.empty,
        size = 1030,
        stats = """{"numRecords":1,"minValues":{"name":"3","age":3,"birthday":"2020-01-01"},"maxValues":{"name":"3","age":3,"birthday":"2020-01-01"},"nullCount":{"name":0,"age":0,"birthday":0}}""",
        version = 1,
        timestamp = 1651272635000L
      ),
      AddFile(
        url = actualFiles(2).url,
        expirationTimestamp = actualFiles(2).expirationTimestamp,
        id = "a6dc5694a4ebcc9a067b19c348526ad6",
        partitionValues = Map.empty,
        size = 1030,
        stats = """{"numRecords":1,"minValues":{"name":"2","age":2,"birthday":"2020-01-01"},"maxValues":{"name":"2","age":2,"birthday":"2020-01-01"},"nullCount":{"name":0,"age":0,"birthday":0}}""",
        version = 1,
        timestamp = 1651272635000L
      )
    )
    assert(actualFiles.count(_.expirationTimestamp != null) == 3)
    assert(expectedFiles == actualFiles.toList)
    verifyPreSignedUrl(actualFiles(0).url, 1030)
    verifyPreSignedUrl(actualFiles(1).url, 1030)
    verifyPreSignedUrl(actualFiles(2).url, 1030)
  }

  integrationTest("query table with version/timestamp/startingVersion - exceptions") {
    // only one of version/timestamp/startingVersion is supported
    assertHttpError(
      url = requestPath("/shares/share8/schemas/default/tables/cdf_table_cdf_enabled/query"),
      method = "POST",
      data = Some("""{"timestamp": "abc", "version": "3"}"""),
      expectedErrorCode = 400,
      expectedErrorMessage = ErrorStrings.multipleParametersSetErrorMsg(
        Seq("version", "timestamp", "startingVersion"))
    )
    assertHttpError(
      url = requestPath("/shares/share8/schemas/default/tables/cdf_table_cdf_enabled/query"),
      method = "POST",
      data = Some("""{"timestamp": "abc", "startingVersion": "3"}"""),
      expectedErrorCode = 400,
      expectedErrorMessage = ErrorStrings.multipleParametersSetErrorMsg(
        Seq("version", "timestamp", "startingVersion"))
    )
    assertHttpError(
      url = requestPath("/shares/share8/schemas/default/tables/cdf_table_cdf_enabled/query"),
      method = "POST",
      data = Some("""{"startingVersion": 2, "version": "3"}"""),
      expectedErrorCode = 400,
      expectedErrorMessage = ErrorStrings.multipleParametersSetErrorMsg(
        Seq("version", "timestamp", "startingVersion"))
    )
    assertHttpError(
      url = requestPath("/shares/share8/schemas/default/tables/cdf_table_cdf_enabled/query"),
      method = "POST",
      data = Some("""{"timestamp": "abc", "version": "3", "startingVersion": "2"}"""),
      expectedErrorCode = 400,
      expectedErrorMessage = ErrorStrings.multipleParametersSetErrorMsg(
        Seq("version", "timestamp", "startingVersion"))
    )

    // version/startVersion cannot be negative, and needs to be numeric
    assertHttpError(
      url = requestPath("/shares/share1/schemas/default/tables/table1/query"),
      method = "POST",
      data = Some("""
        {"version": -2}
      """),
      expectedErrorCode = 400,
      expectedErrorMessage = "table version cannot be negative"
    )
    assertHttpError(
      url = requestPath("/shares/share1/schemas/default/tables/table1/query"),
      method = "POST",
      data = Some("""
        {"startingVersion": -2}
      """),
      expectedErrorCode = 400,
      expectedErrorMessage = "startingVersion cannot be negative"
    )
    assertHttpError(
      url = requestPath("/shares/share1/schemas/default/tables/table1/query"),
      method = "POST",
      data = Some("""
        {"version": "x3"}
      """),
      expectedErrorCode = 400,
      expectedErrorMessage = "Not a numeric value: x3"
    )
    assertHttpError(
      url = requestPath("/shares/share1/schemas/default/tables/table1/query"),
      method = "POST",
      data = Some("""
        {"startingVersion": "x3"}
      """),
      expectedErrorCode = 400,
      expectedErrorMessage = "Not a numeric value: x3"
    )
    assertHttpError(
      url = requestPath("/shares/share8/schemas/default/tables/cdf_table_cdf_enabled/query"),
      method = "POST",
      data = Some("""
        {"startingVersion": "3", "endingVersion": "x3"}
      """),
      expectedErrorCode = 400,
      expectedErrorMessage = "Not a numeric value: x3"
    )
    assertHttpError(
      url = requestPath("/shares/share8/schemas/default/tables/cdf_table_cdf_enabled/query"),
      method = "POST",
      data = Some("""
        {"startingVersion": 3, "endingVersion": 2}
      """),
      expectedErrorCode = 400,
      expectedErrorMessage = "startingVersion(3) must be smaller than or equal to endingVersion(2)"
    )
    assertHttpError(
      url = requestPath("/shares/share8/schemas/default/tables/cdf_table_cdf_enabled/query"),
      method = "POST",
      data = Some("""
        {"startingVersion": 2, "endingVersion": 10}
      """),
      expectedErrorCode = 400,
      expectedErrorMessage = "End version cannot be greater than the latest version"
    )

    // timestamp before the earliest version
    assertHttpError(
      url = requestPath("/shares/share8/schemas/default/tables/cdf_table_cdf_enabled/query"),
      method = "POST",
      data = Some("""{"timestamp": "2000-01-01T00:00:00-08:00"}"""),
      expectedErrorCode = 400,
      expectedErrorMessage = "The provided timestamp "
    )

    // timestamp after the latest version
    assertHttpError(
      url = requestPath("/shares/share8/schemas/default/tables/cdf_table_cdf_enabled/query"),
      method = "POST",
      data = Some("""{"timestamp": "9999-01-01T00:00:00-08:00"}"""),
      expectedErrorCode = 400,
      expectedErrorMessage = "The provided timestamp "
    )

    // can only query table data since version 1
    // 1651614979 PST: 2022-05-03T14:56:19.000-08:00, version 1 is 1 second later
    val tsStr = new Timestamp(1651614979000L).toInstant.toString
    assertHttpError(
      url = requestPath("/shares/share8/schemas/default/tables/cdf_table_with_partition/query"),
      method = "POST",
      data = Some(s"""{"timestamp": "$tsStr"}"""),
      expectedErrorCode = 400,
      expectedErrorMessage = "You can only query table data since version 1"
    )
  }

  integrationTest("cdf_table_cdf_enabled - timestamp on version 1 - /shares/{share}/schemas/{schema}/tables/{table}/query") {
    // 1651272635000, PST: 2022-04-29 15:50:35.0 -> version 1
    // endingVersion is ignored
    val tsStr = new Timestamp(1651272635000L).toInstant.toString
    val p =
      s"""
         |{
         | "timestamp": "$tsStr",
         | "endingVersion": 2
         |}
         |""".stripMargin
    val response = readNDJson(requestPath("/shares/share8/schemas/default/tables/cdf_table_cdf_enabled/query"), Some("POST"), Some(p), Some(1))
    val lines = response.split("\n")
    val protocol = lines(0)
    val metadata = lines(1)
    val expectedProtocol = Protocol(minReaderVersion = 1).wrap
    assert(expectedProtocol == JsonUtils.fromJson[SingleAction](protocol))
    val expectedMetadata = Metadata(
      id = "16736144-3306-4577-807a-d3f899b77670",
      format = Format(),
      schemaString = """{"type":"struct","fields":[{"name":"name","type":"string","nullable":true,"metadata":{}},{"name":"age","type":"integer","nullable":true,"metadata":{}},{"name":"birthday","type":"date","nullable":true,"metadata":{}}]}""",
      configuration = Map("enableChangeDataFeed" -> "true"),
      partitionColumns = Nil).wrap
    assert(expectedMetadata == JsonUtils.fromJson[SingleAction](metadata))
    val files = lines.drop(2)
    val actualFiles = files.map(f => JsonUtils.fromJson[SingleAction](f).file)
    assert(actualFiles.size == 3)
    val expectedFiles = Seq(
      AddFile(
        url = actualFiles(0).url,
        expirationTimestamp = actualFiles(0).expirationTimestamp,
        id = "60d0cf57f3e4367db154aa2c36152a1f",
        partitionValues = Map.empty,
        size = 1030,
        stats = """{"numRecords":1,"minValues":{"name":"1","age":1,"birthday":"2020-01-01"},"maxValues":{"name":"1","age":1,"birthday":"2020-01-01"},"nullCount":{"name":0,"age":0,"birthday":0}}""",
        version = 1,
        timestamp = 1651272635000L
      ),
      AddFile(
        url = actualFiles(1).url,
        expirationTimestamp = actualFiles(1).expirationTimestamp,
        id = "d7ed708546dd70fdff9191b3e3d6448b",
        partitionValues = Map.empty,
        size = 1030,
        stats = """{"numRecords":1,"minValues":{"name":"3","age":3,"birthday":"2020-01-01"},"maxValues":{"name":"3","age":3,"birthday":"2020-01-01"},"nullCount":{"name":0,"age":0,"birthday":0}}""",
        version = 1,
        timestamp = 1651272635000L
      ),
      AddFile(
        url = actualFiles(2).url,
        expirationTimestamp = actualFiles(2).expirationTimestamp,
        id = "a6dc5694a4ebcc9a067b19c348526ad6",
        partitionValues = Map.empty,
        size = 1030,
        stats = """{"numRecords":1,"minValues":{"name":"2","age":2,"birthday":"2020-01-01"},"maxValues":{"name":"2","age":2,"birthday":"2020-01-01"},"nullCount":{"name":0,"age":0,"birthday":0}}""",
        version = 1,
        timestamp = 1651272635000L
      )
    )
    assert(actualFiles.count(_.expirationTimestamp != null) == 3)
    assert(expectedFiles == actualFiles.toList)
    verifyPreSignedUrl(actualFiles(0).url, 1030)
    verifyPreSignedUrl(actualFiles(1).url, 1030)
    verifyPreSignedUrl(actualFiles(2).url, 1030)
  }

  integrationTest("streaming_table_with_optimize - startingVersion success") {
    Seq(RESPONSE_FORMAT_PARQUET, RESPONSE_FORMAT_DELTA).foreach { responseFormat =>
      val p =
        s"""
           |{
           | "startingVersion": 0
           |}
           |""".stripMargin
      val response = readNDJson(requestPath("/shares/share8/schemas/default/tables/streaming_table_with_optimize/query"), Some("POST"), Some(p), Some(0), responseFormat)
      val lines = response.split("\n")
      val protocol = lines(0)
      val metadata = lines(1)
      if (responseFormat == RESPONSE_FORMAT_DELTA) {
        val responseProtocol = JsonUtils.fromJson[DeltaResponseSingleAction](protocol).protocol
        assert(responseProtocol.deltaProtocol.minReaderVersion == 1)

        // unable to construct the delta action because the cases classes like AddFile/Metadata
        // are private to io.delta.standalone.internal.
        // So we only verify a couple important fields.
        val responseMetadata = JsonUtils.fromJson[DeltaResponseSingleAction](metadata).metaData
        assert(responseMetadata.deltaMetadata.id == "4929d09e-b085-4d22-a95e-7416fb2f78ab")
        assert(responseMetadata.version == 0)
      } else {
        val expectedProtocol = Protocol(minReaderVersion = 1).wrap
        assert(expectedProtocol == JsonUtils.fromJson[SingleAction](protocol))
        val expectedMetadata = Metadata(
          id = "4929d09e-b085-4d22-a95e-7416fb2f78ab",
          format = Format(),
          schemaString = """{"type":"struct","fields":[{"name":"name","type":"string","nullable":true,"metadata":{}},{"name":"age","type":"integer","nullable":true,"metadata":{}},{"name":"birthday","type":"date","nullable":true,"metadata":{}}]}""",
          configuration = Map("enableChangeDataFeed" -> "true"),
          partitionColumns = Nil,
          version = 0).wrap
        assert(expectedMetadata == JsonUtils.fromJson[SingleAction](metadata))
      }
      val files = lines.drop(2)
      assert(files.size == 7)
      // version 1: INSERT
      // version 2: INSERT
      // version 3: INSERT
      // version 4: OPTIMIZE
      // version 5: REMOVE
      // version 6: REMOVE
      verifyAddFile(
        files(0),
        size = 1030,
        stats =
          """{"numRecords":1,"minValues":{"name":"1","age":1,"birthday":"2020-01-01"},"maxValues":{"name":"1","age":1,"birthday":"2020-01-01"},"nullCount":{"name":0,"age":0,"birthday":0}}""",
        partitionValues = Map.empty,
        version = 1,
        timestamp = 1664325366000L,
        responseFormat
      )
      verifyAddFile(
        files(1),
        size = 1030,
        stats =
          """{"numRecords":1,"minValues":{"name":"2","age":2,"birthday":"2020-01-01"},"maxValues":{"name":"2","age":2,"birthday":"2020-01-01"},"nullCount":{"name":0,"age":0,"birthday":0}}""",
        partitionValues = Map.empty,
        version = 2,
        timestamp = 1664325372000L,
        responseFormat
      )
      verifyAddFile(
        files(2),
        size = 1030,
        stats =
          """{"numRecords":1,"minValues":{"name":"3","age":3,"birthday":"2020-01-01"},"maxValues":{"name":"3","age":3,"birthday":"2020-01-01"},"nullCount":{"name":0,"age":0,"birthday":0}}""",
        partitionValues = Map.empty,
        version = 3,
        timestamp = 1664325375000L,
        responseFormat
      )
      verifyRemove(
        files(3),
        size = 1075,
        partitionValues = Map.empty,
        version = 5,
        timestamp = 1664325546000L,
        responseFormat
      )
      verifyAddFile(
        files(4),
        size = 1283,
        stats =
          """{"numRecords":2,"minValues":{"name":"2","age":2,"birthday":"2020-01-01"},"maxValues":{"name":"3","age":3,"birthday":"2020-01-01"},"nullCount":{"name":0,"age":0,"birthday":0,"_change_type":2}}""",
        partitionValues = Map.empty,
        version = 5,
        timestamp = 1664325546000L,
        responseFormat
      )
      verifyRemove(
        files(5),
        size = 1283,
        partitionValues = Map.empty,
        version = 6,
        timestamp = 1664325549000L,
        responseFormat
      )
      verifyAddFile(
        files(6),
        size = 1247,
        stats =
          """{"numRecords":1,"minValues":{"name":"3","age":3,"birthday":"2020-01-01"},"maxValues":{"name":"3","age":3,"birthday":"2020-01-01"},"nullCount":{"name":0,"age":0,"birthday":0,"_change_type":1}}""",
        partitionValues = Map.empty,
        version = 6,
        timestamp = 1664325549000L,
        responseFormat
      )
    }
  }

  integrationTest("streaming_table_with_optimize - paginated query with startingVersion") {
    Seq(RESPONSE_FORMAT_PARQUET, RESPONSE_FORMAT_DELTA).foreach { responseFormat =>
      // version 6: 1 REMOVE + 1 ADD
      var response = readNDJson(
        requestPath("/shares/share8/schemas/default/tables/streaming_table_with_optimize/query"),
        Some("POST"),
        Some("""{"startingVersion": 6, "maxFiles": 1}"""),
        Some(6),
        responseFormat
      )
      var lines = response.split("\n")
      assert(lines.length == 4)
      val protocol = lines(0)
      val metadata = lines(1)
      val files = ArrayBuffer[String]()
      files.append(lines(2))
      var endAction = JsonUtils.fromJson[SingleAction](lines(3)).endStreamAction
      assert(endAction.minUrlExpirationTimestamp != null)
      var numPages = 1
      while (endAction.nextPageToken != null) {
        numPages += 1
        response = readNDJson(
          requestPath("/shares/share8/schemas/default/tables/streaming_table_with_optimize/query"),
          Some("POST"),
          Some(s"""{"startingVersion": 6, "maxFiles": 1, "pageToken": "${endAction.nextPageToken}"}"""),
          Some(6),
          responseFormat
        )
        lines = response.split("\n")
        assert(lines.length == 4)
        assert(protocol == lines(0))
        assert(metadata == lines(1))
        files.append(lines(2))
        endAction = JsonUtils.fromJson[SingleAction](lines(3)).endStreamAction
        assert(endAction.minUrlExpirationTimestamp != null)
      }
      assert(numPages == 2)

      if (responseFormat == RESPONSE_FORMAT_DELTA) {
        val responseProtocol = JsonUtils.fromJson[DeltaResponseSingleAction](protocol).protocol
        assert(responseProtocol.deltaProtocol.minReaderVersion == 1)

        // unable to construct the delta action because the cases classes like AddFile/Metadata
        // are private to io.delta.standalone.internal.
        // So we only verify a couple important fields.
        val responseMetadata = JsonUtils.fromJson[DeltaResponseSingleAction](metadata).metaData
        assert(responseMetadata.deltaMetadata.id == "4929d09e-b085-4d22-a95e-7416fb2f78ab")
        assert(responseMetadata.version == 6)
      } else {
        val expectedProtocol = Protocol(minReaderVersion = 1).wrap
        assert(expectedProtocol == JsonUtils.fromJson[SingleAction](protocol))
        val expectedMetadata = Metadata(
          id = "4929d09e-b085-4d22-a95e-7416fb2f78ab",
          format = Format(),
          schemaString =
            """{"type":"struct","fields":[{"name":"name","type":"string","nullable":true,"metadata":{}},{"name":"age","type":"integer","nullable":true,"metadata":{}},{"name":"birthday","type":"date","nullable":true,"metadata":{}}]}""",
          configuration = Map("enableChangeDataFeed" -> "true"),
          partitionColumns = Nil,
          version = 6
        ).wrap
        assert(expectedMetadata == JsonUtils.fromJson[SingleAction](metadata))
      }
      assert(files.size == 2)
      verifyRemove(
        files(0),
        size = 1283,
        partitionValues = Map.empty,
        version = 6,
        timestamp = 1664325549000L,
        responseFormat
      )
      verifyAddFile(
        files(1),
        size = 1247,
        stats =
          """{"numRecords":1,"minValues":{"name":"3","age":3,"birthday":"2020-01-01"},"maxValues":{"name":"3","age":3,"birthday":"2020-01-01"},"nullCount":{"name":0,"age":0,"birthday":0,"_change_type":1}}""",
        partitionValues = Map.empty,
        version = 6,
        timestamp = 1664325549000L,
        responseFormat
      )
    }
  }

  integrationTest("streaming_table_with_optimize - paginated query with startingVersion and endingVersion") {
    // version 2: Add
    // version 3: Add
    var response = readNDJson(
      requestPath("/shares/share8/schemas/default/tables/streaming_table_with_optimize/query"),
      Some("POST"),
      Some("""{"startingVersion": 2, "endingVersion": 3, "maxFiles": 1}"""),
      Some(2)
    )
    var lines = response.split("\n")
    assert(lines.length == 4)
    val protocol = lines(0)
    val metadata = lines(1)
    val files = ArrayBuffer[String]()
    files.append(lines(2))
    var endAction = JsonUtils.fromJson[SingleAction](lines(3)).endStreamAction
    assert(endAction.minUrlExpirationTimestamp != null)
    var numPages = 1
    while (endAction.nextPageToken != null) {
      numPages += 1
      response = readNDJson(
        requestPath("/shares/share8/schemas/default/tables/streaming_table_with_optimize/query"),
        Some("POST"),
        Some(s"""{"startingVersion": 2, "endingVersion": 3, "maxFiles": 1, "pageToken": "${endAction.nextPageToken}"}"""),
        Some(2)
      )
      lines = response.split("\n")
      assert(lines.length == 4)
      assert(protocol == lines(0))
      assert(metadata == lines(1))
      files.append(lines(2))
      endAction = JsonUtils.fromJson[SingleAction](lines(3)).endStreamAction
      assert(endAction.minUrlExpirationTimestamp != null)
    }
    assert(numPages == 2)

    val expectedProtocol = Protocol(minReaderVersion = 1).wrap
    assert(expectedProtocol == JsonUtils.fromJson[SingleAction](protocol))
    val expectedMetadata = Metadata(
      id = "4929d09e-b085-4d22-a95e-7416fb2f78ab",
      format = Format(),
      schemaString =
        """{"type":"struct","fields":[{"name":"name","type":"string","nullable":true,"metadata":{}},{"name":"age","type":"integer","nullable":true,"metadata":{}},{"name":"birthday","type":"date","nullable":true,"metadata":{}}]}""",
      configuration = Map("enableChangeDataFeed" -> "true"),
      partitionColumns = Nil,
      version = 2
    ).wrap
    assert(expectedMetadata == JsonUtils.fromJson[SingleAction](metadata))
    assert(files.length == 2)
    verifyAddFile(
      files(0),
      size = 1030,
      stats =
        """{"numRecords":1,"minValues":{"name":"2","age":2,"birthday":"2020-01-01"},"maxValues":{"name":"2","age":2,"birthday":"2020-01-01"},"nullCount":{"name":0,"age":0,"birthday":0}}""",
      partitionValues = Map.empty,
      version = 2,
      timestamp = 1664325372000L
    )
    verifyAddFile(
      files(1),
      size = 1030,
      stats =
        """{"numRecords":1,"minValues":{"name":"3","age":3,"birthday":"2020-01-01"},"maxValues":{"name":"3","age":3,"birthday":"2020-01-01"},"nullCount":{"name":0,"age":0,"birthday":0}}""",
      partitionValues = Map.empty,
      version = 3,
      timestamp = 1664325375000L
    )
  }

  integrationTest("streaming_table_metadata_protocol - startingVersion 0 success") {
    val p =
      s"""
         |{
         | "startingVersion": 0
         |}
         |""".stripMargin
    val response = readNDJson(requestPath("/shares/share8/schemas/default/tables/streaming_table_metadata_protocol/query"), Some("POST"), Some(p), Some(0))
    val actions = response.split("\n").map(JsonUtils.fromJson[SingleAction](_))
    assert(actions.size == 6)

    // version 0: CREATE TABLE, protocol/metadata
    // version 1: INSERT
    // version 2: ALTER TABLE, metadata
    // version 3: ALTER TABLE, metadata
    // version 4: INSERT
    val expectedProtocol = Protocol(minReaderVersion = 1)
    assert(expectedProtocol == actions(0).protocol)
    var expectedMetadata = Metadata(
      id = "eaca659e-28ac-4c68-8c72-0c96205c8160",
      format = Format(),
      schemaString = """{"type":"struct","fields":[{"name":"name","type":"string","nullable":true,"metadata":{}},{"name":"age","type":"integer","nullable":true,"metadata":{}},{"name":"birthday","type":"date","nullable":true,"metadata":{}}]}""",
      partitionColumns = Nil,
      version = 0)
    assert(expectedMetadata == actions(1).metaData)

    assert(actions(2).add != null)

    // Check metadata for version 2.
    expectedMetadata = expectedMetadata.copy(
      configuration = Map("enableChangeDataFeed" -> "true"),
      version = 2)
    assert(expectedMetadata == actions(3).metaData)

    // Check metadata for version 3.
    expectedMetadata = expectedMetadata.copy(
      configuration = Map.empty,
      version = 3)
    assert(expectedMetadata == actions(4).metaData)

    assert(actions(5).add != null)
  }

  integrationTest("streaming_table_metadata_protocol - paginated query") {
    // version 0: CREATE TABLE, protocol/metadata
    // version 1: INSERT
    // version 2: ALTER TABLE, metadata
    // version 3: ALTER TABLE, metadata
    // version 4: INSERT
    val expectedProtocol = Protocol(minReaderVersion = 1)
    val expectedMetadata = Metadata(
      id = "eaca659e-28ac-4c68-8c72-0c96205c8160",
      format = Format(),
      schemaString =
        """{"type":"struct","fields":[{"name":"name","type":"string","nullable":true,"metadata":{}},{"name":"age","type":"integer","nullable":true,"metadata":{}},{"name":"birthday","type":"date","nullable":true,"metadata":{}}]}""",
      partitionColumns = Nil,
      version = 0
    )

    // Page 1
    var response = readNDJson(
      requestPath("/shares/share8/schemas/default/tables/streaming_table_metadata_protocol/query"),
      Some("POST"),
      Some("""{"startingVersion": 0, "maxFiles": 1}"""),
      Some(0)
    )
    var actions = response.split("\n").map(JsonUtils.fromJson[SingleAction](_))
    assert(actions.length == 6)
    assert(expectedProtocol == actions(0).protocol)
    assert(expectedMetadata == actions(1).metaData)
    assert(actions(2).add != null)
    // Check metadata for version 2.
    assert(expectedMetadata.copy(configuration = Map("enableChangeDataFeed" -> "true"), version = 2) == actions(3).metaData)
    // Check metadata for version 3.
    assert(expectedMetadata.copy(configuration = Map.empty, version = 3) == actions(4).metaData)
    var endAction = actions(5).endStreamAction
    assert(endAction.nextPageToken != null)
    assert(endAction.minUrlExpirationTimestamp == actions(2).add.expirationTimestamp)

    // Page 2
    response = readNDJson(
      requestPath("/shares/share8/schemas/default/tables/streaming_table_metadata_protocol/query"),
      Some("POST"),
      Some(s"""{"startingVersion": 0, "maxFiles": 1, "pageToken": "${endAction.nextPageToken}"}"""),
      Some(0)
    )
    actions = response.split("\n").map(JsonUtils.fromJson[SingleAction](_))
    assert(actions.length == 4)
    assert(expectedProtocol == actions(0).protocol)
    assert(expectedMetadata == actions(1).metaData)
    assert(actions(2).add != null)
    // Check this is the last page (token is empty)
    endAction = actions(3).endStreamAction
    assert(endAction.nextPageToken == null)
    assert(endAction.minUrlExpirationTimestamp == actions(2).add.expirationTimestamp)
  }

  integrationTest("streaming_table_metadata_protocol - startingVersion 2 success") {
    val p =
      s"""
         |{
         | "startingVersion": 2
         |}
         |""".stripMargin
    val response = readNDJson(requestPath("/shares/share8/schemas/default/tables/streaming_table_metadata_protocol/query"), Some("POST"), Some(p), Some(2))
    val actions = response.split("\n").map(JsonUtils.fromJson[SingleAction](_))
    assert(actions.size == 4)

    // version 2: ALTER TABLE, protocol/metadata
    // version 3: ALTER TABLE, metadata
    // version 4: INSERT

    // Check metadata for version 2.
    val expectedProtocol = Protocol(minReaderVersion = 1)
    assert(expectedProtocol == actions(0).protocol)
    var expectedMetadata = Metadata(
      id = "eaca659e-28ac-4c68-8c72-0c96205c8160",
      format = Format(),
      schemaString = """{"type":"struct","fields":[{"name":"name","type":"string","nullable":true,"metadata":{}},{"name":"age","type":"integer","nullable":true,"metadata":{}},{"name":"birthday","type":"date","nullable":true,"metadata":{}}]}""",
      partitionColumns = Nil,
      configuration = Map("enableChangeDataFeed" -> "true"),
      version = 2)
    assert(expectedMetadata == actions(1).metaData)

    // Check metadata for version 3.
    expectedMetadata = expectedMetadata.copy(
      configuration = Map.empty,
      version = 3)
    assert(expectedMetadata == actions(2).metaData)

    assert(actions(3).add != null)
  }

  integrationTest("streaming_table_metadata_protocol - startingVersion with endingVersion success") {
    val p =
      s"""
         |{
         | "startingVersion": 0,
         | "endingVersion": 2
         |}
         |""".stripMargin
    val response = readNDJson(requestPath("/shares/share8/schemas/default/tables/streaming_table_metadata_protocol/query"), Some("POST"), Some(p), Some(0))
    val actions = response.split("\n").map(JsonUtils.fromJson[SingleAction](_))
    assert(actions.size == 4)

    // version 0: CREATE TABLE, protocol/metadata
    // version 1: INSERT
    // version 2: ALTER TABLE, metadata
    val expectedProtocol = Protocol(minReaderVersion = 1)
    assert(expectedProtocol == actions(0).protocol)
    var expectedMetadata = Metadata(
      id = "eaca659e-28ac-4c68-8c72-0c96205c8160",
      format = Format(),
      schemaString = """{"type":"struct","fields":[{"name":"name","type":"string","nullable":true,"metadata":{}},{"name":"age","type":"integer","nullable":true,"metadata":{}},{"name":"birthday","type":"date","nullable":true,"metadata":{}}]}""",
      partitionColumns = Nil,
      version = 0)
    assert(expectedMetadata == actions(1).metaData)

    assert(actions(2).add != null)

    // Check metadata for version 2.
    expectedMetadata = expectedMetadata.copy(
      configuration = Map("enableChangeDataFeed" -> "true"),
      version = 2)
    assert(expectedMetadata == actions(3).metaData)
  }

  integrationTest("streaming_table_metadata_protocol - startingVersion equal endingVersion success 1") {
    val p =
      s"""
         |{
         | "startingVersion": 1,
         | "endingVersion": 1
         |}
         |""".stripMargin
    val response = readNDJson(requestPath("/shares/share8/schemas/default/tables/streaming_table_metadata_protocol/query"), Some("POST"), Some(p), Some(1))
    val actions = response.split("\n").map(JsonUtils.fromJson[SingleAction](_))
    assert(actions.size == 3)

    // version 2: ALTER TABLE, metadata
    // Check metadata for version 2.
    val expectedProtocol = Protocol(minReaderVersion = 1)
    assert(expectedProtocol == actions(0).protocol)
    var expectedMetadata = Metadata(
      id = "eaca659e-28ac-4c68-8c72-0c96205c8160",
      format = Format(),
      schemaString = """{"type":"struct","fields":[{"name":"name","type":"string","nullable":true,"metadata":{}},{"name":"age","type":"integer","nullable":true,"metadata":{}},{"name":"birthday","type":"date","nullable":true,"metadata":{}}]}""",
      partitionColumns = Nil,
      version = 1)
    assert(expectedMetadata == actions(1).metaData)

    assert(actions(2).add != null)
    assert(actions(2).add.version == 1)
  }

  integrationTest("streaming_table_metadata_protocol - startingVersion equal endingVersion success 2") {
    val p =
      s"""
         |{
         | "startingVersion": 2,
         | "endingVersion": 2
         |}
         |""".stripMargin
    val response = readNDJson(requestPath("/shares/share8/schemas/default/tables/streaming_table_metadata_protocol/query"), Some("POST"), Some(p), Some(2))
    val actions = response.split("\n").map(JsonUtils.fromJson[SingleAction](_))
    assert(actions.size == 2)

    // version 2: ALTER TABLE, metadata
    // Check metadata for version 2.
    val expectedProtocol = Protocol(minReaderVersion = 1)
    assert(expectedProtocol == actions(0).protocol)
    var expectedMetadata = Metadata(
      id = "eaca659e-28ac-4c68-8c72-0c96205c8160",
      format = Format(),
      schemaString = """{"type":"struct","fields":[{"name":"name","type":"string","nullable":true,"metadata":{}},{"name":"age","type":"integer","nullable":true,"metadata":{}},{"name":"birthday","type":"date","nullable":true,"metadata":{}}]}""",
      partitionColumns = Nil,
      configuration = Map("enableChangeDataFeed" -> "true"),
      version = 2)
    assert(expectedMetadata == actions(1).metaData)
  }

  object StreamingNotnullToNull {
    val metadataV0 = Metadata(
      id = "1e2201ff-12ad-4c3b-a539-4d34e9e36680",
      format = Format(),
      schemaString = """{"type":"struct","fields":[{"name":"name","type":"string","nullable":false,"metadata":{}}]}""",
      configuration = Map("enableChangeDataFeed" -> "true"),
      partitionColumns = Nil
    )
    val metadataV2 = metadataV0.copy(
      schemaString = """{"type":"struct","fields":[{"name":"name","type":"string","nullable":true,"metadata":{}}]}"""
    )
  }

  def getMetadataResponse(version: Long): String = {
    readNDJson(
      requestPath(s"/shares/share8/schemas/default/tables/streaming_notnull_to_null/metadata?version=$version"),
      expectedTableVersion = Some(version)
    )
  }

  def verifyMetadata(response: String, expectedMetadata: Metadata): Unit = {
    val Array(protocol, metadata) = response.split("\n")
    val expectedProtocol = Protocol(minReaderVersion = 1)
    assert(expectedProtocol.wrap == JsonUtils.fromJson[SingleAction](protocol))
    assert(expectedMetadata.wrap == JsonUtils.fromJson[SingleAction](metadata))
  }

  integrationTest("streaming_notnull_to_null - metadata of different versions") {
    verifyMetadata(getMetadataResponse(0), StreamingNotnullToNull.metadataV0)
    verifyMetadata(getMetadataResponse(1), StreamingNotnullToNull.metadataV0)
    verifyMetadata(getMetadataResponse(2), StreamingNotnullToNull.metadataV2)
    verifyMetadata(getMetadataResponse(3), StreamingNotnullToNull.metadataV2)
    assertHttpError(
      url = requestPath(s"/shares/share8/schemas/default/tables/streaming_notnull_to_null/metadata?version=4"),
      method = "GET",
      data = None,
      expectedErrorCode = 400,
      expectedErrorMessage = "Cannot time travel Delta table to version 4"
    )
  }

  def getMetadataResponse(timestamp: String, version: Long): String = {
    readNDJson(
      requestPath(s"/shares/share8/schemas/default/tables/streaming_notnull_to_null/metadata?timestamp=$timestamp"),
      expectedTableVersion = Some(version)
    )
  }

  integrationTest("streaming_notnull_to_null - metadata of different timestamp") {
    verifyMetadata(
      getMetadataResponse("2022-11-13T08:10:41Z", 0), StreamingNotnullToNull.metadataV0
    )
    verifyMetadata(
      getMetadataResponse("2022-11-13T08:10:46Z", 1), StreamingNotnullToNull.metadataV0
    )
    verifyMetadata(
      getMetadataResponse("2022-11-13T08:10:48Z", 2), StreamingNotnullToNull.metadataV2
    )
    verifyMetadata(
      getMetadataResponse("2022-11-13T08:10:50Z", 3), StreamingNotnullToNull.metadataV2
    )
    assertHttpError(
      url = requestPath(s"/shares/share8/schemas/default/tables/streaming_notnull_to_null/metadata?timestamp=2021-01-01"),
      method = "GET",
      data = None,
      expectedErrorCode = 400,
      expectedErrorMessage = "Invalid timestamp"
    )
    assertHttpError(
      url = requestPath(s"/shares/share8/schemas/default/tables/streaming_notnull_to_null/metadata?timestamp=2021-01-01T00:00:00Z"),
      method = "GET",
      data = None,
      expectedErrorCode = 400,
      expectedErrorMessage = "is before the earliest version available"
    )
    assertHttpError(
      url = requestPath(s"/shares/share8/schemas/default/tables/streaming_notnull_to_null/metadata?timestamp=2024-01-01T00:00:00Z"),
      method = "GET",
      data = None,
      expectedErrorCode = 400,
      expectedErrorMessage = "is after the latest version available"
    )
    assertHttpError(
      url = requestPath(s"/shares/share8/schemas/default/tables/streaming_notnull_to_null/metadata?timestamp=2024-01-01T00:00:00Z"),
      method = "GET",
      data = None,
      expectedErrorCode = 400,
      expectedErrorMessage = "is after the latest version available"
    )
  }

  integrationTest("getMetadata - exceptions with parameters") {
    assertHttpError(
      url = requestPath(s"/shares/share8/schemas/default/tables/streaming_notnull_to_null/metadata?version=-1"),
      method = "GET",
      data = None,
      expectedErrorCode = 400,
      expectedErrorMessage = "version cannot be negative"
    )
    assertHttpError(
      url = requestPath(s"/shares/share8/schemas/default/tables/streaming_notnull_to_null/metadata?version=-1&timestamp=abc"),
      method = "GET",
      data = None,
      expectedErrorCode = 400,
      expectedErrorMessage = "Please only provide one of"
    )
  }

  integrationTest("streaming_notnull_to_null - no exceptions") {
    // Changing a column from not null to null
    val p =
      s"""
         |{
         | "startingVersion": 0
         |}
         |""".stripMargin
    val response = readNDJson(requestPath("/shares/share8/schemas/default/tables/streaming_notnull_to_null/query"), Some("POST"), Some(p), Some(0))
    val actions = response.split("\n").map(JsonUtils.fromJson[SingleAction](_))
    assert(actions.size == 5)

    val expectedProtocol = Protocol(minReaderVersion = 1)
    assert(expectedProtocol == actions(0).protocol)
    var expectedMetadata = Metadata(
      id = "1e2201ff-12ad-4c3b-a539-4d34e9e36680",
      format = Format(),
      schemaString = """{"type":"struct","fields":[{"name":"name","type":"string","nullable":false,"metadata":{}}]}""",
      configuration = Map("enableChangeDataFeed" -> "true"),
      partitionColumns = Nil,
      version = 0)
    assert(expectedMetadata == actions(1).metaData)

    assert(actions(2).add != null)

    expectedMetadata = expectedMetadata.copy(
      schemaString = """{"type":"struct","fields":[{"name":"name","type":"string","nullable":true,"metadata":{}}]}""",
      version = 2
    )
    assert(expectedMetadata == actions(3).metaData)

    assert(actions(4).add != null)
  }

  integrationTest("streaming_null_to_notnull - no exceptions") {
    // Changing a column from null to not null
    val p =
      s"""
         |{
         | "startingVersion": 0
         |}
         |""".stripMargin
    val response = readNDJson(requestPath("/shares/share8/schemas/default/tables/streaming_null_to_notnull/query"), Some("POST"), Some(p), Some(0))
    val actions = response.split("\n").map(JsonUtils.fromJson[SingleAction](_))
    assert(actions.size == 7)

    val expectedProtocol = Protocol(minReaderVersion = 1)
    assert(expectedProtocol == actions(0).protocol)
    var expectedMetadata = Metadata(
      id = "f49d6102-3fca-4452-ab83-6e71fecfb118",
      format = Format(),
      schemaString = """{"type":"struct","fields":[{"name":"name","type":"string","nullable":true,"metadata":{}}]}""",
      configuration = Map.empty,
      partitionColumns = Nil,
      version = 0)
    assert(expectedMetadata == actions(1).metaData)

    assert(actions(2).add != null)
    assert(actions(3).remove != null)
    assert(actions(4).add != null)

    expectedMetadata = expectedMetadata.copy(
      schemaString = """{"type":"struct","fields":[{"name":"name","type":"string","nullable":false,"metadata":{}}]}""",
      version = 3
    )
    assert(expectedMetadata == actions(5).metaData)

    assert(actions(6).add != null)
  }

  integrationTest("table_reader_version_increased - exception") {
    assertHttpError(
      url = requestPath("/shares/share8/schemas/default/tables/table_reader_version_increased/query"),
      method = "POST",
      data = Some("""{"startingVersion": 0}"""),
      expectedErrorCode = 400,
      expectedErrorMessage = "Delta protocol version (2,2) is too new for this version of Delta"
    )
  }

  integrationTest("cdf_table_cdf_enabled_changes - query table changes") {
    Seq(
      RESPONSE_FORMAT_PARQUET,
      RESPONSE_FORMAT_DELTA,
      s"$RESPONSE_FORMAT_DELTA,$RESPONSE_FORMAT_PARQUET",
      s"$RESPONSE_FORMAT_PARQUET,$RESPONSE_FORMAT_DELTA"
    ).foreach { responseFormat =>
      val respondedFormat = if (responseFormat == RESPONSE_FORMAT_DELTA) {
        RESPONSE_FORMAT_DELTA
      } else {
        RESPONSE_FORMAT_PARQUET
      }
      val response = readNDJson(requestPath(s"/shares/share8/schemas/default/tables/cdf_table_cdf_enabled/changes?startingVersion=0&endingVersion=3"), Some("GET"), None, Some(0), respondedFormat)
      val lines = response.split("\n")
      val protocol = lines(0)
      val metadata = lines(1)
      if (responseFormat == RESPONSE_FORMAT_DELTA) {
        val responseProtocol = JsonUtils.fromJson[DeltaResponseSingleAction](protocol).protocol
        assert(responseProtocol.deltaProtocol.minReaderVersion == 1)

        // unable to construct the delta action because the cases classes like AddFile/Metadata
        // are private to io.delta.standalone.internal.
        // So we only verify a couple important fields.
        val responseMetadata = JsonUtils.fromJson[DeltaResponseSingleAction](metadata).metaData
        assert(responseMetadata.deltaMetadata.id == "16736144-3306-4577-807a-d3f899b77670")
        assert(responseMetadata.version == 5)
      } else {
        val expectedProtocol = Protocol(minReaderVersion = 1).wrap
        assert(expectedProtocol == JsonUtils.fromJson[SingleAction](protocol))
        val expectedMetadata = Metadata(
          id = "16736144-3306-4577-807a-d3f899b77670",
          format = Format(),
          schemaString = """{"type":"struct","fields":[{"name":"name","type":"string","nullable":true,"metadata":{}},{"name":"age","type":"integer","nullable":true,"metadata":{}},{"name":"birthday","type":"date","nullable":true,"metadata":{}}]}""",
          configuration = Map("enableChangeDataFeed" -> "true"),
          partitionColumns = Nil,
          version = 5).wrap
        assert(expectedMetadata == JsonUtils.fromJson[SingleAction](metadata))
      }
      val files = lines.drop(2)
      assert(files.size == 5)
      verifyAddFile(
        files(0),
        size = 1030,
        stats =
          """{"numRecords":1,"minValues":{"name":"1","age":1,"birthday":"2020-01-01"},"maxValues":{"name":"1","age":1,"birthday":"2020-01-01"},"nullCount":{"name":0,"age":0,"birthday":0}}""",
        partitionValues = Map.empty,
        version = 1,
        timestamp = 1651272635000L,
        responseFormat
      )
      verifyAddFile(
        files(1),
        size = 1030,
        stats =
          """{"numRecords":1,"minValues":{"name":"2","age":2,"birthday":"2020-01-01"},"maxValues":{"name":"2","age":2,"birthday":"2020-01-01"},"nullCount":{"name":0,"age":0,"birthday":0}}""",
        partitionValues = Map.empty,
        version = 1,
        timestamp = 1651272635000L,
        responseFormat
      )
      verifyAddFile(
        files(2),
        size = 1030,
        stats =
          """{"numRecords":1,"minValues":{"name":"3","age":3,"birthday":"2020-01-01"},"maxValues":{"name":"3","age":3,"birthday":"2020-01-01"},"nullCount":{"name":0,"age":0,"birthday":0}}""",
        partitionValues = Map.empty,
        version = 1,
        timestamp = 1651272635000L,
        responseFormat
      )
      verifyAddCDCFile(
        files(3),
        size = 1301,
        partitionValues = Map.empty,
        version = 2,
        timestamp = 1651272655000L,
        responseFormat
      )
      verifyAddCDCFile(
        files(4),
        size = 1416,
        partitionValues = Map.empty,
        version = 3,
        timestamp = 1651272660000L,
        responseFormat
      )
    }
  }

  integrationTest("cdf_table_cdf_enabled_changes - paginated query table changes") {
    // version 1: 3 adds
    // version 2: 1 cdc
    Seq(RESPONSE_FORMAT_PARQUET, RESPONSE_FORMAT_DELTA).foreach { responseFormat =>
      var response = readNDJson(
        requestPath("/shares/share8/schemas/default/tables/cdf_table_cdf_enabled/changes?startingVersion=0&endingVersion=2&maxFiles=2"),
        Some("GET"),
        None,
        Some(0),
        responseFormat
      )
      var lines = response.split("\n")
      assert(lines.length == 5)
      val protocol = lines(0)
      val metadata = lines(1)
      val files = ArrayBuffer[String]()
      files.appendAll(Seq(lines(2), lines(3)))
      var endAction = JsonUtils.fromJson[SingleAction](lines(4)).endStreamAction
      assert(endAction.minUrlExpirationTimestamp != null)
      var numPages = 1
      while (endAction.nextPageToken != null) {
        numPages += 1
        response = readNDJson(
          requestPath(
            s"/shares/share8/schemas/default/tables/cdf_table_cdf_enabled/changes?startingVersion=0&endingVersion=2&maxFiles=2&pageToken=${endAction.nextPageToken}"
          ),
          Some("GET"),
          None,
          Some(0),
          responseFormat
        )
        lines = response.split("\n")
        assert(lines.length == 5)
        assert(protocol == lines(0))
        assert(metadata == lines(1))
        files.appendAll(Seq(lines(2), lines(3)))
        endAction = JsonUtils.fromJson[SingleAction](lines(4)).endStreamAction
        assert(endAction.minUrlExpirationTimestamp != null)
      }
      assert(numPages == 2)

      if (responseFormat == RESPONSE_FORMAT_DELTA) {
        val responseProtocol = JsonUtils.fromJson[DeltaResponseSingleAction](protocol).protocol
        assert(responseProtocol.deltaProtocol.minReaderVersion == 1)

        // unable to construct the delta action because the cases classes like AddFile/Metadata
        // are private to io.delta.standalone.internal.
        // So we only verify a couple important fields.
        val responseMetadata = JsonUtils.fromJson[DeltaResponseSingleAction](metadata).metaData
        assert(responseMetadata.deltaMetadata.id == "16736144-3306-4577-807a-d3f899b77670")
        assert(responseMetadata.version == 5)
      } else {
        val expectedProtocol = Protocol(minReaderVersion = 1).wrap
        assert(expectedProtocol == JsonUtils.fromJson[SingleAction](protocol))
        val expectedMetadata = Metadata(
          id = "16736144-3306-4577-807a-d3f899b77670",
          format = Format(),
          schemaString =
            """{"type":"struct","fields":[{"name":"name","type":"string","nullable":true,"metadata":{}},{"name":"age","type":"integer","nullable":true,"metadata":{}},{"name":"birthday","type":"date","nullable":true,"metadata":{}}]}""",
          configuration = Map("enableChangeDataFeed" -> "true"),
          partitionColumns = Nil,
          version = 5
        ).wrap
        assert(expectedMetadata == JsonUtils.fromJson[SingleAction](metadata))
      }
      assert(files.length == 4)
      verifyAddFile(
        files(0),
        size = 1030,
        stats =
          """{"numRecords":1,"minValues":{"name":"1","age":1,"birthday":"2020-01-01"},"maxValues":{"name":"1","age":1,"birthday":"2020-01-01"},"nullCount":{"name":0,"age":0,"birthday":0}}""",
        partitionValues = Map.empty,
        version = 1,
        timestamp = 1651272635000L,
        responseFormat
      )
      verifyAddFile(
        files(1),
        size = 1030,
        stats =
          """{"numRecords":1,"minValues":{"name":"2","age":2,"birthday":"2020-01-01"},"maxValues":{"name":"2","age":2,"birthday":"2020-01-01"},"nullCount":{"name":0,"age":0,"birthday":0}}""",
        partitionValues = Map.empty,
        version = 1,
        timestamp = 1651272635000L,
        responseFormat
      )
      verifyAddFile(
        files(2),
        size = 1030,
        stats =
          """{"numRecords":1,"minValues":{"name":"3","age":3,"birthday":"2020-01-01"},"maxValues":{"name":"3","age":3,"birthday":"2020-01-01"},"nullCount":{"name":0,"age":0,"birthday":0}}""",
        partitionValues = Map.empty,
        version = 1,
        timestamp = 1651272635000L,
        responseFormat
      )
      verifyAddCDCFile(
        files(3),
        size = 1301,
        partitionValues = Map.empty,
        version = 2,
        timestamp = 1651272655000L,
        responseFormat
      )
    }
  }

  integrationTest("cdf_table_cdf_enabled_changes - timestamp works") {
    // 1651272616000, PST: 2022-04-29 15:50:16.0 -> version 0
    val startStr = new Timestamp(1651272616000L).toInstant.toString
    // 1651272660000, PST: 2022-04-29 15:51:00.0 -> version 3
    val endStr = new Timestamp(1651272660000L).toInstant.toString

    val response = readNDJson(requestPath(s"/shares/share8/schemas/default/tables/cdf_table_cdf_enabled/changes?startingTimestamp=${startStr}&endingTimestamp=${endStr}"), Some("GET"), None, Some(0))
    val lines = response.split("\n")
    val protocol = lines(0)
    val metadata = lines(1)
    val expectedProtocol = Protocol(minReaderVersion = 1).wrap
    assert(expectedProtocol == JsonUtils.fromJson[SingleAction](protocol))
    val expectedMetadata = Metadata(
      id = "16736144-3306-4577-807a-d3f899b77670",
      format = Format(),
      schemaString = """{"type":"struct","fields":[{"name":"name","type":"string","nullable":true,"metadata":{}},{"name":"age","type":"integer","nullable":true,"metadata":{}},{"name":"birthday","type":"date","nullable":true,"metadata":{}}]}""",
      configuration = Map("enableChangeDataFeed" -> "true"),
      partitionColumns = Nil,
      version = 5).wrap
    assert(expectedMetadata == JsonUtils.fromJson[SingleAction](metadata))
    val files = lines.drop(2)
    assert(files.size == 5)
  }

  integrationTest("cdf_table_with_partition - query table changes") {
    Seq(RESPONSE_FORMAT_PARQUET, RESPONSE_FORMAT_DELTA).foreach { responseFormat =>
      val response = readNDJson(requestPath(s"/shares/share8/schemas/default/tables/cdf_table_with_partition/changes?startingVersion=1&endingVersion=3"), Some("GET"), None, Some(1), responseFormat)
      val lines = response.split("\n")
      val files = lines.drop(2)
      assert(files.size == 6)
      // In version 2, birthday is updated from 2020-01-01 to 2020-02-02 for one row, which result in
      // 2 cdc files below.
      verifyAddFile(
        files(0),
        size = 791,
        stats =
          """{"numRecords":1,"minValues":{"name":"1","age":1},"maxValues":{"name":"1","age":1},"nullCount":{"name":0,"age":0}}""",
        partitionValues = Map("birthday" -> "2020-01-01"),
        version = 1,
        timestamp = 1651614980000L,
        responseFormat
      )
      verifyAddFile(
        files(1),
        size = 791,
        stats =
          """{"numRecords":1,"minValues":{"name":"2","age":2},"maxValues":{"name":"2","age":2},"nullCount":{"name":0,"age":0}}""",
        partitionValues = Map("birthday" -> "2020-01-01"),
        version = 1,
        timestamp = 1651614980000L,
        responseFormat
      )
      verifyAddFile(
        files(2),
        size = 791,
        stats =
          """{"numRecords":1,"minValues":{"name":"3","age":3},"maxValues":{"name":"3","age":3},"nullCount":{"name":0,"age":0}}""",
        partitionValues = Map("birthday" -> "2020-03-03"),
        version = 1,
        timestamp = 1651614980000L,
        responseFormat
      )
      verifyAddCDCFile(
        files(3),
        size = 1125,
        partitionValues = Map("birthday" -> "2020-01-01"),
        version = 2,
        timestamp = 1651614986000L,
        responseFormat
      )
      verifyAddCDCFile(
        files(4),
        size = 1132,
        partitionValues = Map("birthday" -> "2020-02-02"),
        version = 2,
        timestamp = 1651614986000L,
        responseFormat
      )
      verifyRemove(
        files(5),
        size = 791,
        partitionValues = Map("birthday" -> "2020-03-03"),
        version = 3,
        timestamp = 1651614994000L,
        responseFormat
      )
    }
  }

  integrationTest("cdf_table_with_partition - paginated query table changes") {
    // version 2: 2 cdc
    // version 3: 1 remove
    Seq(RESPONSE_FORMAT_PARQUET, RESPONSE_FORMAT_DELTA).foreach { responseFormat =>
      var response = readNDJson(
        requestPath(
          "/shares/share8/schemas/default/tables/cdf_table_with_partition/changes?startingVersion=2&maxFiles=1"
        ),
        Some("GET"),
        None,
        Some(2),
        responseFormat
      )
      var lines = response.split("\n")
      assert(lines.length == 4)
      val protocol = lines(0)
      val metadata = lines(1)
      val files = ArrayBuffer[String]()
      files.append(lines(2))
      var endAction = JsonUtils.fromJson[SingleAction](lines(3)).endStreamAction
      assert(endAction.minUrlExpirationTimestamp != null)
      var numPages = 1
      while (endAction.nextPageToken != null) {
        numPages += 1
        response = readNDJson(
          requestPath(
            s"/shares/share8/schemas/default/tables/cdf_table_with_partition/changes?startingVersion=2&maxFiles=1&pageToken=${endAction.nextPageToken}"
          ),
          Some("GET"),
          None,
          Some(2),
          responseFormat
        )
        lines = response.split("\n")
        assert(lines.length == 4)
        assert(protocol == lines(0))
        assert(metadata == lines(1))
        files.append(lines(2))
        endAction = JsonUtils.fromJson[SingleAction](lines(3)).endStreamAction
        assert(endAction.minUrlExpirationTimestamp != null)
      }
      assert(numPages == 3)

      verifyAddCDCFile(
        files(0),
        size = 1125,
        partitionValues = Map("birthday" -> "2020-01-01"),
        version = 2,
        timestamp = 1651614986000L,
        responseFormat
      )
      verifyAddCDCFile(
        files(1),
        size = 1132,
        partitionValues = Map("birthday" -> "2020-02-02"),
        version = 2,
        timestamp = 1651614986000L,
        responseFormat
      )
      verifyRemove(
        files(2),
        size = 791,
        partitionValues = Map("birthday" -> "2020-03-03"),
        version = 3,
        timestamp = 1651614994000L,
        responseFormat
      )
    }
  }

  integrationTest("streaming_notnull_to_null - additional metadata returned") {
    // additional metadata returned for streaming query
    val response = readNDJson(
      requestPath("/shares/share8/schemas/default/tables/streaming_notnull_to_null/changes?startingVersion=0&includeHistoricalMetadata=true"),
      Some("GET"),
      None,
      Some(0)
    )
    val actions = response.split("\n").map(JsonUtils.fromJson[SingleAction](_))
    assert(actions.size == 5)
    val expectedProtocol = Protocol(minReaderVersion = 1)
    assert(expectedProtocol == actions(0).protocol)
    var expectedMetadata = Metadata(
      id = "1e2201ff-12ad-4c3b-a539-4d34e9e36680",
      format = Format(),
      schemaString = """{"type":"struct","fields":[{"name":"name","type":"string","nullable":false,"metadata":{}}]}""",
      configuration = Map("enableChangeDataFeed" -> "true"),
      partitionColumns = Nil,
      version = 0)
    assert(expectedMetadata == actions(1).metaData)

    expectedMetadata = expectedMetadata.copy(
      schemaString = """{"type":"struct","fields":[{"name":"name","type":"string","nullable":true,"metadata":{}}]}""",
      version = 2
    )
    assert(expectedMetadata == actions(3).metaData)
    assert(actions(2).add != null)
    assert(actions(4).add != null)
  }

  integrationTest("streaming_notnull_to_null - paginated query with additional metadata returned") {
    // additional metadata returned with includeHistoricalMetadata=true
    val expectedProtocol = Protocol(minReaderVersion = 1)
    val expectedMetadata = Metadata(
      id = "1e2201ff-12ad-4c3b-a539-4d34e9e36680",
      format = Format(),
      schemaString =
        """{"type":"struct","fields":[{"name":"name","type":"string","nullable":false,"metadata":{}}]}""",
      configuration = Map("enableChangeDataFeed" -> "true"),
      partitionColumns = Nil,
      version = 0
    )

    // page 1
    var response = readNDJson(
      requestPath(
        "/shares/share8/schemas/default/tables/streaming_notnull_to_null/changes?startingVersion=0&includeHistoricalMetadata=true&maxFiles=1"
      ),
      Some("GET"),
      None,
      Some(0)
    )
    var actions = response.split("\n").map(JsonUtils.fromJson[SingleAction](_))
    assert(actions.length == 5)
    assert(expectedProtocol == actions(0).protocol)
    assert(expectedMetadata == actions(1).metaData)
    assert(actions(2).add != null)
    assert(
      actions(3).metaData == expectedMetadata.copy(
        schemaString =
          """{"type":"struct","fields":[{"name":"name","type":"string","nullable":true,"metadata":{}}]}""",
        version = 2
      )
    )
    var endAction = actions(4).endStreamAction
    assert(endAction.nextPageToken != null)
    assert(endAction.minUrlExpirationTimestamp == actions(2).add.expirationTimestamp)

    // page 2
    response = readNDJson(
      requestPath(
        s"/shares/share8/schemas/default/tables/streaming_notnull_to_null/changes?startingVersion=0&includeHistoricalMetadata=true&maxFiles=1&pageToken=${endAction.nextPageToken}"
      ),
      Some("GET"),
      None,
      Some(0)
    )
    actions = response.split("\n").map(JsonUtils.fromJson[SingleAction](_))
    assert(actions.length == 4)
    assert(expectedProtocol == actions(0).protocol)
    assert(expectedMetadata == actions(1).metaData)
    assert(actions(2).add != null)
    endAction = actions(3).endStreamAction
    assert(endAction.nextPageToken == null)
    assert(endAction.minUrlExpirationTimestamp == actions(2).add.expirationTimestamp)
  }

  integrationTest("streaming_notnull_to_null - additional metadata not returned") {
    // additional metadata not returned when includeHistoricalMetadata is not set
    val response = readNDJson(requestPath("/shares/share8/schemas/default/tables/streaming_notnull_to_null/changes?startingVersion=0"), Some("GET"), None, Some(0))
    val actions = response.split("\n").map(JsonUtils.fromJson[SingleAction](_))
    assert(actions.size == 4)
    val expectedProtocol = Protocol(minReaderVersion = 1)
    assert(expectedProtocol == actions(0).protocol)
    var expectedMetadata = Metadata(
      id = "1e2201ff-12ad-4c3b-a539-4d34e9e36680",
      format = Format(),
      schemaString = """{"type":"struct","fields":[{"name":"name","type":"string","nullable":true,"metadata":{}}]}""",
      configuration = Map("enableChangeDataFeed" -> "true"),
      partitionColumns = Nil,
      version = 3)
    assert(expectedMetadata == actions(1).metaData)

    assert(actions(2).add != null)
    assert(actions(3).add != null)
  }

  integrationTest("streaming_notnull_to_null - paginated query with additional metadata not returned") {
    // additional metadata not returned when includeHistoricalMetadata is not set
    val expectedProtocol = Protocol(minReaderVersion = 1)
    val expectedMetadata = Metadata(
      id = "1e2201ff-12ad-4c3b-a539-4d34e9e36680",
      format = Format(),
      schemaString =
        """{"type":"struct","fields":[{"name":"name","type":"string","nullable":true,"metadata":{}}]}""",
      configuration = Map("enableChangeDataFeed" -> "true"),
      partitionColumns = Nil,
      version = 3
    )

    // page 1
    var response = readNDJson(
      requestPath(
        "/shares/share8/schemas/default/tables/streaming_notnull_to_null/changes?startingVersion=0&maxFiles=1"
      ),
      Some("GET"),
      None,
      Some(0)
    )
    var actions = response.split("\n").map(JsonUtils.fromJson[SingleAction](_))
    assert(actions.length == 4)
    assert(expectedProtocol == actions(0).protocol)
    assert(expectedMetadata == actions(1).metaData)
    assert(actions(2).add != null)
    var endAction = actions(3).endStreamAction
    assert(endAction.nextPageToken != null)
    assert(endAction.minUrlExpirationTimestamp == actions(2).add.expirationTimestamp)

    // page 2
    response = readNDJson(
      requestPath(
        s"/shares/share8/schemas/default/tables/streaming_notnull_to_null/changes?startingVersion=0&maxFiles=1&pageToken=${endAction.nextPageToken}"
      ),
      Some("GET"),
      None,
      Some(0)
    )
    actions = response.split("\n").map(JsonUtils.fromJson[SingleAction](_))
    assert(actions.length == 4)
    assert(expectedProtocol == actions(0).protocol)
    assert(expectedMetadata == actions(1).metaData)
    assert(actions(2).add != null)
    endAction = actions(3).endStreamAction
    assert(endAction.nextPageToken == null)
    assert(endAction.minUrlExpirationTimestamp == actions(2).add.expirationTimestamp)
  }

  private def verifyAddFile(
      actionStr: String,
      size: Long,
      stats: String,
      partitionValues: Map[String, String],
      version: Long,
      timestamp: Long,
      responseFormat: String = RESPONSE_FORMAT_PARQUET): Unit = {
    if (responseFormat == RESPONSE_FORMAT_DELTA) {
      val responseFileAction = JsonUtils.fromJson[DeltaResponseSingleAction](actionStr).file
      val addFile = responseFileAction.deltaSingleAction.add
      assert(addFile.size == size)
      assert(addFile.stats == stats)
      assert(addFile.partitionValues == partitionValues)
      verifyPreSignedUrl(addFile.path, size.toInt)
      assert(responseFileAction.version == version)
      assert(responseFileAction.timestamp == timestamp)
      val timeToExpiration = responseFileAction.expirationTimestamp - System.currentTimeMillis()
      assert(timeToExpiration < 60 * 60 * 1000 && timeToExpiration > 50 * 60 * 1000)
    } else {
      assert(actionStr.startsWith("{\"add\":{"))
      val addFile = JsonUtils.fromJson[SingleAction](actionStr).add
      assert(addFile.size == size)
      assert(addFile.stats == stats)
      assert(addFile.partitionValues == partitionValues)
      assert(addFile.version == version)
      assert(addFile.timestamp == timestamp)
      verifyPreSignedUrl(addFile.url, size.toInt)
      val timeToExpiration = addFile.expirationTimestamp - System.currentTimeMillis()
      assert(timeToExpiration < 60 * 60 * 1000 && timeToExpiration > 50 * 60 * 1000)
    }
  }

  private def verifyAddCDCFile(
      actionStr: String,
      size: Long,
      partitionValues: Map[String, String],
      version: Long,
      timestamp: Long,
      responseFormat: String = RESPONSE_FORMAT_PARQUET): Unit = {
    if (responseFormat == RESPONSE_FORMAT_DELTA) {
      val responseFileAction = JsonUtils.fromJson[DeltaResponseSingleAction](actionStr).file
      val addCDCFile = responseFileAction.deltaSingleAction.cdc
      assert(addCDCFile.size == size)
      assert(addCDCFile.partitionValues == partitionValues)
      verifyPreSignedUrl(addCDCFile.path, size.toInt)
      assert(responseFileAction.version == version)
      assert(responseFileAction.timestamp == timestamp)
      val timeToExpiration = responseFileAction.expirationTimestamp - System.currentTimeMillis()
      assert(timeToExpiration < 60 * 60 * 1000 && timeToExpiration > 50 * 60 * 1000)
    } else {
      assert(actionStr.startsWith("{\"cdf\":{"))
      val addCDCFile = JsonUtils.fromJson[SingleAction](actionStr).cdf
      assert(addCDCFile.size == size)
      assert(addCDCFile.partitionValues == partitionValues)
      assert(addCDCFile.version == version)
      assert(addCDCFile.timestamp == timestamp)
      verifyPreSignedUrl(addCDCFile.url, size.toInt)
      val timeToExpiration = addCDCFile.expirationTimestamp - System.currentTimeMillis()
      assert(timeToExpiration < 60 * 60 * 1000 && timeToExpiration > 50 * 60 * 1000)
    }
  }

  private def verifyRemove(
      actionStr: String,
      size: Long,
      partitionValues: Map[String, String],
      version: Long,
      timestamp: Long,
      responseFormat: String = RESPONSE_FORMAT_PARQUET): Unit = {
    if (responseFormat == RESPONSE_FORMAT_DELTA) {
      val responseFileAction = JsonUtils.fromJson[DeltaResponseSingleAction](actionStr).file
      val removeFile = responseFileAction.deltaSingleAction.remove
      assert(removeFile.size == Some(size))
      assert(removeFile.partitionValues == partitionValues)
      verifyPreSignedUrl(removeFile.path, size.toInt)
      assert(responseFileAction.version == version)
      assert(responseFileAction.timestamp == timestamp)
      val timeToExpiration = responseFileAction.expirationTimestamp - System.currentTimeMillis()
      assert(timeToExpiration < 60 * 60 * 1000 && timeToExpiration > 50 * 60 * 1000)
    } else {
      assert(actionStr.startsWith("{\"remove\":{"))
      val removeFile = JsonUtils.fromJson[SingleAction](actionStr).remove
      assert(removeFile.size == size)
      assert(removeFile.partitionValues == partitionValues)
      assert(removeFile.version == version)
      assert(removeFile.timestamp == timestamp)
      verifyPreSignedUrl(removeFile.url, size.toInt)
      val timeToExpiration = removeFile.expirationTimestamp - System.currentTimeMillis()
      assert(timeToExpiration < 60 * 60 * 1000 && timeToExpiration > 50 * 60 * 1000)
    }
  }

  integrationTest("table_data_loss_with_checkpoint - /shares/{share}/schemas/{schema}/tables/{table}/query") {
    // For table_data_loss_with_checkpoint:
    // VERSION 1: INSERT 2 rows
    // VERSION 2: INSERT 1 row
    // generated a checkpoint at version 2.
    // deleted json file for version 1 -- data loss
    val expectedProtocol = Protocol(minReaderVersion = 1)

    // queryTable, latest snapshot at version 2, works because of checkpoint
    var response = readNDJson(requestPath("/shares/share8/schemas/default/tables/table_data_loss_with_checkpoint/query"), Some("POST"), Some("{}"), Some(2))
    var lines = response.split("\n")
    assert(expectedProtocol == JsonUtils.fromJson[SingleAction](lines(0)).protocol)
    val metadata = JsonUtils.fromJson[SingleAction](lines(1)).metaData
    val expectedMetadata = Metadata(
      id = metadata.id,
      format = Format(),
      schemaString = """{"type":"struct","fields":[{"name":"name","type":"string","nullable":true,"metadata":{}},{"name":"age","type":"integer","nullable":true,"metadata":{}},{"name":"birthday","type":"date","nullable":true,"metadata":{}}]}""",
      configuration = Map("enableChangeDataFeed" -> "true"),
      partitionColumns = Nil)
    assert(expectedMetadata == metadata)
    assert(lines.size == 4)

    // queryTable, version 2, works because of checkpoint
    var p =
      """
        |{
        |  "version": "2"
        |}
        |""".stripMargin
    response = readNDJson(requestPath("/shares/share8/schemas/default/tables/table_data_loss_with_checkpoint/query"), Some("POST"), Some(p), Some(2))
    lines = response.split("\n")
    assert(expectedProtocol == JsonUtils.fromJson[SingleAction](lines(0)).protocol)
    assert(expectedMetadata == JsonUtils.fromJson[SingleAction](lines(1)).metaData)
    assert(lines.size == 4)

    // queryTable, startingVersion 2, works because of checkpoint
    p =
      """
        |{
        |  "startingVersion": "2"
        |}
        |""".stripMargin
    response = readNDJson(requestPath("/shares/share8/schemas/default/tables/table_data_loss_with_checkpoint/query"), Some("POST"), Some(p), Some(2))
    lines = response.split("\n")
    assert(expectedProtocol == JsonUtils.fromJson[SingleAction](lines(0)).protocol)
    assert(expectedMetadata.copy(version = 2) ==
      JsonUtils.fromJson[SingleAction](lines(1)).metaData)
    assert(lines.size == 3)

    // queryTable, startingVersion 1, fails because data loss
    p =
      """
        |{
        |  "startingVersion": "1"
        |}
        |""".stripMargin
    assertHttpError(
      url = requestPath("/shares/share8/schemas/default/tables/table_data_loss_with_checkpoint/query"),
      method = "POST",
      data = Some(p),
      expectedErrorCode = 500,
      expectedErrorMessage = ""
    )
  }

  integrationTest("table_data_loss_no_checkpoint - /shares/{share}/schemas/{schema}/tables/{table}/query") {
    val expectedProtocol = Protocol(minReaderVersion = 1)

    // queryTable, latest snapshot
    assertHttpError(
      url = requestPath("/shares/share8/schemas/default/tables/table_data_loss_no_checkpoint/query"),
      method = "POST",
      data = Some("{}"),
      expectedErrorCode = 500,
      expectedErrorMessage = ""
    )

    // queryTable, version 2
    var p =
      """
        |{
        |  "version": "2"
        |}
        |""".stripMargin
    assertHttpError(
      url = requestPath("/shares/share8/schemas/default/tables/table_data_loss_no_checkpoint/query"),
      method = "POST",
      data = Some(p),
      expectedErrorCode = 500,
      expectedErrorMessage = ""
    )

    // queryTable, startingVersion 1
    p =
      """
        |{
        |  "startingVersion": "2"
        |}
        |""".stripMargin
    assertHttpError(
      url = requestPath("/shares/share8/schemas/default/tables/table_data_loss_no_checkpoint/query"),
      method = "POST",
      data = Some(p),
      expectedErrorCode = 500,
      expectedErrorMessage = ""
    )
  }

  def assertHttpError(
    url: String,
    method: String,
    data: Option[String],
    expectedErrorCode: Int,
    expectedErrorMessage: String,
    headers: Map[String, String] = Map.empty[String, String]): Unit = {
    val connection = new URL(url).openConnection().asInstanceOf[HttpsURLConnection]
    connection.setRequestProperty("Authorization", s"Bearer ${TestResource.testAuthorizationToken}")
    connection.setRequestMethod(method)
    data.foreach { d =>
      connection.setDoOutput(true)
      connection.setRequestProperty("Content-Type", "application/json; charset=utf8")
      headers.foreach((item) => connection.setRequestProperty(item._1, item._2))

      val output = connection.getOutputStream()
      try {
        output.write(d.getBytes(UTF_8))
      } finally {
        output.close()
      }
    }
    val responseStatusCode = connection.getResponseCode()
    assert(responseStatusCode == expectedErrorCode)
    // If the http method is HEAD, error message is not returned from the server.
    assert(method == "HEAD" || IOUtils.toString(connection.getErrorStream()).contains(expectedErrorMessage))
  }

  integrationTest("valid request json but incorrect field type") {
    assertHttpError(
      url = requestPath("/shares/share1/schemas/default/tables/table1/query"),
      method = "POST",
      data = Some(
        """
          |{
          |  "predicateHints": {}
          |}
          |""".stripMargin),
      expectedErrorCode = 400,
      expectedErrorMessage =
        "Expected an array for repeated field predicateHints of QueryTableRequest"
    )
  }

  integrationTest("invalid request json") {
    assertHttpError(
      url = requestPath("/shares/share1/schemas/default/tables/table1/query"),
      method = "POST",
      data = Some(""),
      expectedErrorCode = 400,
      expectedErrorMessage = "No content to map due to end-of-input"
    )
  }

  integrationTest("invalid 'maxFiles' value") {
    assertHttpError(
      url = requestPath("/shares/share1/schemas/default/tables/table1/query"),
      method = "POST",
      data = Some("""{"maxFiles": 0}"""),
      expectedErrorCode = 400,
      expectedErrorMessage = "maxFiles must be positive"
    )
    assertHttpError(
      url = requestPath("/shares/share1/schemas/default/tables/table1/query"),
      method = "POST",
      data = Some("""{"maxFiles": 3000000000}"""),
      expectedErrorCode = 400,
      expectedErrorMessage = "Not an int32 value"
    )

    assertHttpError(
      url = requestPath("/shares/share1/schemas/default/tables/table1/changes?maxFiles=-1"),
      method = "GET",
      data = None,
      expectedErrorCode = 400,
      expectedErrorMessage = "maxFiles must be positive"
    )

    assertHttpError(
      url = requestPath("/shares/share1/schemas/default/tables/table1/changes?maxFiles=string"),
      method = "GET",
      data = None,
      expectedErrorCode = 400,
      expectedErrorMessage = "expected a number but the string didn't have the appropriate format"
    )
  }

  integrationTest("wrong 'maxResults' type") {
    assertHttpError(
      url = requestPath("/shares?maxResults=string"),
      method = "GET",
      data = None,
      expectedErrorCode = 400,
      expectedErrorMessage = "expected a number but the string didn't have the appropriate format"
    )
  }

  integrationTest("table1 - cannot query table changes") {
    assertHttpError(
      url = requestPath("/shares/share1/schemas/default/tables/table1/changes"),
      method = "GET",
      data = None,
      expectedErrorCode = 400,
      expectedErrorMessage = "cdf is not enabled on table share1.default.table1"
    )
  }

  integrationTest("cdf_table_cdf_enabled_changes - exceptions") {
    assertHttpError(
      url = requestPath("/shares/share8/schemas/default/tables/cdf_table_cdf_enabled/changes?startingTimestamp=2000-01-01T00:00:00-08:00"),
      method = "GET",
      data = None,
      expectedErrorCode = 400,
      expectedErrorMessage = "The provided timestamp ("
    )

    assertHttpError(
      url = requestPath("/shares/share8/schemas/default/tables/cdf_table_cdf_enabled/changes?startingTimestamp=9999-01-01T00:00:00-08:00"),
      method = "GET",
      data = None,
      expectedErrorCode = 400,
      expectedErrorMessage = "The provided timestamp ("
    )

    assertHttpError(
      url = requestPath("/shares/share8/schemas/default/tables/cdf_table_cdf_enabled/changes"),
      method = "GET",
      data = None,
      expectedErrorCode = 400,
      expectedErrorMessage = "No startingVersion or startingTimestamp provided for CDF read"
    )

    assertHttpError(
      url = requestPath("/shares/share8/schemas/default/tables/cdf_table_cdf_enabled/changes?startingVersion=1&startingTimestamp=2022-02-02T00:00:00Z"),
      method = "GET",
      data = None,
      expectedErrorCode = 400,
      expectedErrorMessage = "Multiple starting arguments provided for CDF read"
    )

    assertHttpError(
      url = requestPath("/shares/share8/schemas/default/tables/cdf_table_cdf_enabled/changes?startingVersion=1&endingVersion=3&endingTimestamp=randomString"),
      method = "GET",
      data = None,
      expectedErrorCode = 400,
      expectedErrorMessage = "Multiple ending arguments provided for CDF read"
    )

    assertHttpError(
      url = requestPath("/shares/share8/schemas/default/tables/cdf_table_cdf_enabled/changes?startingTimestamp=2022-04-29"),
      method = "GET",
      data = None,
      expectedErrorCode = 400,
      expectedErrorMessage = "Invalid startingTimestamp"
    )

    assertHttpError(
      url = requestPath("/shares/share8/schemas/default/tables/cdf_table_cdf_enabled/changes?startingVersion=2&endingVersion=1"),
      method = "GET",
      data = None,
      expectedErrorCode = 400,
      expectedErrorMessage = "CDF range from start 2 to end 1 was invalid. End cannot be before start"
    )

    assertHttpError(
      url = requestPath("/shares/share8/schemas/default/tables/cdf_table_cdf_enabled/changes?startingVersion=6"),
      method = "GET",
      data = None,
      expectedErrorCode = 400,
      expectedErrorMessage = "Provided Start version(6) for reading change data is invalid. Start version cannot be greater than the latest version of the table(5)"
    )

    assertHttpError(
      url = requestPath("/shares/share8/schemas/default/tables/cdf_table_cdf_enabled/changes?startingVersion=0&endingVersion=5"),
      method = "GET",
      data = None,
      expectedErrorCode = 400,
      expectedErrorMessage = "Error getting change data for range [0, 5] as change data was not recorded for version [4]"
    )

    assertHttpError(
      url = requestPath("/shares/share8/schemas/default/tables/cdf_table_cdf_enabled/changes?startingVersion=4"),
      method = "GET",
      data = None,
      expectedErrorCode = 400,
      expectedErrorMessage = "Error getting change data for range [4, 5] as change data was not recorded for version [4]"
    )
  }

  integrationTest("cdf_table_with_partition - exceptions on startVersion") {
    assertHttpError(
      url = requestPath("/shares/share8/schemas/default/tables/cdf_table_with_partition/changes?startingVersion=0"),
      method = "GET",
      data = None,
      expectedErrorCode = 400,
      expectedErrorMessage = "You can only query table changes since version 1"
    )

    assertHttpError(
      url = requestPath("/shares/share8/schemas/default/tables/cdf_table_with_partition/query"),
      method = "POST",
      data = Some("""
        {"version": "0"}
      """),
      expectedErrorCode = 400,
      expectedErrorMessage = "You can only query table data since version 1"
    )

    assertHttpError(
      url = requestPath("/shares/share8/schemas/default/tables/cdf_table_with_partition/query"),
      method = "POST",
      data = Some("""
        {"startingVersion": "0"}
      """),
      expectedErrorCode = 400,
      expectedErrorMessage = "You can only query table data since version 1"
    )
  }

  integrationTest("azure support") {
    for (azureTableName <- "table_wasb" :: "table_abfs" :: Nil) {
      val response = readNDJson(requestPath(s"/shares/share_azure/schemas/default/tables/${azureTableName}/query"), Some("POST"), Some("{}"), Some(0))
      val lines = response.split("\n")
      val protocol = lines(0)
      val metadata = lines(1)
      val expectedProtocol = Protocol(minReaderVersion = 1).wrap
      assert(expectedProtocol == JsonUtils.fromJson[SingleAction](protocol))
      val expectedMetadata = Metadata(
        id = "de102585-bd69-4bba-bb10-fa92c50a7f85",
        format = Format(),
        schemaString = """{"type":"struct","fields":[{"name":"c1","type":"string","nullable":true,"metadata":{}},{"name":"c2","type":"string","nullable":true,"metadata":{}}]}""",
        partitionColumns = Seq("c2")).wrap
      assert(expectedMetadata == JsonUtils.fromJson[SingleAction](metadata))
      val files = lines.drop(2)
      val actualFiles = files.map(f => JsonUtils.fromJson[SingleAction](f).file)
      assert(actualFiles.size == 1)
      val expectedFiles = Seq(
        AddFile(
          url = actualFiles(0).url,
          expirationTimestamp = actualFiles(0).expirationTimestamp,
          id = "84f5f9e4de01e99837f77bfc2b7215b0",
          partitionValues = Map("c2" -> "foo bar"),
          size = 568,
          stats = """{"numRecords":1,"minValues":{"c1":"foo bar"},"maxValues":{"c1":"foo bar"},"nullCount":{"c1":0}}"""
        )
      )
      assert(expectedFiles == actualFiles.toList)
      verifyPreSignedUrl(actualFiles(0).url, 568)
    }
  }

  integrationTest("gcp support") {
    val gcsTableName = "table_gcs"
    val response = readNDJson(requestPath(s"/shares/share_gcp/schemas/default/tables/${gcsTableName}/query"), Some("POST"), Some("{}"), Some(0))
    val lines = response.split("\n")
    val protocol = lines(0)
    val metadata = lines(1)
    val expectedProtocol = Protocol(minReaderVersion = 1).wrap
    assert(expectedProtocol == JsonUtils.fromJson[SingleAction](protocol))
    val expectedMetadata = Metadata(
      id = "de102585-bd69-4bba-bb10-fa92c50a7f85",
      format = Format(),
      schemaString = """{"type":"struct","fields":[{"name":"c1","type":"string","nullable":true,"metadata":{}},{"name":"c2","type":"string","nullable":true,"metadata":{}}]}""",
      partitionColumns = Seq("c2")).wrap
    assert(expectedMetadata == JsonUtils.fromJson[SingleAction](metadata))
    val files = lines.drop(2)
    val actualFiles = files.map(f => JsonUtils.fromJson[SingleAction](f).file)
    assert(actualFiles.size == 1)
    val expectedFiles = Seq(
      AddFile(
        url = actualFiles(0).url,
        expirationTimestamp = actualFiles(0).expirationTimestamp,
        id = "84f5f9e4de01e99837f77bfc2b7215b0",
        partitionValues = Map("c2" -> "foo bar"),
        size = 568,
        stats = """{"numRecords":1,"minValues":{"c1":"foo bar"},"maxValues":{"c1":"foo bar"},"nullCount":{"c1":0}}"""
      )
    )
    assert(expectedFiles == actualFiles.toList)
    verifyPreSignedUrl(actualFiles(0).url, 568)
  }
}
