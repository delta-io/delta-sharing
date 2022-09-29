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
import io.delta.standalone.internal.DeltaCDFErrors
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
      server = DeltaSharingService.start(serverConfig)
    }
  }

  override def afterAll() {
    if (server != null) {
      server.stop().get()
    }
  }

  def readJson(url: String, expectedTableVersion: Option[Long] = None): String = {
    readHttpContent(url, None, None, expectedTableVersion, "application/json; charset=utf-8")
  }

  def readNDJson(url: String, method: Option[String] = None, data: Option[String] = None, expectedTableVersion: Option[Long] = None): String = {
    readHttpContent(url, method, data, expectedTableVersion, "application/x-ndjson; charset=utf-8")
  }

  def readHttpContent(url: String, method: Option[String], data: Option[String] = None, expectedTableVersion: Option[Long] = None, expectedContentType: String): String = {
    val connection = new URL(url).openConnection().asInstanceOf[HttpsURLConnection]
    connection.setRequestProperty("Authorization", s"Bearer ${TestResource.testAuthorizationToken}")
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
        Share().withName("share_gcp")
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
        Share().withName("share_gcp")
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
      Table().withName("table1").withSchema("default").withShare("share1") ::
        Table().withName("table3").withSchema("default").withShare("share1") ::
        Table().withName("table7").withSchema("default").withShare("share1") ::
        Table().withName("cdf_table_cdf_enabled").withSchema("default").withShare("share1") ::
        Table().withName("cdf_table_with_partition").withSchema("default").withShare("share1") ::
        Table().withName("cdf_table_with_vacuum").withSchema("default").withShare("share1") ::
        Table().withName("cdf_table_missing_log").withSchema("default").withShare("share1") :: Nil)
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
      Table().withName("table1").withSchema("default").withShare("share1") ::
        Table().withName("table3").withSchema("default").withShare("share1") ::
        Table().withName("table7").withSchema("default").withShare("share1") ::
        Table().withName("cdf_table_cdf_enabled").withSchema("default").withShare("share1") ::
        Table().withName("cdf_table_with_partition").withSchema("default").withShare("share1") ::
        Table().withName("cdf_table_with_vacuum").withSchema("default").withShare("share1") ::
        Table().withName("cdf_table_missing_log").withSchema("default").withShare("share1") :: Nil
    assert(expected == tables)
  }

  integrationTest("/shares/{share}/all-tables") {
    val response = readJson(requestPath("/shares/share7/all-tables"))
    val expected = ListAllTablesResponse(
      Table().withName("table8").withSchema("schema1").withShare("share7") ::
        Table().withName("table9").withSchema("schema2").withShare("share7") :: Nil)
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
      Table().withName("table8").withSchema("schema1").withShare("share7") ::
        Table().withName("table9").withSchema("schema2").withShare("share7") :: Nil
    assert(expected == tables)
  }


  integrationTest("table1 - head - /shares/{share}/schemas/{schema}/tables/{table}") {
    val url = requestPath("/shares/share1/schemas/default/tables/table1")
    val connection = new URL(url).openConnection().asInstanceOf[HttpsURLConnection]
    connection.setRequestMethod("HEAD")
    connection.setRequestProperty("Authorization", s"Bearer ${TestResource.testAuthorizationToken}")
    val input = connection.getInputStream()
    try {
      IOUtils.toString(input)
    } finally {
      input.close()
    }
    val deltaTableVersion = connection.getHeaderField("Delta-Table-Version")
    assert(deltaTableVersion == "2")
  }

  integrationTest("table1 - non partitioned - /shares/{share}/schemas/{schema}/tables/{table}/metadata") {
    val response = readNDJson(requestPath("/shares/share1/schemas/default/tables/table1/metadata"), expectedTableVersion = Some(2))
    val Array(protocol, metadata) = response.split("\n")
    val expectedProtocol = Protocol(minReaderVersion = 1).wrap
    assert(expectedProtocol == JsonUtils.fromJson[SingleAction](protocol))
    val expectedMetadata = Metadata(
      id = "ed96aa41-1d81-4b7f-8fb5-846878b4b0cf",
      format = Format(),
      schemaString = """{"type":"struct","fields":[{"name":"eventTime","type":"timestamp","nullable":true,"metadata":{}},{"name":"date","type":"date","nullable":true,"metadata":{}}]}""",
      partitionColumns = Nil).wrap
    assert(expectedMetadata == JsonUtils.fromJson[SingleAction](metadata))
  }

  integrationTest("table1 - non partitioned - /shares/{share}/schemas/{schema}/tables/{table}/query") {
    val p =
      """
        |{
        |  "predicateHints": [
        |    "date = CAST('2021-04-28' AS DATE)"
        |  ]
        |}
        |""".stripMargin
    val response = readNDJson(requestPath("/shares/share1/schemas/default/tables/table1/query"), Some("POST"), Some(p), Some(2))
    val lines = response.split("\n")
    val protocol = lines(0)
    val metadata = lines(1)
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
        id = "061cb3683a467066995f8cdaabd8667d",
        partitionValues = Map.empty,
        size = 781,
        stats = """{"numRecords":1,"minValues":{"eventTime":"2021-04-28T06:32:22.421Z","date":"2021-04-28"},"maxValues":{"eventTime":"2021-04-28T06:32:22.421Z","date":"2021-04-28"},"nullCount":{"eventTime":0,"date":0}}"""
      ),
      AddFile(
        url = actualFiles(1).url,
        id = "e268cbf70dbaa6143e7e9fa3e2d3b00e",
        partitionValues = Map.empty,
        size = 781,
        stats = """{"numRecords":1,"minValues":{"eventTime":"2021-04-28T06:32:02.070Z","date":"2021-04-28"},"maxValues":{"eventTime":"2021-04-28T06:32:02.070Z","date":"2021-04-28"},"nullCount":{"eventTime":0,"date":0}}"""
      )
    )
    assert(expectedFiles == actualFiles.toList)
    verifyPreSignedUrl(actualFiles(0).url, 781)
    verifyPreSignedUrl(actualFiles(1).url, 781)
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

  integrationTest("table2 - version 1 : cdfEnabled is false") {
    assertHttpError(
      url = requestPath("/shares/share2/schemas/default/tables/table2/query"),
      method = "POST",
      data = Some("""{"version": 1}"""),
      expectedErrorCode = 400,
      expectedErrorMessage = "Reading table by version or timestamp is not supported because change data feed is not enabled on table: share2.default.table2"
    )
  }

  integrationTest("table2 - timestamp not supported: cdfEnabled is false") {
    // timestamp can be any string here, it's resolved in DeltaSharedTableLoader
    assertHttpError(
      url = requestPath("/shares/share2/schemas/default/tables/table2/query"),
      method = "POST",
      data = Some("""{"timestamp": "abc"}"""),
      expectedErrorCode = 400,
      expectedErrorMessage = "Reading table by version or timestamp is not supported because change data feed is not enabled on table: share2.default.table2"
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
        id = "9f1a49539c5cffe1ea7f9e055d5c003c",
        partitionValues = Map("date" -> "2021-04-28"),
        size = 573,
        stats = """{"numRecords":1,"minValues":{"eventTime":"2021-04-28T23:33:57.955Z"},"maxValues":{"eventTime":"2021-04-28T23:33:57.955Z"},"nullCount":{"eventTime":0}}"""
      ),
      AddFile(
        url = actualFiles(1).url,
        id = "cd2209b32f5ed5305922dd50f5908a75",
        partitionValues = Map("date" -> "2021-04-28"),
        size = 573,
        stats = """{"numRecords":1,"minValues":{"eventTime":"2021-04-28T23:33:48.719Z"},"maxValues":{"eventTime":"2021-04-28T23:33:48.719Z"},"nullCount":{"eventTime":0}}"""
      )
    )
    assert(expectedFiles == actualFiles.toList)
    verifyPreSignedUrl(actualFiles(0).url, 573)
    verifyPreSignedUrl(actualFiles(1).url, 573)
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
        id = "db213271abffec6fd6c7fc2aad9d4b3f",
        partitionValues = Map("date" -> "2021-04-28"),
        size = 778,
        stats = """{"numRecords":1,"minValues":{"eventTime":"2021-04-28T23:36:51.945Z","type":"bar"},"maxValues":{"eventTime":"2021-04-28T23:36:51.945Z","type":"bar"},"nullCount":{"eventTime":0,"type":0}}"""
      ),
      AddFile(
        url = actualFiles(1).url,
        id = "f1f8be229d8b18eb6d6a34255f2d7089",
        partitionValues = Map("date" -> "2021-04-28"),
        size = 778,
        stats = """{"numRecords":1,"minValues":{"eventTime":"2021-04-28T23:36:47.599Z","type":"foo"},"maxValues":{"eventTime":"2021-04-28T23:36:47.599Z","type":"foo"},"nullCount":{"eventTime":0,"type":0}}"""
      ),
      AddFile(
        url = actualFiles(2).url,
        id = "a892a55d770ee70b34ffb2ebf7dc2fd0",
        partitionValues = Map("date" -> "2021-04-28"),
        size = 573,
        stats = """{"numRecords":1,"minValues":{"eventTime":"2021-04-28T23:35:53.156Z"},"maxValues":{"eventTime":"2021-04-28T23:35:53.156Z"},"nullCount":{"eventTime":0}}"""
      )
    )
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
    val response = readNDJson(requestPath("/shares/share1/schemas/default/tables/cdf_table_cdf_enabled/metadata"), expectedTableVersion = Some(5))
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
    val response = readNDJson(requestPath("/shares/share1/schemas/default/tables/cdf_table_cdf_enabled/query"), Some("POST"), Some(p), Some(1))
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
        id = "60d0cf57f3e4367db154aa2c36152a1f",
        partitionValues = Map.empty,
        size = 1030,
        stats = """{"numRecords":1,"minValues":{"name":"1","age":1,"birthday":"2020-01-01"},"maxValues":{"name":"1","age":1,"birthday":"2020-01-01"},"nullCount":{"name":0,"age":0,"birthday":0}}"""
      ),
      AddFile(
        url = actualFiles(1).url,
        id = "d7ed708546dd70fdff9191b3e3d6448b",
        partitionValues = Map.empty,
        size = 1030,
        stats = """{"numRecords":1,"minValues":{"name":"3","age":3,"birthday":"2020-01-01"},"maxValues":{"name":"3","age":3,"birthday":"2020-01-01"},"nullCount":{"name":0,"age":0,"birthday":0}}"""
      ),
      AddFile(
        url = actualFiles(2).url,
        id = "a6dc5694a4ebcc9a067b19c348526ad6",
        partitionValues = Map.empty,
        size = 1030,
        stats = """{"numRecords":1,"minValues":{"name":"2","age":2,"birthday":"2020-01-01"},"maxValues":{"name":"2","age":2,"birthday":"2020-01-01"},"nullCount":{"name":0,"age":0,"birthday":0}}"""
      )
    )
    assert(expectedFiles == actualFiles.toList)
    verifyPreSignedUrl(actualFiles(0).url, 1030)
    verifyPreSignedUrl(actualFiles(1).url, 1030)
    verifyPreSignedUrl(actualFiles(2).url, 1030)
  }

  integrationTest("cdf_table_cdf_enabled - exceptions") {
    // only one of version and timestamp is supported
    assertHttpError(
      url = requestPath("/shares/share1/schemas/default/tables/cdf_table_cdf_enabled/query"),
      method = "POST",
      data = Some("""{"timestamp": "abc", "version": "3"}"""),
      expectedErrorCode = 400,
      expectedErrorMessage = "Please only provide one of: version,timestamp,startingVersion"
    )

    // timestamp before the earliest version
    assertHttpError(
      url = requestPath("/shares/share1/schemas/default/tables/cdf_table_cdf_enabled/query"),
      method = "POST",
      data = Some("""{"timestamp": "2000-01-01 00:00:00"}"""),
      expectedErrorCode = 400,
      expectedErrorMessage = "The provided timestamp (2000-01-01 00:00:00.0) is before the earliest version"
    )

    // timestamp after the latest version
    assertHttpError(
      url = requestPath("/shares/share1/schemas/default/tables/cdf_table_cdf_enabled/query"),
      method = "POST",
      data = Some("""{"timestamp": "9999-01-01 00:00:00"}"""),
      expectedErrorCode = 400,
      expectedErrorMessage = "The provided timestamp (9999-01-01 00:00:00.0) is after the latest version available"
    )
  }

  integrationTest("cdf_table_cdf_enabled - timestamp on version 1 - /shares/{share}/schemas/{schema}/tables/{table}/query") {
    // 1651272635000, PST: 2022-04-29 15:50:35.0 -> version 1
    val tsStr = new Timestamp(1651272635000L).toString
    val p =
      s"""
        |{
        | "timestamp": "$tsStr"
        |}
        |""".stripMargin
    val response = readNDJson(requestPath("/shares/share1/schemas/default/tables/cdf_table_cdf_enabled/query"), Some("POST"), Some(p), Some(1))
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
        id = "60d0cf57f3e4367db154aa2c36152a1f",
        partitionValues = Map.empty,
        size = 1030,
        stats = """{"numRecords":1,"minValues":{"name":"1","age":1,"birthday":"2020-01-01"},"maxValues":{"name":"1","age":1,"birthday":"2020-01-01"},"nullCount":{"name":0,"age":0,"birthday":0}}"""
      ),
      AddFile(
        url = actualFiles(1).url,
        id = "d7ed708546dd70fdff9191b3e3d6448b",
        partitionValues = Map.empty,
        size = 1030,
        stats = """{"numRecords":1,"minValues":{"name":"3","age":3,"birthday":"2020-01-01"},"maxValues":{"name":"3","age":3,"birthday":"2020-01-01"},"nullCount":{"name":0,"age":0,"birthday":0}}"""
      ),
      AddFile(
        url = actualFiles(2).url,
        id = "a6dc5694a4ebcc9a067b19c348526ad6",
        partitionValues = Map.empty,
        size = 1030,
        stats = """{"numRecords":1,"minValues":{"name":"2","age":2,"birthday":"2020-01-01"},"maxValues":{"name":"2","age":2,"birthday":"2020-01-01"},"nullCount":{"name":0,"age":0,"birthday":0}}"""
      )
    )
    assert(expectedFiles == actualFiles.toList)
    verifyPreSignedUrl(actualFiles(0).url, 1030)
    verifyPreSignedUrl(actualFiles(1).url, 1030)
    verifyPreSignedUrl(actualFiles(2).url, 1030)
  }

  integrationTest("cdf_table_cdf_enabled_changes: query table changes") {
    val response = readNDJson(requestPath("/shares/share1/schemas/default/tables/cdf_table_cdf_enabled/changes?startingVersion=0&endingVersion=3"), Some("GET"), None, None)
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
    assert(files.size == 5)
    verifyAddCDCFile(
      files(0),
      size = 1301,
      partitionValues = Map.empty,
      version = 2,
      timestamp = 1651272655000L
    )
    verifyAddCDCFile(
      files(1),
      size = 1416,
      partitionValues = Map.empty,
      version = 3,
      timestamp = 1651272660000L
    )
    verifyAddFile(
      files(2),
      size = 1030,
      stats =
        """{"numRecords":1,"minValues":{"name":"1","age":1,"birthday":"2020-01-01"},"maxValues":{"name":"1","age":1,"birthday":"2020-01-01"},"nullCount":{"name":0,"age":0,"birthday":0}}""",
      partitionValues = Map.empty,
      version = 1,
      timestamp = 1651272635000L
    )
    verifyAddFile(
      files(3),
      size = 1030,
      stats =
        """{"numRecords":1,"minValues":{"name":"2","age":2,"birthday":"2020-01-01"},"maxValues":{"name":"2","age":2,"birthday":"2020-01-01"},"nullCount":{"name":0,"age":0,"birthday":0}}""",
      partitionValues = Map.empty,
      version = 1,
      timestamp = 1651272635000L
    )
    verifyAddFile(
      files(4),
      size = 1030,
      stats =
        """{"numRecords":1,"minValues":{"name":"3","age":3,"birthday":"2020-01-01"},"maxValues":{"name":"3","age":3,"birthday":"2020-01-01"},"nullCount":{"name":0,"age":0,"birthday":0}}""",
      partitionValues = Map.empty,
      version = 1,
      timestamp = 1651272635000L
    )
  }


  integrationTest("cdf_table_cdf_enabled_changes: timestamp works") {
    // 1651272616000, PST: 2022-04-29 15:50:16.0 -> version 0
    val startStr = URLEncoder.encode(new Timestamp(1651272616000L).toString)
    // 1651272660000, PST: 2022-04-29 15:51:00.0 -> version 3
    val endStr = URLEncoder.encode(new Timestamp(1651272660000L).toString)

    val response = readNDJson(requestPath(s"/shares/share1/schemas/default/tables/cdf_table_cdf_enabled/changes?startingTimestamp=${startStr}&endingTimestamp=${endStr}"), Some("GET"), None, None)
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
    assert(files.size == 5)
  }

  integrationTest("cdf_table_with_partition: query table changes") {
    val response = readNDJson(requestPath("/shares/share1/schemas/default/tables/cdf_table_with_partition/changes?startingVersion=1&endingVersion=3"), Some("GET"), None, None)
    val lines = response.split("\n")
    val files = lines.drop(2)
    assert(files.size == 6)
    // In version 2, birthday is updated from 2020-01-01 to 2020-02-02 for one row, which result in
    // 2 cdc files below.
    verifyAddCDCFile(
      files(0),
      size = 1125,
      partitionValues = Map("birthday" -> "2020-01-01"),
      version = 2,
      timestamp = 1651614986000L
    )
    verifyAddCDCFile(
      files(1),
      size = 1132,
      partitionValues = Map("birthday" -> "2020-02-02"),
      version = 2,
      timestamp = 1651614986000L
    )
    verifyAddFile(
      files(2),
      size = 791,
      stats =
        """{"numRecords":1,"minValues":{"name":"1","age":1},"maxValues":{"name":"1","age":1},"nullCount":{"name":0,"age":0}}""",
      partitionValues = Map("birthday" -> "2020-01-01"),
      version = 1,
      timestamp = 1651614980000L
    )
    verifyAddFile(
      files(3),
      size = 791,
      stats =
        """{"numRecords":1,"minValues":{"name":"2","age":2},"maxValues":{"name":"2","age":2},"nullCount":{"name":0,"age":0}}""",
      partitionValues = Map("birthday" -> "2020-01-01"),
      version = 1,
      timestamp = 1651614980000L
    )
    verifyAddFile(
      files(4),
      size = 791,
      stats =
        """{"numRecords":1,"minValues":{"name":"3","age":3},"maxValues":{"name":"3","age":3},"nullCount":{"name":0,"age":0}}""",
      partitionValues = Map("birthday" -> "2020-03-03"),
      version = 1,
      timestamp = 1651614980000L
    )
    verifyRemove(
      files(5),
      size = 791,
      partitionValues = Map("birthday" -> "2020-03-03"),
      version = 3,
      timestamp = 1651614994000L
    )
  }

  private def verifyAddFile(
      actionStr: String,
      size: Long,
      stats: String,
      partitionValues: Map[String, String],
      version: Long,
      timestamp: Long): Unit = {
    assert(actionStr.startsWith("{\"add\":{"))
    val addFile = JsonUtils.fromJson[SingleAction](actionStr).add
    assert(addFile.size == size)
    assert(addFile.stats == stats)
    assert(addFile.partitionValues == partitionValues)
    assert(addFile.version == version)
    assert(addFile.timestamp == timestamp)
    verifyPreSignedUrl(addFile.url, size.toInt)
  }

  private def verifyAddCDCFile(
      actionStr: String,
      size: Long,
      partitionValues: Map[String, String],
      version: Long,
      timestamp: Long): Unit = {
    assert(actionStr.startsWith("{\"cdf\":{"))
    val addCDCFile = JsonUtils.fromJson[SingleAction](actionStr).cdf
    assert(addCDCFile.size == size)
    assert(addCDCFile.partitionValues == partitionValues)
    assert(addCDCFile.version == version)
    assert(addCDCFile.timestamp == timestamp)
    verifyPreSignedUrl(addCDCFile.url, size.toInt)
  }

  private def verifyRemove(
      actionStr: String,
      size: Long,
      partitionValues: Map[String, String],
      version: Long,
      timestamp: Long): Unit = {
    assert(actionStr.startsWith("{\"remove\":{"))
    val removeFile = JsonUtils.fromJson[SingleAction](actionStr).remove
    assert(removeFile.size == size)
    assert(removeFile.partitionValues == partitionValues)
    assert(removeFile.version == version)
    assert(removeFile.timestamp == timestamp)
    verifyPreSignedUrl(removeFile.url, size.toInt)
  }

  def assertHttpError(
    url: String,
    method: String,
    data: Option[String],
    expectedErrorCode: Int,
    expectedErrorMessage: String): Unit = {
    val connection = new URL(url).openConnection().asInstanceOf[HttpsURLConnection]
    connection.setRequestProperty("Authorization", s"Bearer ${TestResource.testAuthorizationToken}")
    connection.setRequestMethod(method)
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
    val e = intercept[IOException] {
      connection.getInputStream()
    }
    assert(e.getMessage.contains(s"Server returned HTTP response code: $expectedErrorCode"))
    assert(IOUtils.toString(connection.getErrorStream()).contains(expectedErrorMessage))
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

  integrationTest("version negative") {
    assertHttpError(
      url = requestPath("/shares/share1/schemas/default/tables/table1/query"),
      method = "POST",
      data = Some("""
        {"version": -2}
      """),
      expectedErrorCode = 400,
      expectedErrorMessage = "table version cannot be negative"
    )
  }

   integrationTest("version needs to be numeric") {
    assertHttpError(
      url = requestPath("/shares/share1/schemas/default/tables/table1/query"),
      method = "POST",
      data = Some("""
        {"version": "x3"}
      """),
      expectedErrorCode = 400,
      expectedErrorMessage = "Not a numeric value: x3"
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
      url = requestPath("/shares/share1/schemas/default/tables/cdf_table_cdf_enabled/changes?startingTimestamp=2000-01-01%2000:00:00"),
      method = "GET",
      data = None,
      expectedErrorCode = 400,
      expectedErrorMessage = "The provided timestamp (2000-01-01 00:00:00.0) is before the earliest version available"
    )

    assertHttpError(
      url = requestPath("/shares/share1/schemas/default/tables/cdf_table_cdf_enabled/changes?startingTimestamp=9999-01-01%2000:00:00"),
      method = "GET",
      data = None,
      expectedErrorCode = 400,
      expectedErrorMessage = "The provided timestamp (9999-01-01 00:00:00.0) is after the latest version available"
    )

    assertHttpError(
      url = requestPath("/shares/share1/schemas/default/tables/cdf_table_cdf_enabled/changes"),
      method = "GET",
      data = None,
      expectedErrorCode = 400,
      expectedErrorMessage = "No startingVersion or startingTimestamp provided for CDF read"
    )

    assertHttpError(
      url = requestPath("/shares/share1/schemas/default/tables/cdf_table_cdf_enabled/changes?startingVersion=1&startingTimestamp=2022-02-02%2000:00:00"),
      method = "GET",
      data = None,
      expectedErrorCode = 400,
      expectedErrorMessage = "Multiple starting arguments provided for CDF read"
    )

    assertHttpError(
      url = requestPath("/shares/share1/schemas/default/tables/cdf_table_cdf_enabled/changes?startingVersion=1&endingVersion=3&endingTimestamp=randomString"),
      method = "GET",
      data = None,
      expectedErrorCode = 400,
      expectedErrorMessage = "Multiple ending arguments provided for CDF read"
    )

    assertHttpError(
      url = requestPath("/shares/share1/schemas/default/tables/cdf_table_cdf_enabled/changes?startingTimestamp=2022-04-29"),
      method = "GET",
      data = None,
      expectedErrorCode = 400,
      expectedErrorMessage = "Invalid startingTimestamp"
    )

    assertHttpError(
      url = requestPath("/shares/share1/schemas/default/tables/cdf_table_cdf_enabled/changes?startingVersion=2&endingVersion=1"),
      method = "GET",
      data = None,
      expectedErrorCode = 400,
      expectedErrorMessage = "CDF range from start 2 to end 1 was invalid. End cannot be before start"
    )

    assertHttpError(
      url = requestPath("/shares/share1/schemas/default/tables/cdf_table_cdf_enabled/changes?startingVersion=6"),
      method = "GET",
      data = None,
      expectedErrorCode = 400,
      expectedErrorMessage = "Provided Start version(6) for reading change data is invalid. Start version cannot be greater than the latest version of the table(5)"
    )

    assertHttpError(
      url = requestPath("/shares/share1/schemas/default/tables/cdf_table_cdf_enabled/changes?startingVersion=0&endingVersion=5"),
      method = "GET",
      data = None,
      expectedErrorCode = 400,
      expectedErrorMessage = "Error getting change data for range [0, 5] as change data was not recorded for version [4]"
    )

    assertHttpError(
      url = requestPath("/shares/share1/schemas/default/tables/cdf_table_cdf_enabled/changes?startingVersion=4"),
      method = "GET",
      data = None,
      expectedErrorCode = 400,
      expectedErrorMessage = "Error getting change data for range [4, 5] as change data was not recorded for version [4]"
    )
  }

  integrationTest("cdf_table_with_partition - exceptions") {
    assertHttpError(
      url = requestPath("/shares/share1/schemas/default/tables/cdf_table_with_partition/changes?startingVersion=0"),
      method = "GET",
      data = None,
      expectedErrorCode = 400,
      expectedErrorMessage = "You can only query table changes since version 1"
    )

    assertHttpError(
      url = requestPath("/shares/share1/schemas/default/tables/cdf_table_with_partition/query"),
      method = "POST",
      data = Some("""
        {"version": "0"}
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
