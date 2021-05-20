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
import java.net.URL
import java.nio.charset.StandardCharsets.UTF_8
import java.security.cert.X509Certificate
import javax.net.ssl._

import scala.collection.mutable.ArrayBuffer

import com.linecorp.armeria.server.Server
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
    sys.env.get("AWS_ACCESS_KEY_ID").exists(_.length > 0)
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

  integrationTest("401 Unauthorized Error") {
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
      Vector(Share().withName("share1"), Share().withName("share2")))
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
    val expected = Seq(Share().withName("share1"), Share().withName("share2"))
    assert(expected == shares)
  }

  integrationTest("/shares/{share}/schemas") {
    val response = readJson(requestPath("/shares/share1/schemas"))
    val expected = ListSchemasResponse(
      Schema().withName("default").withShare("share1") :: Nil)
    assert(expected ==  JsonFormat.fromJsonString[ListSchemasResponse](response))
  }

  integrationTest("/shares/{share}/schemas/{schema}/tables") {
    val response = readJson(requestPath("/shares/share1/schemas/default/tables"))
    val expected = ListTablesResponse(
      Table().withName("table1").withSchema("default").withShare("share1") ::
        Table().withName("table3").withSchema("default").withShare("share1") :: Nil)
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
        Table().withName("table3").withSchema("default").withShare("share1") :: Nil
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
        |    "date = '2021-01-31'"
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
    assert(IOUtils.toByteArray(new URL(actualFiles(0).url)).size == 781)
    assert(IOUtils.toByteArray(new URL(actualFiles(1).url)).size == 781)
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

  integrationTest("table2 - partitioned - /shares/{share}/schemas/{schema}/tables/{table}/query") {
    val p =
      """
        |{
        |  "predicateHints": [
        |    "date = '2021-01-31'"
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
    assert(IOUtils.toByteArray(new URL(actualFiles(0).url)).size == 573)
    assert(IOUtils.toByteArray(new URL(actualFiles(1).url)).size == 573)
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
    assert(IOUtils.toByteArray(new URL(actualFiles(0).url)).size == 778)
    assert(IOUtils.toByteArray(new URL(actualFiles(1).url)).size == 778)
    assert(IOUtils.toByteArray(new URL(actualFiles(2).url)).size == 573)
  }
}
