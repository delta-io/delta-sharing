package io.delta.exchange.server

import java.io.IOException
import java.net.URL
import java.nio.file.Files

import javax.net.ssl.HostnameVerifier
import javax.net.ssl.HttpsURLConnection
import javax.net.ssl.SSLContext
import javax.net.ssl.SSLSession
import javax.net.ssl.TrustManager
import javax.net.ssl.X509TrustManager
import java.security.cert.X509Certificate

import scalapb.json4s.JsonFormat
import io.delta.exchange.protocol.{ListSchemasResponse, ListSharesResponse, ListTablesResponse, Schema, Share, Table}
import io.delta.exchange.server.model.{AddFile, Format, Metadata, Protocol, SingleAction}
import org.apache.commons.io.IOUtils
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class DeltaExchangeServiceSuite extends FunSuite with BeforeAndAfterAll {
  import DeltaExchangeService.requestPath
  import TestResource._

  private def enableUntrustedServer(): Unit = {
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

  override def beforeAll() {
    if (sys.env.get("DELTA_EXCHANGE_TEST_AWS_ACCESS_KEY").nonEmpty) {
      enableUntrustedServer()
      DeltaExchangeService.start().get()
    }
  }

  override def afterAll() {
    if (sys.env.get("DELTA_EXCHANGE_TEST_AWS_ACCESS_KEY").nonEmpty) {
      DeltaExchangeService.stop().get()
    }
  }

  def readJson(url: String, expectedTableVersion: Option[Long] = None): String = {
    readHttpContent(url, None, None, fakeTokenForRecipient1, expectedTableVersion, "application/json; charset=utf-8")
  }

  def readNDJson(url: String, method: Option[String] = None, data: Option[String] = None, expectedTableVersion: Option[Long] = None): String = {
    readHttpContent(url, method, data, fakeTokenForRecipient1, expectedTableVersion, "application/x-ndjson; charset=utf-8")
  }

  def readHttpContent(url: String, method: Option[String], data: Option[String] = None, token: String, expectedTableVersion: Option[Long] = None, expectedContentType: String): String = {
    val connection = new URL(url).openConnection().asInstanceOf[HttpsURLConnection]
    connection.setRequestProperty("Authorization", s"Bearer $token")
    method.foreach(connection.setRequestMethod)
    data.foreach { d =>
      connection.setDoOutput(true)
      connection.setRequestProperty("Content-Type", "application/json; charset=utf8")
      val output = connection.getOutputStream()
      try {
        output.write(d.getBytes("UTF-8"))
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
      assume(sys.env.get("DELTA_EXCHANGE_TEST_AWS_ACCESS_KEY").nonEmpty)
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

  integrationTest("table1 - head - /shares/{share}/schemas/{schema}/tables/{table}") {
    val url = requestPath("/shares/share1/schemas/default/tables/table1")
    val connection = new URL(url).openConnection().asInstanceOf[HttpsURLConnection]
    connection.setRequestMethod("HEAD")
    connection.setRequestProperty("Authorization", s"Bearer $fakeTokenForRecipient1")
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
    val actualFiles = files.map(f => JsonUtils.fromJson[SingleAction](f).add)
    assert(actualFiles.size == 2)
    // TODO verify stats
    val expectedFiles = Seq(
      AddFile(
        url = actualFiles(0).url,
        id = "part-00000-7d93fddb-fc8b-4922-9ac9-5eab51368afa-c000.snappy.parquet",
        partitionValues = Map.empty,
        size = 781,
        stats = """{"numRecords":1,"minValues":{"eventTime":"2021-04-28T06:32:22.421Z","date":"2021-04-28"},"maxValues":{"eventTime":"2021-04-28T06:32:22.421Z","date":"2021-04-28"},"nullCount":{"eventTime":0,"date":0}}""",
      ),
      AddFile(
        url = actualFiles(1).url,
        id = "part-00000-feb9b2f7-4f80-455c-860d-04530534753e-c000.snappy.parquet",
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
    val actualFiles = files.map(f => JsonUtils.fromJson[SingleAction](f).add)
    assert(actualFiles.size == 2)
    // TODO verify stats
    val expectedFiles = Seq(
      AddFile(
        url = actualFiles(0).url,
        id = "date=2021-04-28/part-00000-8b0086f2-7b27-4935-ac5a-8ed6215a6640.c000.snappy.parquet",
        partitionValues = Map("date" -> "2021-04-28"),
        size = 573,
        stats = """{"numRecords":1,"minValues":{"eventTime":"2021-04-28T23:33:57.955Z"},"maxValues":{"eventTime":"2021-04-28T23:33:57.955Z"},"nullCount":{"eventTime":0}}"""
      ),
      AddFile(
        url = actualFiles(1).url,
        id = "date=2021-04-28/part-00000-591723a8-6a27-4240-a90e-57426f4736d2.c000.snappy.parquet",
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
    val actualFiles = files.map(f => JsonUtils.fromJson[SingleAction](f).add)
    assert(actualFiles.size == 3)
    // TODO verify stats
    val expectedFiles = Seq(
      AddFile(
        url = actualFiles(0).url,
        id = "date=2021-04-28/part-00000-77c3cdf5-cce2-4c0a-ad13-9fad7b1ebce1.c000.snappy.parquet",
        partitionValues = Map("date" -> "2021-04-28"),
        size = 778,
        stats = """{"numRecords":1,"minValues":{"eventTime":"2021-04-28T23:36:51.945Z","type":"bar"},"maxValues":{"eventTime":"2021-04-28T23:36:51.945Z","type":"bar"},"nullCount":{"eventTime":0,"type":0}}"""
      ),
      AddFile(
        url = actualFiles(1).url,
        id = "date=2021-04-28/part-00000-e0a66ac8-57a9-44a3-b00f-d099ff34479d.c000.snappy.parquet",
        partitionValues = Map("date" -> "2021-04-28"),
        size = 778,
        stats = """{"numRecords":1,"minValues":{"eventTime":"2021-04-28T23:36:47.599Z","type":"foo"},"maxValues":{"eventTime":"2021-04-28T23:36:47.599Z","type":"foo"},"nullCount":{"eventTime":0,"type":0}}"""
      ),
      AddFile(
        url = actualFiles(2).url,
        id = "date=2021-04-28/part-00000-8bede38b-766a-4858-9454-4aa47631d0fe.c000.snappy.parquet",
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
