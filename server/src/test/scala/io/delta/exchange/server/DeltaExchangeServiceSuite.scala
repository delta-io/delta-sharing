package io.delta.exchange.server

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

  def readJson(url: String, token: String = fakeTokenForRecipient1): String = {
    readHttpContent(url, token, "application/json; charset=utf-8")
  }

  def readNDJson(url: String, token: String = fakeTokenForRecipient1): String = {
    readHttpContent(url, token, "application/x-ndjson; charset=utf-8")
  }

  def readHttpContent(url: String, token: String, expectedContentType: String): String = {
    val connection = new URL(url).openConnection()
    connection.setRequestProperty("Authorization", s"Bearer $token")
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
    content
  }

  def integrationTest(testName: String)(func: => Unit): Unit = {
    test(testName) {
      assume(sys.env.get("DELTA_EXCHANGE_TEST_AWS_ACCESS_KEY").nonEmpty)
      func
    }
  }

  integrationTest("recipient1: /shares") {
    val response = readJson(requestPath("/shares"))
    val expected = ListSharesResponse(
      Vector(Share().withName("share1"), Share().withName("share2")))
    assert(expected == JsonFormat.fromJsonString[ListSharesResponse](response))
  }

  integrationTest("recipient2: /shares") {
    val response = readJson(requestPath("/shares"), fakeTokenForRecipient2)
    val expected = ListSharesResponse(Vector(Share().withName("share1")))
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

  integrationTest("table1 - non partitioned - /shares/{share}/schemas/{schema}/tables/{table}/metadata") {
    val response = readNDJson(requestPath("/shares/share1/schemas/default/tables/table1/metadata"))
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
    val response = readNDJson(requestPath("/shares/share1/schemas/default/tables/table1/query"))
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
        path = actualFiles(0).path,
        partitionValues = Map.empty,
        size = 781,
        dataChange = false
      ),
      AddFile(
        path = actualFiles(1).path,
        partitionValues = Map.empty,
        size = 781,
        dataChange = false
      )
    )
    assert(expectedFiles == actualFiles.toList)
    assert(IOUtils.toByteArray(new URL(actualFiles(0).path)).size == 781)
    assert(IOUtils.toByteArray(new URL(actualFiles(1).path)).size == 781)
  }

  integrationTest("table2 - partitioned - /shares/{share}/schemas/{schema}/tables/{table}/metadata") {
    val response = readNDJson(requestPath("/shares/share2/schemas/default/tables/table2/metadata"))
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
    val response = readNDJson(requestPath("/shares/share2/schemas/default/tables/table2/query"))
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
        path = actualFiles(0).path,
        partitionValues = Map("date" -> "2021-04-28"),
        size = 573,
        dataChange = false
      ),
      AddFile(
        path = actualFiles(1).path,
        partitionValues = Map("date" -> "2021-04-28"),
        size = 573,
        dataChange = false
      )
    )
    assert(expectedFiles == actualFiles.toList)
    assert(IOUtils.toByteArray(new URL(actualFiles(0).path)).size == 573)
    assert(IOUtils.toByteArray(new URL(actualFiles(1).path)).size == 573)
  }

  integrationTest("table3 - different data file schemas - /shares/{share}/schemas/{schema}/tables/{table}/metadata") {
    val response = readNDJson(requestPath("/shares/share1/schemas/default/tables/table3/metadata"))
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
    val response = readNDJson(requestPath("/shares/share1/schemas/default/tables/table3/query"))
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
        path = actualFiles(0).path,
        partitionValues = Map("date" -> "2021-04-28"),
        size = 778,
        dataChange = false
      ),
      AddFile(
        path = actualFiles(1).path,
        partitionValues = Map("date" -> "2021-04-28"),
        size = 778,
        dataChange = false
      ),
      AddFile(
        path = actualFiles(2).path,
        partitionValues = Map("date" -> "2021-04-28"),
        size = 573,
        dataChange = false
      )
    )
    assert(expectedFiles == actualFiles.toList)
    assert(IOUtils.toByteArray(new URL(actualFiles(0).path)).size == 778)
    assert(IOUtils.toByteArray(new URL(actualFiles(1).path)).size == 778)
    assert(IOUtils.toByteArray(new URL(actualFiles(2).path)).size == 573)
  }
}
