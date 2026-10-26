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

package io.delta.sharing.server.credential.azure

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream, OutputStream}
import java.net.{HttpURLConnection, URI, URL, URLDecoder}
import java.nio.charset.StandardCharsets.UTF_8
import java.time.OffsetDateTime
import java.util.Base64
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

import scala.collection.mutable.ArrayBuffer
import scala.xml.XML

import com.azure.storage.file.datalake.models.UserDelegationKey
import org.apache.hadoop.conf.Configuration
import org.scalatest.FunSuite

import io.delta.sharing.server.common.AzureUserDelegationSasGenerator
import io.delta.sharing.server.credential.{CloudCredentialVendor, CredentialContext, Privilege}

class AzureCredentialVendorSuite extends FunSuite {

  test("parseLocation abfss with userInfo") {
    val uri = new URI("abfss://mycontainer@myaccount.dfs.core.windows.net/table/path")
    val parts = AzureCredentialVendor.parseLocation(uri)
    assert(parts.accountNameFull == "myaccount.dfs.core.windows.net")
    assert(parts.accountShort == "myaccount")
    assert(parts.container == "mycontainer")
    assert(parts.pathPrefix == "table/path")
    assert(parts.endpointSuffix == "core.windows.net")
  }

  test("parseLocation wasbs with userInfo") {
    val uri = new URI("wasbs://container@account.blob.core.windows.net/foo/bar")
    val parts = AzureCredentialVendor.parseLocation(uri)
    assert(parts.accountNameFull == "account.blob.core.windows.net")
    assert(parts.accountShort == "account")
    assert(parts.container == "container")
    assert(parts.pathPrefix == "foo/bar")
    assert(parts.endpointSuffix == "core.windows.net")
  }

  test("parseLocation abfss path-only container") {
    val uri = new URI("abfss://account.dfs.core.windows.net/container/table/path")
    val parts = AzureCredentialVendor.parseLocation(uri)
    assert(parts.accountShort == "account")
    assert(parts.container == "container")
    assert(parts.pathPrefix == "table/path")
  }

  test("parseLocation throws for non-Azure host") {
    val uri = new URI("abfss://account.other.net/container/path")
    intercept[IllegalArgumentException] {
      AzureCredentialVendor.parseLocation(uri)
    }
  }

  test("parseTenantFromEndpoint") {
    assert(AzureCredentialVendor.parseTenantFromEndpoint(null).isEmpty)
    assert(AzureCredentialVendor.parseTenantFromEndpoint("").isEmpty)
    assert(AzureCredentialVendor.parseTenantFromEndpoint(
      "https://login.microsoftonline.com/tenant-id-123/oauth2/v2.0/token")
      == Some("tenant-id-123"))
    assert(AzureCredentialVendor.parseTenantFromEndpoint(
      "https://other.com/foo").isEmpty)
  }

  test("getAadConfig from delta.sharing.azure keys") {
    val conf = new Configuration()
    conf.set(AzureCredentialVendor.CONF_TENANT_ID, "tenant1")
    conf.set(AzureCredentialVendor.CONF_CLIENT_ID, "client1")
    conf.set(AzureCredentialVendor.CONF_CLIENT_SECRET, "secret1")
    val result = AzureCredentialVendor.getAadConfig(conf, "myaccount.dfs.core.windows.net")
    assert(result == Some(("tenant1", "client1", "secret1")))
  }

  test("getAadConfig empty when key missing") {
    val conf = new Configuration()
    conf.set(AzureCredentialVendor.CONF_TENANT_ID, "tenant1")
    assert(AzureCredentialVendor.getAadConfig(conf, "account.dfs.core.windows.net").isEmpty)
  }

  test("getAadConfig account-scoped Hadoop OAuth keys") {
    val conf = new Configuration()
    val account = "myaccount.dfs.core.windows.net"
    conf.set(s"${AzureCredentialVendor.HADOOP_OAUTH_CLIENT_ID}.$account", "cid")
    conf.set(s"${AzureCredentialVendor.HADOOP_OAUTH_CLIENT_SECRET}.$account", "csecret")
    conf.set(s"${AzureCredentialVendor.HADOOP_OAUTH_CLIENT_ENDPOINT}.$account",
      "https://login.microsoftonline.com/tenant-abc/oauth2/v2.0/token")
    val result = AzureCredentialVendor.getAadConfig(conf, account)
    assert(result == Some(("tenant-abc", "cid", "csecret")))
  }

  private val testKey = Base64.getEncoder.encodeToString(Array.fill[Byte](32)(1))

  private def sasParameters(token: String): Map[String, String] = {
    token.split("&").map { parameter =>
      val pair = parameter.split("=", 2)
      pair(0) -> URLDecoder.decode(pair(1), "UTF-8")
    }.toMap
  }

  private def assertSignedPath(
      parameters: Map[String, String],
      path: String,
      delegation: Boolean): Unit = {
    val canonicalName = "/blob/account/container" + (if (path.isEmpty) "" else "/" + path)
    val keyFields = if (delegation) {
      Seq("skoid", "sktid", "skt", "ske", "sks", "skv", "saoid", "suoid", "scid")
    } else {
      Seq("si")
    }
    val fields = Seq("sp", "st", "se") ++ Seq("canonical") ++ keyFields ++
      Seq("sip", "spr", "sv", "sr", "snapshot", "ses", "rscc", "rscd", "rsce", "rscl", "rsct")
    val stringToSign = fields.map {
      case "canonical" => canonicalName
      case field => parameters.getOrElse(field, "")
    }.mkString("\n")
    val mac = Mac.getInstance("HmacSHA256")
    mac.init(new SecretKeySpec(Base64.getDecoder.decode(testKey), "HmacSHA256"))
    val expected = Base64.getEncoder.encodeToString(mac.doFinal(stringToSign.getBytes(UTF_8)))
    assert(parameters("sig") == expected, s"Signature must bind the requested path: $path")
    assert(parameters("sp") == "rl")
    assert(parameters("spr") == "https")
    assert(parameters("sr") == (if (path.isEmpty) "c" else "d"))
    if (path.nonEmpty) {
      assert(parameters("sdd") == path.split("/").length.toString)
    }
  }

  test("account-key vending signs each table directory for ABFS and WASB locations") {
    for {
      scheme <- Seq("abfs", "abfss", "wasb", "wasbs")
      path <- Seq("shared/table", "shared/table/", "shared/other", "shared/table%20name")
    } {
      val host = if (scheme.startsWith("abfs")) "dfs" else "blob"
      val conf = new Configuration(false)
      conf.set(s"fs.azure.account.key.account.$host.core.windows.net", testKey)
      val location = new URI(s"$scheme://container@account.$host.core.windows.net/$path")
      val context = CredentialContext.create(location, Set(Privilege.SELECT))
      val credentials = new CloudCredentialVendor(conf).vendCredential(context, 3600)
      assert(credentials.location == location.toString)
      val parameters = sasParameters(credentials.azureUserDelegationSas.sasToken)
      assertSignedPath(parameters, location.getPath.stripPrefix("/").stripSuffix("/"), false)
    }
  }

  test("container-root vending signs only the requested container") {
    val conf = new Configuration(false)
    conf.set("fs.azure.account.key.account.dfs.core.windows.net", testKey)
    val context = CredentialContext.create(
      new URI("abfss://container@account.dfs.core.windows.net/"), Set(Privilege.SELECT))
    val credentials = new CloudCredentialVendor(conf).vendCredential(context, 3600)
    assertSignedPath(sasParameters(credentials.azureUserDelegationSas.sasToken), "", false)
  }

  test("user-delegation signing binds directory paths and depth without a live tenant") {
    val start = OffsetDateTime.parse("2026-01-01T00:00:00Z")
    val expiry = start.plusHours(1)
    val key = new UserDelegationKey()
      .setSignedObjectId("test-object-id")
      .setSignedTenantId("test-tenant-id")
      .setSignedStart(start)
      .setSignedExpiry(expiry)
      .setSignedService("b")
      .setSignedVersion("2023-11-03")
      .setValue(testKey)
    for (path <- Seq("shared/table", "shared/other", "shared/table name", "")) {
      val token = AzureUserDelegationSasGenerator.signUserDelegationSas(
        "account", "container", path, key, expiry)
      assertSignedPath(sasParameters(token), path, true)
    }
  }


  test("ambiguous Azure directory paths are rejected before credentials are signed") {
    for (path <- Seq("shared/../table", "shared/./table", "shared//table", "shared/%2e%2e/table",
        "shared/table%5Cchild")) {
      intercept[IllegalArgumentException] {
        AzureCredentialVendor.parseLocation(
          new URI(s"abfss://container@account.dfs.core.windows.net/$path"))
      }
    }
  }


  // Substitute only the two remote HTTP responses; the full credential-vending path runs.
  private class CredentialConnection(url: URL, status: Int, response: String)
      extends HttpURLConnection(url) {
    val requestBody = new ByteArrayOutputStream()
    var disconnected = false
    override def connect(): Unit = ()
    override def disconnect(): Unit = { disconnected = true }
    override def usingProxy(): Boolean = false
    override def getOutputStream: OutputStream = requestBody
    override def getInputStream: InputStream = new ByteArrayInputStream(response.getBytes(UTF_8))
    override def getResponseCode: Int = status
  }

  private def aadConfiguration(): Configuration = {
    val conf = new Configuration(false)
    conf.set(AzureCredentialVendor.CONF_TENANT_ID, "test-tenant")
    conf.set(AzureCredentialVendor.CONF_CLIENT_ID, "test-client")
    conf.set(AzureCredentialVendor.CONF_CLIENT_SECRET, "test-secret&+")
    conf
  }

  private def delegationKeyXml: String = {
    val now = OffsetDateTime.now()
    s"""<UserDelegationKey>
       |<SignedOid>test-object-id</SignedOid><SignedTid>test-tenant-id</SignedTid>
       |<SignedStart>${now.minusMinutes(5)}</SignedStart>
       |<SignedExpiry>${now.plusHours(2)}</SignedExpiry>
       |<SignedService>b</SignedService><SignedVersion>2023-11-03</SignedVersion>
       |<Value>$testKey</Value></UserDelegationKey>""".stripMargin
  }

  test("AAD vending fetches OAuth and delegation key responses and signs the requested directory") {
    val requests = ArrayBuffer.empty[CredentialConnection]
    val factory: URL => HttpURLConnection = url => {
      val response = if (url.getHost == "login.microsoftonline.com") {
        """{"access_token":"test-aad-token","expires_in":3600}"""
      } else {
        assert(url.toString ==
          "https://account.blob.core.windows.net/?restype=service&comp=userdelegationkey")
        delegationKeyXml
      }
      val connection = new CredentialConnection(url, 200, response)
      requests += connection
      connection
    }
    val conf = aadConfiguration()
    // AAD must not try to load a shared key, even when an invalid one is configured.
    conf.set("fs.azure.account.key.account.dfs.core.windows.net", "not-a-shared-key")
    val context = CredentialContext.create(
      new URI("abfss://container@account.dfs.core.windows.net/shared/table/"),
      Set(Privilege.SELECT))
    val credentials = new AzureCredentialVendor(conf, factory).vendAzureCredential(context, 3600)
    assertSignedPath(
      sasParameters(credentials.azureUserDelegationSas.sasToken), "shared/table", true)
    assert(requests.size == 2)
    assert(requests.head.getURL.toString ==
      "https://login.microsoftonline.com/test-tenant/oauth2/v2.0/token")
    val oauthBody = requests.head.requestBody.toString("UTF-8").split("&").map { pair =>
      val fields = pair.split("=", 2)
      fields(0) -> URLDecoder.decode(fields(1), "UTF-8")
    }.toMap
    assert(oauthBody("grant_type") == "client_credentials")
    assert(oauthBody("client_id") == "test-client")
    assert(oauthBody("client_secret") == "test-secret&+")
    assert(oauthBody("scope") == "https://storage.azure.com/.default")
    val keyRequest = requests(1)
    assert(keyRequest.getRequestProperty("Authorization") == "Bearer test-aad-token")
    assert(keyRequest.getRequestProperty("x-ms-version") == "2023-11-03")
    assert(keyRequest.getRequestProperty("x-ms-date").nonEmpty)
    val keyInfo = XML.loadString(keyRequest.requestBody.toString("UTF-8"))
    val start = OffsetDateTime.parse((keyInfo \ "Start").text)
    val expiry = OffsetDateTime.parse((keyInfo \ "Expiry").text)
    assert(expiry == start.plusSeconds(3600))
    requests.foreach { connection =>
      assert(connection.getRequestMethod == "POST")
      assert(!connection.getInstanceFollowRedirects)
      assert(connection.getConnectTimeout == 10000)
      assert(connection.getReadTimeout == 10000)
      assert(connection.disconnected)
    }
  }

  test("AAD failures neither fall back to shared keys nor expose response bodies") {
    for (failureHost <- Seq("login.microsoftonline.com", "account.blob.core.windows.net")) {
      val requests = ArrayBuffer.empty[CredentialConnection]
      val factory: URL => HttpURLConnection = url => {
        val failed = url.getHost == failureHost
        val response = if (failed) "private-error-details" else {
          """{"access_token":"test-aad-token","expires_in":3600}"""
        }
        val connection = new CredentialConnection(url, if (failed) 403 else 200, response)
        requests += connection
        connection
      }
      val conf = aadConfiguration()
      conf.set("fs.azure.account.key.account.dfs.core.windows.net", testKey)
      val context = CredentialContext.create(
        new URI("abfss://container@account.dfs.core.windows.net/shared/table"),
        Set(Privilege.SELECT))
      val error = intercept[RuntimeException] {
        new AzureCredentialVendor(conf, factory).vendAzureCredential(context, 3600)
      }
      assert(error.getCause.getMessage.contains("HTTP 403"))
      assert(!error.getCause.getMessage.contains("private-error-details"))
      assert(requests.size == (if (failureHost == "login.microsoftonline.com") 1 else 2))
      assert(requests.forall(_.disconnected))
    }
  }

  test("AAD vending rejects incomplete delegation keys and XML document type declarations") {
    for (response <- Seq("<UserDelegationKey/>", "<Unexpected/>",
        "<!DOCTYPE UserDelegationKey><UserDelegationKey/>")) {
      val factory: URL => HttpURLConnection = url => {
        val body = if (url.getHost == "login.microsoftonline.com") {
          """{"access_token":"test-aad-token","expires_in":3600}"""
        } else response
        new CredentialConnection(url, 200, body)
      }
      val context = CredentialContext.create(
        new URI("abfss://container@account.dfs.core.windows.net/shared/table"),
        Set(Privilege.SELECT))
      intercept[RuntimeException] {
        new AzureCredentialVendor(aadConfiguration(), factory).vendAzureCredential(context, 3600)
      }
    }
  }

}
