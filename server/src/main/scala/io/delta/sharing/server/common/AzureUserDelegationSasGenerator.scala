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

package io.delta.sharing.server.common

import java.io.{InputStream, OutputStreamWriter}
import java.net.{HttpURLConnection, URL}
import java.nio.charset.StandardCharsets.UTF_8
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import java.util.{Base64, Scanner}
import java.util.concurrent.TimeUnit.SECONDS
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import javax.xml.parsers.DocumentBuilderFactory

import org.xml.sax.InputSource

import io.delta.sharing.server.common.JsonUtils

/**
 * Generates Azure User Delegation SAS tokens via direct REST calls.
 * Avoids the Azure SDK BlobServiceClient HTTP pipeline which requires
 * Jackson 2.9+ and conflicts with the Jackson 2.6.x pin required
 * by delta-standalone.
 *
 * TODO: migrate to Azure Storage SDK (BlobServiceClient) when the
 * Jackson version constraint is resolved (see delta-io/delta#5598).
 * The SDK handles SAS version changes automatically.
 */
object AzureUserDelegationSasGenerator {

  val StorageScope = "https://storage.azure.com/.default"
  val AadTokenEndpointTemplate =
    "https://login.microsoftonline.com/%s/oauth2/v2.0/token"
  // SAS signature format is coupled to this version. Update the
  // string-to-sign in computeUserDelegationSas if changing.
  val StorageApiVersion = "2022-11-02"
  private val ISO = DateTimeFormatter.ofPattern(
    "yyyy-MM-dd'T'HH:mm:ss'Z'")
    .withZone(java.time.ZoneOffset.UTC)

  /** Fetch AAD access token using client_credentials grant. */
  def getAadToken(
      tenantId: String,
      clientId: String,
      clientSecret: String,
      scope: String = StorageScope): (String, Long) = {
    val url = new URL(AadTokenEndpointTemplate.format(tenantId))
    val body = Seq(
      "grant_type=client_credentials",
      s"client_id=${java.net.URLEncoder.encode(clientId, UTF_8.name)}",
      s"client_secret=${java.net.URLEncoder.encode(clientSecret, UTF_8.name)}",
      s"scope=${java.net.URLEncoder.encode(scope, UTF_8.name)}"
    ).mkString("&")
    val conn = url.openConnection().asInstanceOf[HttpURLConnection]
    conn.setRequestMethod("POST")
    conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded")
    conn.setDoOutput(true)
    conn.setConnectTimeout(10000)
    conn.setReadTimeout(10000)
    try {
      val writer = new OutputStreamWriter(conn.getOutputStream, UTF_8)
      writer.write(body)
      writer.close()
      val code = conn.getResponseCode
      val stream: InputStream =
        if (code >= 200 && code < 300) conn.getInputStream else conn.getErrorStream
      val response = new Scanner(stream, UTF_8.name).useDelimiter("\\A").next()
      if (code >= 300) {
        throw new RuntimeException(s"AAD token request failed ($code): $response")
      }
      val tree = JsonUtils.mapper.readTree(response)
      val accessToken = tree.get("access_token").asText
      val expiresIn = if (tree.has("expires_in")) tree.get("expires_in").asInt else 3600
      val expiryMillis = System.currentTimeMillis() + SECONDS.toMillis(expiresIn.toLong)
      (accessToken, expiryMillis)
    } finally {
      conn.disconnect()
    }
  }

  /**
   * Fetch an AAD token from the Azure IMDS managed identity endpoint.
   * @param clientId optional UAMI client ID; if None, uses SAMI.
   * @return (accessToken, expiryMillis)
   */
  def getManagedIdentityToken(
      clientId: Option[String]): (String, Long) = {
    val resource = "https://storage.azure.com/"
    val baseUrl =
      "http://169.254.169.254/metadata/identity/oauth2/token" +
        s"?api-version=2018-02-01&resource=$resource"
    val urlStr = clientId match {
      case Some(id) =>
        baseUrl + "&client_id=" +
          java.net.URLEncoder.encode(id, UTF_8.name)
      case None => baseUrl
    }
    val url = new URL(urlStr)
    val conn =
      url.openConnection().asInstanceOf[HttpURLConnection]
    conn.setRequestMethod("GET")
    conn.setRequestProperty("Metadata", "true")
    conn.setConnectTimeout(10000)
    conn.setReadTimeout(10000)
    try {
      val code = conn.getResponseCode
      val stream: InputStream =
        if (code >= 200 && code < 300) conn.getInputStream
        else conn.getErrorStream
      val response = new Scanner(stream, UTF_8.name)
        .useDelimiter("\\A").next()
      if (code >= 300) {
        throw new RuntimeException(
          s"IMDS token request failed ($code): $response")
      }
      val tree = JsonUtils.mapper.readTree(response)
      val accessToken = tree.get("access_token").asText
      val expiresIn = if (tree.has("expires_in")) {
        tree.get("expires_in").asText.toLong
      } else 3600L
      val expiryMillis =
        System.currentTimeMillis() + SECONDS.toMillis(expiresIn)
      (accessToken, expiryMillis)
    } finally {
      conn.disconnect()
    }
  }

  /** Parsed fields from Get User Delegation Key REST response. */
  case class UserDelegationKeyInfo(
      signedOid: String,
      signedTid: String,
      signedStart: String,
      signedExpiry: String,
      signedService: String,
      signedVersion: String,
      value: String)

  /**
   * Fetch a User Delegation Key via the Azure Blob REST API.
   * POST https://acct.blob.suffix/?restype=service&comp=userdelegationkey
   */
  def getUserDelegationKey(
      accountName: String,
      endpointSuffix: String,
      accessToken: String,
      keyStart: OffsetDateTime,
      keyExpiry: OffsetDateTime): UserDelegationKeyInfo = {
    val endpoint =
      s"https://$accountName.blob.$endpointSuffix" +
        "/?restype=service&comp=userdelegationkey"
    val startStr = keyStart.format(ISO)
    val expiryStr = keyExpiry.format(ISO)
    val xmlBody = Seq(
      "<?xml version=\"1.0\" encoding=\"utf-8\"?>",
      "<KeyInfo>",
      s"  <Start>$startStr</Start>",
      s"  <Expiry>$expiryStr</Expiry>",
      "</KeyInfo>"
    ).mkString("\n")

    val url = new URL(endpoint)
    val conn =
      url.openConnection().asInstanceOf[HttpURLConnection]
    conn.setRequestMethod("POST")
    conn.setRequestProperty(
      "Authorization", s"Bearer $accessToken")
    conn.setRequestProperty(
      "x-ms-version", StorageApiVersion)
    conn.setRequestProperty(
      "Content-Type", "application/xml")
    conn.setDoOutput(true)
    conn.setConnectTimeout(10000)
    conn.setReadTimeout(10000)
    try {
      val writer =
        new OutputStreamWriter(conn.getOutputStream, UTF_8)
      writer.write(xmlBody)
      writer.close()
      val code = conn.getResponseCode
      val stream: InputStream =
        if (code >= 200 && code < 300) conn.getInputStream
        else conn.getErrorStream
      val response = new Scanner(stream, UTF_8.name)
        .useDelimiter("\\A").next()
      if (code >= 300) {
        throw new RuntimeException(
          s"getUserDelegationKey failed ($code): $response")
      }
      parseUserDelegationKeyXml(response)
    } finally {
      conn.disconnect()
    }
  }

  private def parseUserDelegationKeyXml(
      xml: String): UserDelegationKeyInfo = {
    // Strip UTF-8 BOM if present (Azure REST responses
    // may include it)
    val clean = if (xml.charAt(0) == 0xFEFF) {
      xml.substring(1)
    } else xml
    val factory = DocumentBuilderFactory.newInstance()
    val builder = factory.newDocumentBuilder()
    val doc = builder.parse(
      new InputSource(new java.io.StringReader(clean)))
    def text(tag: String): String =
      doc.getElementsByTagName(tag).item(0).getTextContent
    UserDelegationKeyInfo(
      signedOid = text("SignedOid"),
      signedTid = text("SignedTid"),
      signedStart = text("SignedStart"),
      signedExpiry = text("SignedExpiry"),
      signedService = text("SignedService"),
      signedVersion = text("SignedVersion"),
      value = text("Value")
    )
  }

  /**
   * Compute a container-scoped User Delegation SAS locally.
   * Follows the Azure Storage SAS spec for version 2022-11-02.
   */
  def computeUserDelegationSas(
      accountName: String,
      container: String,
      key: UserDelegationKeyInfo,
      permissions: String,
      sasStart: OffsetDateTime,
      sasExpiry: OffsetDateTime): String = {
    val sv = StorageApiVersion
    val sr = "c"
    val sp = permissions
    val st = sasStart.format(ISO)
    val se = sasExpiry.format(ISO)
    val spr = "https"
    val cr = s"/blob/$accountName/$container"

    // String-to-sign per Azure docs for sv=2022-11-02
    val sts = Seq(
      sp, st, se, cr,
      key.signedOid, key.signedTid,
      key.signedStart, key.signedExpiry,
      key.signedService, key.signedVersion,
      "", "", "", "", spr, sv, sr,
      "", "", "", "", "", "", ""
    ).mkString("\n")

    val keyBytes = Base64.getDecoder.decode(key.value)
    val mac = Mac.getInstance("HmacSHA256")
    mac.init(new SecretKeySpec(keyBytes, "HmacSHA256"))
    val sig = Base64.getEncoder.encodeToString(
      mac.doFinal(sts.getBytes(UTF_8)))

    val e = java.net.URLEncoder.encode(_: String, UTF_8.name)
    Seq(
      s"sv=$sv", s"sr=$sr", s"sp=$sp",
      s"st=${e(st)}", s"se=${e(se)}", s"spr=$spr",
      s"skoid=${e(key.signedOid)}",
      s"sktid=${e(key.signedTid)}",
      s"skt=${e(key.signedStart)}",
      s"ske=${e(key.signedExpiry)}",
      s"sks=${key.signedService}",
      s"skv=${key.signedVersion}",
      s"sig=${e(sig)}"
    ).mkString("&")
  }

  /**
   * Generate a container-scoped User Delegation SAS from a raw
   * access token. Fetches the delegation key via REST and computes
   * the SAS locally (no Azure SDK HTTP pipeline).
   */
  def generateUserDelegationSasFromToken(
      accountName: String,
      endpointSuffix: String,
      container: String,
      accessToken: String,
      validitySeconds: Long): String = {
    val now = OffsetDateTime.now()
    val keyExpiry = now.plusHours(1)
    val key = getUserDelegationKey(
      accountName, endpointSuffix, accessToken,
      now, keyExpiry)
    val sasExpiry = now.plusSeconds(validitySeconds)
    computeUserDelegationSas(
      accountName, container, key, "r", now, sasExpiry)
  }

  /**
   * Generate a container-scoped User Delegation SAS using
   * client_credentials (service principal).
   */
  def generateUserDelegationSas(
      accountName: String,
      endpointSuffix: String,
      container: String,
      tenantId: String,
      clientId: String,
      clientSecret: String,
      validitySeconds: Long): String = {
    val (accessToken, _) =
      getAadToken(tenantId, clientId, clientSecret)
    generateUserDelegationSasFromToken(
      accountName, endpointSuffix, container,
      accessToken, validitySeconds)
  }
}
