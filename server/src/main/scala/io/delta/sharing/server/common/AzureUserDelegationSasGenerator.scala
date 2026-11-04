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

import java.io.{IOException, OutputStreamWriter, StringReader}
import java.net.{HttpURLConnection, URL}
import java.nio.charset.StandardCharsets.UTF_8
import java.time.{OffsetDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter
import java.util.Scanner
import java.util.concurrent.TimeUnit.SECONDS
import javax.xml.XMLConstants
import javax.xml.parsers.DocumentBuilderFactory

import com.azure.core.util.Context
import com.azure.storage.common.sas.SasProtocol
import com.azure.storage.file.datalake.implementation.util.DataLakeSasImplUtil
import com.azure.storage.file.datalake.models.UserDelegationKey
import com.azure.storage.file.datalake.sas.{DataLakeServiceSasSignatureValues, FileSystemSasPermission, PathSasPermission}
import org.xml.sax.InputSource

/** Fetches a delegation key through Azure REST and signs directory-scoped SAS locally. */
object AzureUserDelegationSasGenerator {

  val StorageScope = "https://storage.azure.com/.default"
  private val STORAGE_API_VERSION = "2023-11-03"
  val AadTokenEndpointTemplate = "https://login.microsoftonline.com/%s/oauth2/v2.0/token"

  private[server] def openConnection(url: URL): HttpURLConnection = {
    url.openConnection().asInstanceOf[HttpURLConnection]
  }

  private def post(
      url: URL,
      body: String,
      contentType: String,
      headers: Map[String, String],
      connectionFactory: URL => HttpURLConnection): String = {
    val conn = connectionFactory(url)
    conn.setInstanceFollowRedirects(false)
    conn.setRequestMethod("POST")
    conn.setRequestProperty("Content-Type", contentType)
    headers.foreach { case (name, value) => conn.setRequestProperty(name, value) }
    conn.setDoOutput(true)
    conn.setConnectTimeout(10000)
    conn.setReadTimeout(10000)
    try {
      val writer = new OutputStreamWriter(conn.getOutputStream, UTF_8)
      try writer.write(body) finally writer.close()
      val code = conn.getResponseCode
      if (code < 200 || code >= 300) {
        // Error bodies may contain credentials; expose only the status.
        throw new IOException(s"Azure credential request failed (HTTP $code)")
      }
      val scanner = new Scanner(conn.getInputStream, UTF_8.name).useDelimiter("\\A")
      try {
        if (!scanner.hasNext) throw new IOException("Azure credential response was empty")
        scanner.next()
      } finally {
        scanner.close()
      }
    } finally {
      conn.disconnect()
    }
  }

  /** Fetch AAD access token using client_credentials grant. */
  def getAadToken(
      tenantId: String,
      clientId: String,
      clientSecret: String,
      scope: String = StorageScope,
      connectionFactory: URL => HttpURLConnection = openConnection _): (String, Long) = {
    val url = new URL(AadTokenEndpointTemplate.format(tenantId))
    val body = Seq(
      "grant_type=client_credentials",
      s"client_id=${java.net.URLEncoder.encode(clientId, UTF_8.name)}",
      s"client_secret=${java.net.URLEncoder.encode(clientSecret, UTF_8.name)}",
      s"scope=${java.net.URLEncoder.encode(scope, UTF_8.name)}"
    ).mkString("&")
    val response = post(
      url, body, "application/x-www-form-urlencoded", Map.empty, connectionFactory)
    val tree = JsonUtils.mapper.readTree(response)
    val token = tree.get("access_token")
    if (token == null || !token.isTextual || token.asText().isEmpty) {
      throw new IOException("AAD response did not contain an access token")
    }
    val expiresIn = if (tree.has("expires_in")) tree.get("expires_in").asLong else 3600L
    (token.asText(), System.currentTimeMillis() + SECONDS.toMillis(expiresIn))
  }

  private def parseDelegationKey(response: String): UserDelegationKey = {
    val factory = DocumentBuilderFactory.newInstance()
    factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true)
    factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true)
    factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "")
    factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "")
    factory.setXIncludeAware(false)
    factory.setExpandEntityReferences(false)
    val root = factory.newDocumentBuilder()
      .parse(new InputSource(new StringReader(response))).getDocumentElement
    if (root.getTagName != "UserDelegationKey") {
      throw new IOException("Azure response did not contain a user delegation key")
    }
    def value(name: String): String = {
      val elements = root.getElementsByTagName(name)
      if (elements.getLength != 1 || elements.item(0).getTextContent.trim.isEmpty) {
        throw new IOException(s"Azure delegation key response has missing or duplicate $name")
      }
      elements.item(0).getTextContent.trim
    }
    new UserDelegationKey()
      .setSignedObjectId(value("SignedOid"))
      .setSignedTenantId(value("SignedTid"))
      .setSignedStart(OffsetDateTime.parse(value("SignedStart")))
      .setSignedExpiry(OffsetDateTime.parse(value("SignedExpiry")))
      .setSignedService(value("SignedService"))
      .setSignedVersion(value("SignedVersion"))
      .setValue(value("Value"))
  }

  /**
   * Generate a directory-scoped User Delegation SAS (container-scoped for a container root).
   * @param accountName Storage account name (e.g. "mystorageaccount")
   * @param endpointSuffix e.g. "core.windows.net"
   * @param container Container name
   * @param pathPrefix Table directory within the container; empty for the container root
   * @param tenantId Azure AD tenant ID
   * @param clientId Azure AD application (client) ID
   * @param clientSecret Azure AD client secret
   * @param validitySeconds SAS validity in seconds
   * @return SAS query string (without leading "?")
   */
  def generateUserDelegationSas(
      accountName: String,
      endpointSuffix: String,
      container: String,
      pathPrefix: String,
      tenantId: String,
      clientId: String,
      clientSecret: String,
      validitySeconds: Long,
      connectionFactory: URL => HttpURLConnection = openConnection _): String = {
    val (accessToken, _) = getAadToken(
      tenantId, clientId, clientSecret, connectionFactory = connectionFactory)
    val now = OffsetDateTime.now(ZoneOffset.UTC)
    val sasExpiry = now.plusSeconds(validitySeconds)
    val url = new URL(
      s"https://$accountName.blob.$endpointSuffix/?restype=service&comp=userdelegationkey")
    val body = s"<KeyInfo><Start>${now.toInstant}</Start>" +
      s"<Expiry>${sasExpiry.toInstant}</Expiry></KeyInfo>"
    val headers = Map(
      "Authorization" -> s"Bearer $accessToken",
      "x-ms-version" -> STORAGE_API_VERSION,
      "x-ms-date" -> DateTimeFormatter.RFC_1123_DATE_TIME.format(now))
    val userDelegationKey = parseDelegationKey(
      post(url, body, "application/xml", headers, connectionFactory))
    signUserDelegationSas(accountName, container, pathPrefix, userDelegationKey, sasExpiry)
  }

  private[server] def signUserDelegationSas(
      accountName: String,
      container: String,
      pathPrefix: String,
      userDelegationKey: UserDelegationKey,
      expiry: OffsetDateTime): String = {
    if (pathPrefix.nonEmpty) {
      val permissions = new PathSasPermission().setReadPermission(true).setListPermission(true)
      val values = new DataLakeServiceSasSignatureValues(expiry, permissions)
        .setProtocol(SasProtocol.HTTPS_ONLY)
      new DataLakeSasImplUtil(values, container, pathPrefix, true)
        .generateUserDelegationSas(userDelegationKey, accountName, Context.NONE)
    } else {
      val permissions = new FileSystemSasPermission()
        .setReadPermission(true).setListPermission(true)
      val values = new DataLakeServiceSasSignatureValues(expiry, permissions)
        .setProtocol(SasProtocol.HTTPS_ONLY)
      new DataLakeSasImplUtil(values, container, "", false)
        .generateUserDelegationSas(userDelegationKey, accountName, Context.NONE)
    }
  }
}
