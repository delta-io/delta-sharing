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
import java.util.Scanner
import java.util.concurrent.TimeUnit.SECONDS

import com.azure.core.credential.{AccessToken, TokenCredential, TokenRequestContext}
import com.azure.storage.blob.{BlobServiceClient, BlobServiceClientBuilder}
import com.azure.storage.blob.sas.{BlobContainerSasPermission, BlobServiceSasSignatureValues}
import reactor.core.publisher.Mono

import io.delta.sharing.server.common.JsonUtils

/**
 * Generates Azure User Delegation SAS (AAD token -> User Delegation Key -> SAS).
 * Replicates the Unity Catalog flow: get AAD token via client_credentials, then
 * BlobServiceClient.getUserDelegationKey(), then build SAS locally.
 */
object AzureUserDelegationSasGenerator {

  val StorageScope = "https://storage.azure.com/.default"
  val AadTokenEndpointTemplate = "https://login.microsoftonline.com/%s/oauth2/v2.0/token"

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

  /** TokenCredential that returns a pre-fetched AAD token. */
  private class StaticTokenCredential(
      accessToken: String, expiryMillis: Long) extends TokenCredential {
    override def getToken(request: TokenRequestContext): Mono[AccessToken] =
      Mono.just(new AccessToken(
        accessToken,
        java.time.Instant.ofEpochMilli(expiryMillis)
          .atOffset(OffsetDateTime.now().getOffset)))
  }

  /**
   * Generate a container-scoped User Delegation SAS.
   * @param accountName Storage account name (e.g. "mystorageaccount")
   * @param endpointSuffix e.g. "core.windows.net" or "blob.core.windows.net"
   * @param container Container name
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
      tenantId: String,
      clientId: String,
      clientSecret: String,
      validitySeconds: Long): String = {
    val (accessToken, expiryMillis) = getAadToken(tenantId, clientId, clientSecret)
    val credential = new StaticTokenCredential(accessToken, expiryMillis)
    val endpoint = s"https://$accountName.blob.$endpointSuffix"
    val blobServiceClient: BlobServiceClient = new BlobServiceClientBuilder()
      .endpoint(endpoint)
      .credential(credential)
      .buildClient()
    val now = OffsetDateTime.now()
    val keyExpiry = now.plusHours(1)
    val userDelegationKey = blobServiceClient.getUserDelegationKey(now, keyExpiry)
    val sasExpiry = now.plusSeconds(validitySeconds)
    val permission = new BlobContainerSasPermission().setReadPermission(true)
    val values = new BlobServiceSasSignatureValues(sasExpiry, permission)
    val containerClient = blobServiceClient.getBlobContainerClient(container)
    containerClient.generateUserDelegationSas(values, userDelegationKey)
  }
}
