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

import java.net.URI
import java.util.Date
import java.util.concurrent.TimeUnit.SECONDS

import com.microsoft.azure.storage.{CloudStorageAccount, SharedAccessProtocols}
import com.microsoft.azure.storage.blob.{SharedAccessBlobPermissions, SharedAccessBlobPolicy}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.azure.AzureNativeFileSystemStore

import io.delta.sharing.server.common.AzureUserDelegationSasGenerator
import io.delta.sharing.server.credential.CredentialContext
import io.delta.sharing.server.model.{AzureUserDelegationSas, Credentials}

/** Parsed parts of an Azure storage location (WASB or ABFS). */
case class AzureLocationParts(
    accountNameFull: String,  // e.g. myaccount.dfs.core.windows.net
    accountShort: String,    // e.g. myaccount
    container: String,
    pathPrefix: String,
    endpointSuffix: String)   // e.g. core.windows.net

object AzureCredentialVendor {

  val CONF_TENANT_ID = "delta.sharing.azure.tenant.id"
  val CONF_CLIENT_ID = "delta.sharing.azure.client.id"
  val CONF_CLIENT_SECRET = "delta.sharing.azure.client.secret"
  val CONF_AUTH_TYPE = "delta.sharing.azure.auth.type"
  val CONF_MANAGED_IDENTITY_CLIENT_ID = "delta.sharing.azure.managed.identity.client.id"
  val HADOOP_OAUTH_CLIENT_ID = "fs.azure.account.oauth2.client.id"
  val HADOOP_OAUTH_CLIENT_SECRET = "fs.azure.account.oauth2.client.secret"
  val HADOOP_OAUTH_CLIENT_ENDPOINT = "fs.azure.account.oauth2.client.endpoint"

  val AUTH_TYPE_MANAGED_IDENTITY = "managed_identity"
  val AUTH_TYPE_CLIENT_CREDENTIALS = "client_credentials"
  val AUTH_TYPE_SHARED_KEY = "shared_key"

  def parseLocation(uri: URI): AzureLocationParts = {
    val host = Option(uri.getHost).getOrElse("")
    val userInfo = Option(uri.getUserInfo).getOrElse("")
    val path = Option(uri.getPath).getOrElse("").stripPrefix("/")
    val (container, pathPrefix) = if (userInfo.nonEmpty) {
      (userInfo, path)
    } else {
      val segments = path.split("/", 2)
      (segments(0), if (segments.length > 1) segments(1) else "")
    }
    val (accountShort, endpointSuffix) = if (host.endsWith(".dfs.core.windows.net")) {
      (host.stripSuffix(".dfs.core.windows.net"), "core.windows.net")
    } else if (host.endsWith(".blob.core.windows.net")) {
      (host.stripSuffix(".blob.core.windows.net"), "blob.core.windows.net")
    } else {
      throw new IllegalArgumentException(s"Cannot parse Azure authority: $host")
    }
    AzureLocationParts(
      accountNameFull = host,
      accountShort = accountShort,
      container = container,
      pathPrefix = pathPrefix,
      endpointSuffix = endpointSuffix
    )
  }

  def getAadConfig(conf: Configuration, accountName: String): Option[(String, String, String)] = {
    def get(key: String): Option[String] = Option(conf.get(key)).filter(_.nonEmpty)
    def getAccountScoped(prefix: String): Option[String] =
      get(s"$prefix.$accountName").orElse(get(prefix))
    val tenant = get(CONF_TENANT_ID)
      .orElse(parseTenantFromEndpoint(
        getAccountScoped(HADOOP_OAUTH_CLIENT_ENDPOINT).orNull))
    val clientId = get(CONF_CLIENT_ID)
      .orElse(getAccountScoped(HADOOP_OAUTH_CLIENT_ID))
    val secret = get(CONF_CLIENT_SECRET)
      .orElse(getAccountScoped(HADOOP_OAUTH_CLIENT_SECRET))
    if (tenant.isDefined && clientId.isDefined && secret.isDefined) {
      Some((tenant.get, clientId.get, secret.get))
    } else None
  }

  def parseTenantFromEndpoint(endpoint: String): Option[String] = {
    if (endpoint == null || endpoint.isEmpty) return None
    val parts = endpoint.split("/")
    val idx = parts.indexWhere(_.contains("microsoftonline.com"))
    if (idx >= 0 && idx + 1 < parts.length) Some(parts(idx + 1)) else None
  }

  def getAuthType(conf: Configuration): Option[String] =
    Option(conf.get(CONF_AUTH_TYPE)).map(_.trim).filter(_.nonEmpty)

  def getManagedIdentityClientId(conf: Configuration): Option[String] =
    Option(conf.get(CONF_MANAGED_IDENTITY_CLIENT_ID)).map(_.trim).filter(_.nonEmpty)

  /**
   * Fetch an AAD token via the Azure IMDS managed identity endpoint.
   * Uses direct HTTP (like getAadToken for client_credentials) to avoid
   * the Azure SDK HTTP pipeline which requires Jackson 2.9+
   * (incompatible with the delta-standalone Jackson 2.6.x pin).
   * @param clientId optional UAMI client ID; if None, uses system-assigned.
   * @return (accessToken, expiryMillis)
   */
  def getManagedIdentityToken(
      clientId: Option[String]): (String, Long) = {
    AzureUserDelegationSasGenerator.getManagedIdentityToken(clientId)
  }
}

/**
 * Vends Azure User Delegation SAS or account-key SAS for the context location.
 * Mirrors Unity Catalog's AzureCredentialVendor + DatalakeCredentialGenerator.
 *
 * Supports three authentication modes configured via `delta.sharing.azure.auth.type`:
 * `managed_identity` (SAMI/UAMI), `client_credentials` (service principal), or
 * `shared_key` (account key). If not set, auto-detects existing behavior.
 */
class AzureCredentialVendor(conf: Configuration) {

  def vendAzureCredential(context: CredentialContext, validitySeconds: Long): Credentials = {
    val location = context.locations.head
    val parts = AzureCredentialVendor.parseLocation(location)
    val expirationTime = System.currentTimeMillis() + SECONDS.toMillis(validitySeconds)
    val authType = AzureCredentialVendor.getAuthType(conf)

    val sasToken = authType match {
      case Some(AzureCredentialVendor.AUTH_TYPE_MANAGED_IDENTITY) =>
        vendWithManagedIdentity(parts, validitySeconds)
      case Some(AzureCredentialVendor.AUTH_TYPE_CLIENT_CREDENTIALS) =>
        vendWithClientCredentials(parts, validitySeconds)
      case Some(AzureCredentialVendor.AUTH_TYPE_SHARED_KEY) =>
        vendWithSharedKey(parts, expirationTime)
      case Some(other) =>
        throw new IllegalArgumentException(
          s"Unknown delta.sharing.azure.auth.type: '$other'. " +
            "Supported values: managed_identity, client_credentials, shared_key.")
      case None =>
        // Auto-detect: try client_credentials, then fall back to shared_key
        val aadConfig = AzureCredentialVendor.getAadConfig(conf, parts.accountNameFull)
        aadConfig match {
          case Some((tenantId, clientId, clientSecret)) =>
            vendWithClientCredentialsParams(
              parts, tenantId, clientId, clientSecret, validitySeconds)
          case None =>
            vendWithSharedKey(parts, expirationTime)
        }
    }
    Credentials(
      location = location.toString,
      azureUserDelegationSas = AzureUserDelegationSas(sasToken = sasToken),
      expirationTime = expirationTime
    )
  }

  private def vendWithManagedIdentity(
      parts: AzureLocationParts,
      validitySeconds: Long): String = {
    try {
      val uamiClientId =
        AzureCredentialVendor.getManagedIdentityClientId(conf)
      val (accessToken, _) =
        AzureCredentialVendor.getManagedIdentityToken(uamiClientId)
      AzureUserDelegationSasGenerator
        .generateUserDelegationSasFromToken(
          parts.accountShort,
          parts.endpointSuffix,
          parts.container,
          accessToken,
          validitySeconds
        )
    } catch {
      case e: Exception =>
        throw new RuntimeException(
          "Failed to generate Azure User Delegation SAS via managed identity. " +
            "Ensure the VM has a managed identity assigned and it has " +
            "Storage Blob Data Reader + Storage Blob Delegator RBAC roles.", e)
    }
  }

  private def vendWithClientCredentials(
      parts: AzureLocationParts,
      validitySeconds: Long): String = {
    val aadConfig = AzureCredentialVendor.getAadConfig(conf, parts.accountNameFull)
    aadConfig match {
      case Some((tenantId, clientId, clientSecret)) =>
        vendWithClientCredentialsParams(parts, tenantId, clientId, clientSecret, validitySeconds)
      case None =>
        throw new RuntimeException(
          "delta.sharing.azure.auth.type is set to 'client_credentials' but " +
            "tenant/client/secret configuration is missing. Set delta.sharing.azure.tenant.id, " +
            "delta.sharing.azure.client.id, and delta.sharing.azure.client.secret.")
    }
  }

  private def vendWithClientCredentialsParams(
      parts: AzureLocationParts,
      tenantId: String,
      clientId: String,
      clientSecret: String,
      validitySeconds: Long): String = {
    try {
      AzureUserDelegationSasGenerator.generateUserDelegationSas(
        parts.accountShort,
        parts.endpointSuffix,
        parts.container,
        tenantId,
        clientId,
        clientSecret,
        validitySeconds
      )
    } catch {
      case e: Exception =>
        throw new RuntimeException(
          "Failed to generate Azure User Delegation SAS (check AAD config and RBAC). " +
            "Ensure the app has Storage Blob Data Contributor or generateUserDelegationKey.", e)
    }
  }

  private def vendWithSharedKey(
      parts: AzureLocationParts,
      expirationTime: Long): String = {
    val accountKey = AzureNativeFileSystemStore.getAccountKeyFromConfiguration(
      parts.accountNameFull, conf)
    val connectionString = Seq(
      "DefaultEndpointsProtocol=https",
      s"AccountName=${parts.accountShort}",
      s"AccountKey=$accountKey",
      s"EndpointSuffix=${parts.endpointSuffix}"
    ).mkString(";")
    val account = CloudStorageAccount.parse(connectionString)
    val blobClient = account.createCloudBlobClient()
    val policy = new SharedAccessBlobPolicy()
    policy.setPermissions(java.util.EnumSet.of(SharedAccessBlobPermissions.READ))
    policy.setSharedAccessExpiryTime(new Date(expirationTime))
    val containerRef = blobClient.getContainerReference(parts.container)
    containerRef.generateSharedAccessSignature(policy, null)
  }
}
