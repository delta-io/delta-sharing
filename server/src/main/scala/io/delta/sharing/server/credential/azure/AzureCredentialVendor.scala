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
  val HADOOP_OAUTH_CLIENT_ID = "fs.azure.account.oauth2.client.id"
  val HADOOP_OAUTH_CLIENT_SECRET = "fs.azure.account.oauth2.client.secret"
  val HADOOP_OAUTH_CLIENT_ENDPOINT = "fs.azure.account.oauth2.client.endpoint"

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
}

/**
 * Vends Azure User Delegation SAS or account-key SAS for the context location.
 * Mirrors Unity Catalog's AzureCredentialVendor + DatalakeCredentialGenerator.
 */
class AzureCredentialVendor(conf: Configuration) {

  def vendAzureCredential(context: CredentialContext, validitySeconds: Long): Credentials = {
    val location = context.locations.head
    val parts = AzureCredentialVendor.parseLocation(location)
    val accountKey = AzureNativeFileSystemStore.getAccountKeyFromConfiguration(
      parts.accountNameFull, conf)
    val aadConfig = AzureCredentialVendor.getAadConfig(conf, parts.accountNameFull)
    val expirationTime = System.currentTimeMillis() + SECONDS.toMillis(validitySeconds)
    val sasToken = aadConfig match {
      case Some((tenantId, clientId, clientSecret)) =>
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
      case None =>
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
    Credentials(
      location = location.toString,
      azureUserDelegationSas = AzureUserDelegationSas(sasToken = sasToken),
      expirationTime = expirationTime
    )
  }
}
