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

import java.net.URI
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Date
import java.util.concurrent.TimeUnit.SECONDS

import com.amazonaws.HttpMethod
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest
import com.google.cloud.hadoop.gcsio.StorageResourceId
import com.google.cloud.storage.BlobId
import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageOptions
import com.microsoft.azure.storage.{CloudStorageAccount, SharedAccessProtocols,
  StorageCredentialsSharedAccessSignature}
import com.microsoft.azure.storage.blob.{SharedAccessBlobPermissions, SharedAccessBlobPolicy}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.azure.{AzureNativeFileSystemStore, NativeAzureFileSystem}
import org.apache.hadoop.fs.azurebfs.{AzureBlobFileSystem, AzureBlobFileSystemStore}
import org.apache.hadoop.fs.azurebfs.services.AuthType
import org.apache.hadoop.fs.s3a.DefaultS3ClientFactory
import org.apache.hadoop.fs.s3a.S3ClientFactory.S3ClientCreationParameters
import org.apache.hadoop.util.ReflectionUtils

import io.delta.sharing.server.credential.azure.AzureCredentialVendor

/**
 * @param url The signed url.
 * @param expirationTimestamp The expiration timestamp in millis of the signed url, a minimum
 *                            between the timeout of the url and of the token.
 */
case class PreSignedUrl(url: String, expirationTimestamp: Long)

trait CloudFileSigner {
  def sign(path: Path): PreSignedUrl
}

class S3FileSigner(
    name: URI,
    conf: Configuration,
    preSignedUrlTimeoutSeconds: Long) extends CloudFileSigner {

  private val s3Client = ReflectionUtils.newInstance(classOf[DefaultS3ClientFactory], conf)
    .createS3Client(name, new S3ClientCreationParameters())

  override def sign(path: Path): PreSignedUrl = {
    val absPath = path.toUri
    val bucketName = absPath.getHost
    val objectKey = absPath.getPath.stripPrefix("/")
    val expiration =
      new Date(System.currentTimeMillis() + SECONDS.toMillis(preSignedUrlTimeoutSeconds))
    assert(objectKey.nonEmpty, s"cannot get object key from $path")
    val request = new GeneratePresignedUrlRequest(bucketName, objectKey)
      .withMethod(HttpMethod.GET)
      .withExpiration(expiration)
    PreSignedUrl(
      s3Client.generatePresignedUrl(request).toString,
      System.currentTimeMillis() + SECONDS.toMillis(preSignedUrlTimeoutSeconds)
    )
  }
}

class AzureFileSigner(
    accountName: String,
    storageKey: String,
    container: String,
    preSignedUrlTimeoutSeconds: Long,
    objectKeyExtractor: Path => String) extends CloudFileSigner {

  private val (rawAccountName, endpointSuffix) = {
    val splits = accountName.split("\\.", 3)
    if (splits.length != 3) {
      throw new IllegalArgumentException(s"Incorrect account name: $accountName")
    }
    (splits(0), splits(2))
  }

  private def getCloudStorageAccount: CloudStorageAccount = {
    val connectionString = Seq(
      "DefaultEndpointsProtocol=https",
      s"AccountName=$rawAccountName",
      s"AccountKey=$storageKey",
      s"EndpointSuffix=$endpointSuffix"
    ).mkString(";")
    CloudStorageAccount.parse(connectionString)
  }

  private val cloudStorageAccount = getCloudStorageAccount

  private val blobClient = cloudStorageAccount.createCloudBlobClient()

  private def getAccessPolicy: SharedAccessBlobPolicy = {
    val expiration =
      new Date(System.currentTimeMillis() + SECONDS.toMillis(preSignedUrlTimeoutSeconds))
    val sharedAccessPolicy = new SharedAccessBlobPolicy()
    sharedAccessPolicy.setPermissions(java.util.EnumSet.of(SharedAccessBlobPermissions.READ))
    sharedAccessPolicy.setSharedAccessExpiryTime(expiration)
    sharedAccessPolicy
  }

  override def sign(path: Path): PreSignedUrl = {
    val containerRef = blobClient.getContainerReference(container)
    val objectKey = objectKeyExtractor(path)
    assert(objectKey.nonEmpty, s"cannot get object key from $path")
    val blobRef = containerRef.getBlockBlobReference(objectKey)
    val accessPolicy = getAccessPolicy
    val sasToken = blobRef.generateSharedAccessSignature(
      accessPolicy,
      /* headers */ null,
      /* groupPolicyIdentifier */ null,
      /* ipRange */ null,
      SharedAccessProtocols.HTTPS_ONLY
    )
    val sasTokenCredentials = new StorageCredentialsSharedAccessSignature(sasToken)
    PreSignedUrl(
      sasTokenCredentials.transformUri(blobRef.getUri).toString,
      System.currentTimeMillis() + SECONDS.toMillis(preSignedUrlTimeoutSeconds)
    )
  }
}

object WasbFileSigner {
  private def getAccountFromAuthority(store: AzureNativeFileSystemStore, uri: URI): String = {
    val getAccountFromAuthorityMethod = classOf[AzureNativeFileSystemStore]
      .getDeclaredMethod("getAccountFromAuthority", classOf[URI])
    getAccountFromAuthorityMethod.setAccessible(true)
    getAccountFromAuthorityMethod.invoke(store, uri).asInstanceOf[String]
  }

  private def getContainerFromAuthority(store: AzureNativeFileSystemStore, uri: URI): String = {
    val getContainerFromAuthorityMethod = classOf[AzureNativeFileSystemStore]
      .getDeclaredMethod("getContainerFromAuthority", classOf[URI])
    getContainerFromAuthorityMethod.setAccessible(true)
    getContainerFromAuthorityMethod.invoke(store, uri).asInstanceOf[String]
  }

  def apply(
      fs: NativeAzureFileSystem,
      uri: URI,
      conf: Configuration,
      preSignedUrlTimeoutSeconds: Long): CloudFileSigner = {
    val accountName = getAccountFromAuthority(fs.getStore, uri)
    val accountKey = AzureNativeFileSystemStore.getAccountKeyFromConfiguration(accountName, conf)
    val container = getContainerFromAuthority(fs.getStore, uri)
    new AzureFileSigner(
      accountName,
      accountKey,
      container,
      preSignedUrlTimeoutSeconds,
      fs.pathToKey)
  }
}

object AbfsFileSigner {
  private def getAbfsStore(fs: AzureBlobFileSystem): AzureBlobFileSystemStore = {
    val getAbfsStoreMethod = classOf[AzureBlobFileSystem].getDeclaredMethod("getAbfsStore")
    getAbfsStoreMethod.setAccessible(true)
    getAbfsStoreMethod.invoke(fs).asInstanceOf[AzureBlobFileSystemStore]
  }

  private def getRelativePath(abfsStore: AzureBlobFileSystemStore, path: Path): String = {
    val getRelativePathMethod = classOf[AzureBlobFileSystemStore]
      .getDeclaredMethod("getRelativePath", classOf[Path])
    getRelativePathMethod.setAccessible(true)
    var relativePath = getRelativePathMethod.invoke(abfsStore, path).asInstanceOf[String]

    // remove duplicate separator character for azure relative path
    // see https://github.com/apache/hadoop/commit/af98f32f7dbb9d71915690b66f12c33758011450
    // #diff-94925ffd3b21968d7e6b476f7e85f68f5ea326f186262017fad61a5a6a3815cbL1215
    // for more details
    if (relativePath.charAt(0) == Path.SEPARATOR_CHAR) {
      relativePath = relativePath.substring(1)
    }
    relativePath
  }

  private def authorityParts(abfsStore: AzureBlobFileSystemStore, uri: URI): Array[String] = {
    val authorityPartsMethod = classOf[AzureBlobFileSystemStore]
      .getDeclaredMethod("authorityParts", classOf[URI])
    authorityPartsMethod.setAccessible(true)
    authorityPartsMethod.invoke(abfsStore, uri).asInstanceOf[Array[String]]
  }

  def apply(
      fs: AzureBlobFileSystem,
      uri: URI,
      preSignedUrlTimeoutSeconds: Long): CloudFileSigner = {
    val abfsStore = getAbfsStore(fs)
    val abfsConfiguration = abfsStore.getAbfsConfiguration
    val accountName = abfsConfiguration.accountConf("dummy").stripPrefix("dummy.")
    val authType = abfsConfiguration.getAuthType(accountName)
    val container = authorityParts(abfsStore, uri)(0)
    authType match {
      case AuthType.SharedKey =>
        val accountKey = abfsConfiguration.getStorageAccountKey
        new AzureFileSigner(
          accountName,
          accountKey,
          container,
          preSignedUrlTimeoutSeconds,
          getRelativePath(abfsStore, _))
      case AuthType.OAuth =>
        // Fetch managed identity token via IMDS and use REST-based
        // User Delegation SAS generation (avoids Azure SDK HTTP
        // pipeline which requires Jackson 2.9+).
        val conf = fs.getConf
        val uamiClientId =
          AzureCredentialVendor.getManagedIdentityClientId(conf)
        val splits = accountName.split("\\.", 3)
        if (splits.length != 3) {
          throw new IllegalArgumentException(
            s"Incorrect account name: $accountName")
        }
        val rawAccountName = splits(0)
        val endpointSuffix = splits(2)
        new AbfsUserDelegationFileSigner(
          rawAccountName,
          endpointSuffix,
          container,
          uamiClientId,
          preSignedUrlTimeoutSeconds,
          getRelativePath(abfsStore, _))
      case _ =>
        throw new UnsupportedOperationException(
          s"unsupported auth type: $authType")
    }
  }
}

/**
 * Signs ABFS file URLs using User Delegation SAS for
 * OAuth-authenticated filesystems (e.g. managed identity).
 * Uses REST calls to fetch the delegation key (cached 50 min)
 * and computes the SAS locally via HMAC-SHA256.
 *
 * TODO: migrate to Azure Storage SDK when Jackson is upgraded
 * (see delta-io/delta#5598).
 */
class AbfsUserDelegationFileSigner(
    rawAccountName: String,
    endpointSuffix: String,
    container: String,
    uamiClientId: Option[String],
    preSignedUrlTimeoutSeconds: Long,
    objectKeyExtractor: Path => String) extends CloudFileSigner {

  private val blobEndpoint =
    s"https://$rawAccountName.blob.$endpointSuffix"

  // Cache delegation key to avoid a REST call per file.
  // Keys are valid 1 hour; refresh after 50 min.
  private val KEY_REFRESH_MINUTES = 50
  @volatile private var cachedToken: String = _
  @volatile private var cachedKey:
    AzureUserDelegationSasGenerator.UserDelegationKeyInfo = _
  @volatile private var cachedKeyRefreshAt:
    java.time.OffsetDateTime = _

  private def ensureKey(): (
      String,
      AzureUserDelegationSasGenerator.UserDelegationKeyInfo) = {
    val now = java.time.OffsetDateTime.now()
    if (cachedKey == null || now.isAfter(cachedKeyRefreshAt)) {
      synchronized {
        val nowInner = java.time.OffsetDateTime.now()
        if (cachedKey == null ||
            nowInner.isAfter(cachedKeyRefreshAt)) {
          val (token, _) = AzureUserDelegationSasGenerator
            .getManagedIdentityToken(uamiClientId)
          val keyExpiry = nowInner.plusHours(1)
          cachedToken = token
          cachedKey = AzureUserDelegationSasGenerator
            .getUserDelegationKey(
              rawAccountName, endpointSuffix,
              token, nowInner, keyExpiry)
          cachedKeyRefreshAt =
            nowInner.plusMinutes(KEY_REFRESH_MINUTES)
        }
      }
    }
    (cachedToken, cachedKey)
  }

  override def sign(path: Path): PreSignedUrl = {
    val objectKey = objectKeyExtractor(path)
    assert(objectKey.nonEmpty, s"cannot get object key from $path")
    val now = java.time.OffsetDateTime.now()
    val (_, key) = ensureKey()
    val sasExpiry = now.plusSeconds(preSignedUrlTimeoutSeconds)
    val sasToken = AzureUserDelegationSasGenerator
      .computeUserDelegationSas(
        rawAccountName, container, key, "r",
        now, sasExpiry)
    // Encode each path segment individually. URLEncoder
    // would encode / as %2F which is a different blob path.
    val encodedKey = objectKey.split("/").map(seg =>
      java.net.URLEncoder.encode(seg, UTF_8.name)
    ).mkString("/")
    val blobUrl = s"$blobEndpoint/$container/$encodedKey"
    PreSignedUrl(
      s"$blobUrl?$sasToken",
      System.currentTimeMillis() +
        SECONDS.toMillis(preSignedUrlTimeoutSeconds)
    )
  }
}

class GCSFileSigner(
    name: URI,
    conf: Configuration,
    preSignedUrlTimeoutSeconds: Long) extends CloudFileSigner {

  private val storage = StorageOptions.newBuilder.build.getService

  override def sign(path: Path): PreSignedUrl = {
    val (bucketName, objectName) = GCSFileSigner.getBucketAndObjectNames(path)
    assert(objectName.nonEmpty, s"cannot get object key from $path")
    val blobInfo = BlobInfo.newBuilder(BlobId.of(bucketName, objectName)).build
    PreSignedUrl(
      storage.signUrl(
      blobInfo, preSignedUrlTimeoutSeconds, SECONDS, Storage.SignUrlOption.withV4Signature())
      .toString,
      System.currentTimeMillis() + SECONDS.toMillis(preSignedUrlTimeoutSeconds)
    )
  }
}

object GCSFileSigner {
  def getBucketAndObjectNames(path: Path): (String, String) = {
    val resourceId = StorageResourceId.fromUriPath(path.toUri, false /* = allowEmptyObjectName */)
    (resourceId.getBucketName, resourceId.getObjectName)
  }
}
