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

import java.net.URI
import java.util.Date
import java.util.concurrent.TimeUnit.SECONDS

import com.amazonaws.HttpMethod
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest
import com.google.cloud.storage.BlobId
import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageOptions
import com.microsoft.azure.storage.{CloudStorageAccount, SharedAccessProtocols, StorageCredentialsSharedAccessSignature}
import com.microsoft.azure.storage.blob.{SharedAccessBlobPermissions, SharedAccessBlobPolicy}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.azure.{AzureNativeFileSystemStore, NativeAzureFileSystem}
import org.apache.hadoop.fs.azurebfs.{AzureBlobFileSystem, AzureBlobFileSystemStore}
import org.apache.hadoop.fs.azurebfs.services.AuthType
import org.apache.hadoop.fs.s3a.DefaultS3ClientFactory
import org.apache.hadoop.util.ReflectionUtils


trait CloudFileSigner {
  def sign(path: Path): String
}

class S3FileSigner(
    name: URI,
    conf: Configuration,
    preSignedUrlTimeoutSeconds: Long) extends CloudFileSigner {

  private val s3Client = ReflectionUtils.newInstance(classOf[DefaultS3ClientFactory], conf)
    .createS3Client(name)

  override def sign(path: Path): String = {
    val absPath = path.toUri
    val bucketName = absPath.getHost
    val objectKey = absPath.getPath.stripPrefix("/")
    val expiration =
      new Date(System.currentTimeMillis() + SECONDS.toMillis(preSignedUrlTimeoutSeconds))
    assert(objectKey.nonEmpty, s"cannot get object key from $path")
    val request = new GeneratePresignedUrlRequest(bucketName, objectKey)
      .withMethod(HttpMethod.GET)
      .withExpiration(expiration)
    s3Client.generatePresignedUrl(request).toString
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

  override def sign(path: Path): String = {
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
    sasTokenCredentials.transformUri(blobRef.getUri).toString
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
    getRelativePathMethod.invoke(abfsStore, path).asInstanceOf[String]
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
    if (authType != AuthType.SharedKey) {
      throw new UnsupportedOperationException(s"unsupported auth type: $authType")
    }
    val accountKey = abfsConfiguration.getStorageAccountKey
    val container = authorityParts(abfsStore, uri)(0)
    new AzureFileSigner(
      accountName,
      accountKey,
      container,
      preSignedUrlTimeoutSeconds,
      getRelativePath(abfsStore, _))
  }
}

class GCSFileSigner(
    name: URI,
    conf: Configuration,
    preSignedUrlTimeoutSeconds: Long) extends CloudFileSigner {

  private val storage = StorageOptions.newBuilder.build.getService

  override def sign(path: Path): String = {
    val absPath = path.toUri
    val bucketName = absPath.getHost
    val objectName = absPath.getPath.stripPrefix("/")
    assert(objectName.nonEmpty, s"cannot get object key from $path")
    val blobInfo = BlobInfo.newBuilder(BlobId.of(bucketName, objectName)).build
    storage.signUrl(
      blobInfo, preSignedUrlTimeoutSeconds, SECONDS, Storage.SignUrlOption.withV4Signature())
      .toString
  }
}
