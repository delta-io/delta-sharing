package io.delta.exchange.spark

import java.net.URL
import java.util.concurrent.TimeUnit
import java.util.Date

import com.amazonaws.auth.AWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.HttpMethod
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest
import com.google.cloud.storage.BlobId
import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageOptions
import com.microsoft.azure.storage.CloudStorageAccount
import com.microsoft.azure.storage.blob.{CloudBlobClient, SharedAccessBlobPermissions, SharedAccessBlobPolicy}

trait CloudFileSigner {
  def sign(bucket: String, objectKey: String): URL
}

class S3FileSigner(
    awsCredentials: AWSCredentials,
    duration: Long, timeUnit: TimeUnit) extends CloudFileSigner {

  private val s3Client = new AmazonS3Client(awsCredentials)

  override def sign(bucket: String, objectKey: String): URL = {
    val expiration = new Date(System.currentTimeMillis() + timeUnit.toMillis(duration))
    val request = new GeneratePresignedUrlRequest(bucket, objectKey)
      .withMethod(HttpMethod.GET)
      .withExpiration(expiration)
    s3Client.generatePresignedUrl(request)
  }
}

class AzureFileSigner(
    storageAccount: String,
    credential: String,
    duration: Long,
    timeUnit: TimeUnit) extends CloudFileSigner {

  private def getCloudBlobClient(): CloudBlobClient = {
    val connectionString = Seq(
      "DefaultEndpointsProtocol=https",
      s"AccountName=$storageAccount",
      s"AccountKey=$credential",
      "EndpointSuffix=core.windows.net"
    ).mkString(";")
    CloudStorageAccount.parse(connectionString).createCloudBlobClient()
  }

  private val blobClient = getCloudBlobClient()

  override def sign(bucket: String, objectKey: String): URL = {
    val expiration = new Date(System.currentTimeMillis() + timeUnit.toMillis(duration))
    val container = blobClient.getContainerReference(bucket)
    val blob = container.getBlockBlobReference(objectKey)

    val sharedAccessPolicy = new SharedAccessBlobPolicy()
    sharedAccessPolicy.setPermissions(java.util.EnumSet.of(SharedAccessBlobPermissions.READ))
    sharedAccessPolicy.setSharedAccessExpiryTime(expiration);

    val blobUri = blob.getSnapshotQualifiedUri.toString
    val sas = blob.generateSharedAccessSignature(sharedAccessPolicy, null)
    new URL(blobUri + "?" + sas)
  }
}

class GCSFileSigner(
    clientEmail: String,
    privateKeyPkcs8: String,
    privateKeyId: String,
    projectId: String,
    duration: Long,
    timeUnit: TimeUnit) extends CloudFileSigner {

  private val storage = {
    val credentials = com.google.auth.oauth2.ServiceAccountCredentials.fromPkcs8(
      null,
      clientEmail,
      privateKeyPkcs8,
      privateKeyId,
      com.google.cloud.hadoop.util.CredentialFactory.DEFAULT_SCOPES
    )
    StorageOptions.newBuilder()
      .setProjectId(projectId)
      .setCredentials(credentials)
      .build().getService()
  }

  override def sign(bucket: String, objectKey: String): URL = {
    val blobInfo = BlobInfo.newBuilder(BlobId.of(bucket, objectKey)).build()
    storage.signUrl(blobInfo, duration, timeUnit, Storage.SignUrlOption.withV4Signature())
  }
}

/** A dummy CloudFileSigner to run tests using the local file system. */
class DummyFileSigner extends CloudFileSigner {
  override def sign(bucket: String, objectKey: String): URL = {
    new URL("file:///" + objectKey)
  }
}
