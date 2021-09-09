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

import java.net.{URI, URL}
import java.util.Date
import java.util.concurrent.TimeUnit.SECONDS

import com.amazonaws.HttpMethod
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest
import com.microsoft.azure.storage.{CloudStorageAccount, SharedAccessAccountPolicy}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.s3a.DefaultS3ClientFactory
import org.apache.hadoop.util.ReflectionUtils

trait CloudFileSigner {
  def sign(bucket: String, objectKey: String): URL
}

class S3FileSigner(
    name: URI,
    conf: Configuration,
    preSignedUrlTimeoutSeconds: Long) extends CloudFileSigner {

  private val s3Client = ReflectionUtils.newInstance(classOf[DefaultS3ClientFactory], conf)
    .createS3Client(name)

  override def sign(bucket: String, objectKey: String): URL = {
    val expiration =
      new Date(System.currentTimeMillis() + SECONDS.toMillis(preSignedUrlTimeoutSeconds))
    val request = new GeneratePresignedUrlRequest(bucket, objectKey)
      .withMethod(HttpMethod.GET)
      .withExpiration(expiration)
    s3Client.generatePresignedUrl(request)
  }
}

class AzureFileSigner(name: URI,
                      accountName: String,
                      storageKey: String,
                      preSignedUrlTimeoutSeconds: Long) extends CloudFileSigner {

  private def getCloudStorageAccount: CloudStorageAccount = {
    val connectionString = Seq(
      "DefaultEndpointsProtocol=https",
      s"AccountName=$accountName",
      s"AccountKey=$storageKey",
      "EndpointSuffix=core.windows.net"
    ).mkString(";")
    CloudStorageAccount.parse(connectionString)
  }

  private val cloudStorageAccount = getCloudStorageAccount

  override def sign(bucket: String, objectKey: String): URL = {

    val startDate = new Date(System.currentTimeMillis())
    val expirationDate =
      new Date(System.currentTimeMillis() + SECONDS.toMillis(preSignedUrlTimeoutSeconds))

    // Generate a Shared Access Policy with expiration
    val sharedAccessAccountPolicy = new SharedAccessAccountPolicy()
    sharedAccessAccountPolicy.setPermissionsFromString("racwdlup")
    sharedAccessAccountPolicy.setSharedAccessStartTime(startDate)
    sharedAccessAccountPolicy.setSharedAccessExpiryTime(expirationDate)
    sharedAccessAccountPolicy.setResourceTypeFromString("sco")
    sharedAccessAccountPolicy.setServiceFromString("bfqt")

    val sasToken = cloudStorageAccount.generateSharedAccessSignature(sharedAccessAccountPolicy)

    // The base URI includes the name of the account, the name of the container, and
    // the name of the blob
    val baseURI = s"https://$bucket/$objectKey?"

    new URL(baseURI + sasToken)
  }
}
