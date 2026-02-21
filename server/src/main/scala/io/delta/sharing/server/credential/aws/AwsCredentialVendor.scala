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

package io.delta.sharing.server.credential.aws

import java.net.URI
import java.util.concurrent.TimeUnit.SECONDS

import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient
import com.amazonaws.services.securitytoken.model.{
  AssumeRoleRequest,
  AssumeRoleResult,
  GetFederationTokenRequest,
  PackedPolicyTooLargeException
}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.s3a.DefaultS3ClientFactory
import org.apache.hadoop.fs.s3a.S3ClientFactory.S3ClientCreationParameters
import org.apache.hadoop.util.ReflectionUtils

import io.delta.sharing.server.credential.CredentialContext
import io.delta.sharing.server.model.{AwsTempCredentials, Credentials}

/**
 * Vends AWS temporary credentials for S3 access (AssumeRole or GetFederationToken)
 * with a session policy scoped to the context locations.
 * Mirrors Unity Catalog's AwsCredentialVendor.
 */
class AwsCredentialVendor(conf: Configuration) {

  private val assumeRoleArn: Option[String] =
    Option(conf.get("delta.sharing.aws.assume.role.arn")).filter(_.nonEmpty)
      .orElse(Option(conf.get("fs.s3a.assume.role.arn")).filter(_.nonEmpty))

  private def s3ClientFor(uri: URI) =
    ReflectionUtils.newInstance(classOf[DefaultS3ClientFactory], conf)
      .createS3Client(uri, new S3ClientCreationParameters())

  def vendAwsCredentials(context: CredentialContext, validitySeconds: Long): Credentials = {
    val location = context.locations.head
    val bucket = location.getHost
    val rawPath = Option(location.getPath).getOrElse("").stripPrefix("/")
    val prefix = if (rawPath.isEmpty) "" else if (rawPath.endsWith("/")) rawPath else rawPath + "/"
    val resourcePrefix = if (prefix.isEmpty) "" else prefix + "*"
    val expirationTime = System.currentTimeMillis() + SECONDS.toMillis(validitySeconds)
    val durationSeconds = math.min(validitySeconds, 3600).toInt
    val sts = new AWSSecurityTokenServiceClient()

    val (accessKeyId, secretAccessKey, sessionToken) = assumeRoleArn match {
      case Some(roleArn) =>
        assumeRoleWithBackupPolicy(sts, roleArn, bucket, resourcePrefix, durationSeconds)
      case None =>
        val policyJson = AwsPolicyGenerator.sessionPolicy(bucket, resourcePrefix)
        val request = new GetFederationTokenRequest()
          .withName("delta-sharing-table")
          .withDurationSeconds(math.min(validitySeconds, 43200).toInt)
          .withPolicy(policyJson)
        val result = sts.getFederationToken(request)
        val c = result.getCredentials
        (c.getAccessKeyId, c.getSecretAccessKey, c.getSessionToken)
    }

    Credentials(
      location = location.toString,
      awsTempCredentials = AwsTempCredentials(
        accessKeyId = accessKeyId,
        secretAccessKey = secretAccessKey,
        sessionToken = sessionToken
      ),
      expirationTime = expirationTime
    )
  }

  private def assumeRoleWithBackupPolicy(
      sts: AWSSecurityTokenServiceClient,
      roleArn: String,
      bucket: String,
      resourcePrefix: String,
      durationSeconds: Int): (String, String, String) = {
    val policy = AwsPolicyGenerator.sessionPolicy(bucket, resourcePrefix)
    val backupPolicy = AwsPolicyGenerator.backupSessionPolicy(bucket, resourcePrefix)
    val request = new AssumeRoleRequest()
      .withRoleArn(roleArn)
      .withRoleSessionName("delta-sharing-table")
      .withDurationSeconds(durationSeconds)
      .withPolicy(policy)
    try {
      val result: AssumeRoleResult = sts.assumeRole(request)
      val c = result.getCredentials
      (c.getAccessKeyId, c.getSecretAccessKey, c.getSessionToken)
    } catch {
      case _: PackedPolicyTooLargeException =>
        val fallbackRequest = new AssumeRoleRequest()
          .withRoleArn(roleArn)
          .withRoleSessionName("delta-sharing-table")
          .withDurationSeconds(durationSeconds)
          .withPolicy(backupPolicy)
        val result = sts.assumeRole(fallbackRequest)
        val c = result.getCredentials
        (c.getAccessKeyId, c.getSecretAccessKey, c.getSessionToken)
    }
  }
}
