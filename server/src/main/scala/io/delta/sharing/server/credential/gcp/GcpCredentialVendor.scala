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

package io.delta.sharing.server.credential.gcp

import java.util.Collections
import java.util.concurrent.TimeUnit.SECONDS

import com.google.auth.http.HttpTransportFactory
import com.google.auth.oauth2.{CredentialAccessBoundary, DownscopedCredentials, GoogleCredentials}
import com.google.cloud.hadoop.gcsio.StorageResourceId
import com.google.cloud.storage.StorageOptions
import org.apache.hadoop.fs.Path

import io.delta.sharing.server.common.JsonUtils
import io.delta.sharing.server.credential.CredentialContext
import io.delta.sharing.server.model.{Credentials, GcpOauthToken}

/**
 * Vends GCP OAuth2 access tokens, downscoped to the context location via CAB.
 * Mirrors Unity Catalog's GcpCredentialVendor.
 */
class GcpCredentialVendor {

  def vendGcpCredential(context: CredentialContext, validitySeconds: Long): Credentials = {
    val storage = StorageOptions.newBuilder.build.getService
    var creds = storage.getOptions.getCredentials
    if (creds == null) {
      creds = GoogleCredentials.getApplicationDefault()
    }
    val oauth2Creds = creds match {
      case c: GoogleCredentials => c
      case _ =>
        try GoogleCredentials.getApplicationDefault()
        catch {
          case _: Exception =>
            throw new UnsupportedOperationException(
              "GCS credential does not support access token; use Application Default " +
                "Credentials (e.g. JSON key or workload identity)")
        }
    }
    vendGcpCredential(context, validitySeconds, oauth2Creds, null)
  }

  private[gcp] def vendGcpCredential(
      context: CredentialContext,
      validitySeconds: Long,
      sourceCredentials: GoogleCredentials,
      transportFactory: HttpTransportFactory): Credentials = {
    val location = context.locations.head
    // Explicitly request cloud-platform scope so token refresh succeeds (avoids invalid_scope from
    // default/empty scopes in some environments).
    val scopedCreds = sourceCredentials.createScoped(
      Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"))
    scopedCreds.refreshIfExpired()
    val (bucketName, objectPrefix) = getBucketAndPrefix(new Path(location))
    val downscoped = DownscopedCredentials.newBuilder()
      .setSourceCredential(scopedCreds)
      .setHttpTransportFactory(transportFactory)
      .setCredentialAccessBoundary(GcpCredentialVendor.accessBoundary(bucketName, objectPrefix))
      .build()
    downscoped.refreshIfExpired()
    val token = downscoped.getAccessToken
    Credentials(
      location = location.toString,
      gcpOauthToken = GcpOauthToken(oauthToken = token.getTokenValue),
      expirationTime = math.min(
        System.currentTimeMillis() + SECONDS.toMillis(validitySeconds),
        Option(token.getExpirationTime).map(_.getTime).getOrElse(Long.MaxValue))
    )
  }

  private def getBucketAndPrefix(path: Path): (String, String) = {
    val resourceId = StorageResourceId.fromUriPath(path.toUri, true)
    (resourceId.getBucketName, Option(resourceId.getObjectName).getOrElse(""))
  }
}

object GcpCredentialVendor {

  private[gcp] def accessBoundary(
      bucketName: String,
      objectPrefix: String): CredentialAccessBoundary = {
    val rule = CredentialAccessBoundary.AccessBoundaryRule.newBuilder
      .setAvailableResource(s"//storage.googleapis.com/projects/_/buckets/$bucketName")
      .addAvailablePermission("inRole:roles/storage.objectViewer")
    val prefix = objectPrefix.reverse.dropWhile(_ == '/').reverse
    if (prefix.nonEmpty) {
      // Quote as a CEL string literal, and include the separator to exclude sibling directories.
      val objectName = JsonUtils.toJson(s"projects/_/buckets/$bucketName/objects/$prefix/")
      val listPrefix = JsonUtils.toJson(prefix + "/")
      val expression = s"resource.name.startsWith($objectName) || " +
        s"api.getAttribute('storage.googleapis.com/objectListPrefix', '').startsWith($listPrefix)"
      rule.setAvailabilityCondition(
        CredentialAccessBoundary.AccessBoundaryRule.AvailabilityCondition.newBuilder
          .setExpression(expression)
          .build())
    }
    // Bucket-root tables still require a boundary; never return the provider's source token.
    CredentialAccessBoundary.newBuilder.addRule(rule.build()).build()
  }
}
