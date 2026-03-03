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

import java.util.concurrent.TimeUnit.SECONDS

import com.google.auth.oauth2.{CredentialAccessBoundary, DownscopedCredentials, GoogleCredentials}
import com.google.cloud.hadoop.gcsio.StorageResourceId
import com.google.cloud.storage.StorageOptions
import org.apache.hadoop.fs.Path

import io.delta.sharing.server.credential.CredentialContext
import io.delta.sharing.server.model.{Credentials, GcpOauthToken}

/**
 * Vends GCP OAuth2 access tokens, optionally downscoped to the context location via CAB.
 * Mirrors Unity Catalog's GcpCredentialVendor.
 */
class GcpCredentialVendor {

  def vendGcpCredential(context: CredentialContext, validitySeconds: Long): Credentials = {
    val location = context.locations.head
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
    oauth2Creds.refreshIfExpired()
    val (bucketName, objectPrefix) = getBucketAndPrefix(new Path(location))
    val token = if (objectPrefix != null && objectPrefix.nonEmpty) {
      val resource = s"//storage.googleapis.com/projects/_/buckets/$bucketName"
      val rule = CredentialAccessBoundary.AccessBoundaryRule.newBuilder
        .setAvailableResource(resource)
        .addAvailablePermission("inRole:roles/storage.objectViewer")
        .build
      val cab = CredentialAccessBoundary.newBuilder.addRule(rule).build
      val downscoped = DownscopedCredentials.newBuilder()
        .setSourceCredential(oauth2Creds)
        .setCredentialAccessBoundary(cab)
        .build()
      downscoped.refreshIfExpired()
      downscoped.getAccessToken
    } else {
      oauth2Creds.getAccessToken
    }
    Credentials(
      location = location.toString,
      gcpOauthToken = GcpOauthToken(oauthToken = token.getTokenValue),
      expirationTime = System.currentTimeMillis() + SECONDS.toMillis(validitySeconds)
    )
  }

  private def getBucketAndPrefix(path: Path): (String, String) = {
    val resourceId = StorageResourceId.fromUriPath(path.toUri, true)
    (resourceId.getBucketName, Option(resourceId.getObjectName).getOrElse(""))
  }
}
