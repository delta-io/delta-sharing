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

import io.delta.sharing.server.credential.{CredentialContext, Privilege}

/**
 * Generates AWS IAM session policies for scoped S3 access.
 * Mirrors Unity Catalog's AwsPolicyGenerator (SELECT = GetObject + ListBucket with prefix).
 */
object AwsPolicyGenerator {

  /** Full session policy: GetObject + ListBucket with prefix condition. */
  def sessionPolicy(bucket: String, resourcePrefix: String): String =
    s"""{
       |  "Version": "2012-10-17",
       |  "Statement": [
       |    {
       |      "Effect": "Allow",
       |      "Action": ["s3:GetObject"],
       |      "Resource": "arn:aws:s3:::${bucket}/${resourcePrefix}"
       |    },
       |    {
       |      "Effect": "Allow",
       |      "Action": ["s3:ListBucket"],
       |      "Resource": "arn:aws:s3:::${bucket}",
       |      "Condition": {
       |        "StringLike": {"s3:prefix": ["${resourcePrefix}"]}
       |      }
       |    }
       |  ]
       |}""".stripMargin

  /** Backup policy (smaller): GetObject only, for PackedPolicyTooLarge retry. */
  def backupSessionPolicy(bucket: String, resourcePrefix: String): String =
    s"""{
       |  "Version": "2012-10-17",
       |  "Statement": [
       |    {
       |      "Effect": "Allow",
       |      "Action": ["s3:GetObject"],
       |      "Resource": "arn:aws:s3:::${bucket}/${resourcePrefix}"
       |    }
       |  ]
       |}""".stripMargin

  /**
   * Build policy for the given context (locations + privileges).
   * Delta Sharing only uses SELECT (read); UPDATE would add Put/Delete/Multipart.
   */
  def generatePolicy(privileges: Set[Privilege.Privilege], locations: Seq[URI]): String = {
    if (locations.isEmpty) {
      throw new IllegalArgumentException("At least one location is required")
    }
    // Single location: bucket + path prefix
    val uri = locations.head
    val bucket = uri.getHost
    val rawPath = Option(uri.getPath).getOrElse("").stripPrefix("/")
    val prefix = if (rawPath.isEmpty) "" else if (rawPath.endsWith("/")) rawPath else rawPath + "/"
    val resourcePrefix = if (prefix.isEmpty) "" else prefix + "*"
    if (privileges.contains(Privilege.UPDATE)) {
      // Could extend with PutObject, DeleteObject, etc. Delta Sharing uses SELECT only.
      sessionPolicy(bucket, resourcePrefix)
    } else {
      sessionPolicy(bucket, resourcePrefix)
    }
  }
}
