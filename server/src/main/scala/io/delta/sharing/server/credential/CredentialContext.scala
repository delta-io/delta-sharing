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

package io.delta.sharing.server.credential

import java.net.URI

/**
 * Storage scheme used to select which cloud credential vendor to use.
 * Mirrors Unity Catalog's UriScheme for credential vending.
 */
object StorageScheme extends Enumeration {
  type StorageScheme = Value
  val S3, AzureWasb, AzureAbfs, GCS, File, Null = Value

  def fromUri(uri: URI): StorageScheme = {
    // scalastyle:off caselocale
    val scheme = Option(uri.getScheme).getOrElse("").toLowerCase
    // scalastyle:on caselocale
    scheme match {
      case "s3a" | "s3" => S3
      case "wasb" | "wasbs" => AzureWasb
      case "abfs" | "abfss" => AzureAbfs
      case "gs" => GCS
      case "file" => File
      case _ => Null
    }
  }
}

/**
 * Privileges for vended credentials (read vs write).
 * Mirrors Unity Catalog's CredentialContext.Privilege.
 */
object Privilege extends Enumeration {
  type Privilege = Value
  val SELECT, UPDATE = Value
}

/**
 * Context for generating cloud storage credentials.
 * Encapsulates scheme, storage base, privileges, and locations so that
 * cloud-specific vendors can produce appropriately scoped temporary credentials.
 *
 * Mirrors Unity Catalog's CredentialContext (path + privileges -> context).
 */
case class CredentialContext(
    storageScheme: StorageScheme.StorageScheme,
    storageBase: URI,
    privileges: Set[Privilege.Privilege],
    locations: Seq[URI])

object CredentialContext {

  /**
   * Storage base is scheme + authority (no path), e.g. s3a://bucket,
   * abfss://container@account.dfs.core.windows.net.
   */
  def storageBaseOf(uri: URI): URI = {
    new URI(
      uri.getScheme,
      uri.getUserInfo,
      uri.getHost,
      if (uri.getPort > 0) uri.getPort else -1,
      "/",
      null,
      null
    )
  }

  /** Create a CredentialContext for a single storage location. */
  def create(
      location: URI,
      privileges: Set[Privilege.Privilege]): CredentialContext = {
    CredentialContext(
      storageScheme = StorageScheme.fromUri(location),
      storageBase = storageBaseOf(location),
      privileges = privileges,
      locations = Seq(location)
    )
  }
}
