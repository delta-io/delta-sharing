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
import java.util.Locale

/**
 * Storage scheme used to select which cloud credential vendor to use.
 *
 * The naming aligns with [[https://github.com/unitycatalog/unitycatalog Unity Catalog (OSS)]]
 * credential-vending types so implementations can stay consistent with the Delta Sharing protocol,
 * which references UC OSS temporary-credentials APIs.
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
 *
 * Aligns with Unity Catalog (OSS) `CredentialContext.Privilege` for protocol consistency.
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
 * Structurally similar to Unity Catalog (OSS) `CredentialContext` (path + privileges).
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

  /**
   * Returns true if `requested` is the same storage location as the table's configured root,
   * or a strict subdirectory under that root (same scheme family and authority), after
   * `URI.normalize()` and path canonicalization. Used so temporary credentials are not vended
   * for unrelated URIs while still allowing clients to request a subprefix of the table root.
   *
   * When table config gains `auxiliaryLocations` (per the Delta Sharing protocol), those URIs
   * should be treated as additional allowed roots (each with the same "same or under" rule).
   */
  def temporaryCredentialLocationAllowed(tableLocation: String, requested: URI): Boolean = {
    val tableUri = new URI(tableLocation).normalize()
    val req = requested.normalize()
    val (tScheme, tAuth, tPath) = locationComponents(tableUri)
    val (rScheme, rAuth, rPath) = locationComponents(req)
    if (tScheme != rScheme || tAuth != rAuth) {
      false
    } else {
      isPathSameOrUnderTableRoot(tPath, rPath)
    }
  }

  /**
   * Scheme family (s3/s3a/...), lowercased authority, and path with no leading/trailing slashes
   * (empty means the authority is the whole location, e.g. bucket root).
   */
  private def locationComponents(uri: URI): (String, String, String) = {
    // scalastyle:off caselocale
    val scheme = Option(uri.getScheme).map(_.toLowerCase(Locale.ROOT)).getOrElse("")
    // scalastyle:on caselocale
    val normalizedScheme = scheme match {
      case "s3" | "s3a" | "s3n" => "s3"
      case "wasbs" | "wasb" => "wasb"
      case "abfss" | "abfs" => "abfs"
      case "gs" => "gs"
      case other => other
    }
    val authority = Option(uri.getAuthority).map { a =>
      // scalastyle:off caselocale
      a.toLowerCase(Locale.ROOT)
      // scalastyle:on caselocale
    }.getOrElse("")
    val path = Option(uri.getPath).getOrElse("")
      .replaceAll("/+", "/")
      .stripPrefix("/")
      .stripSuffix("/")
    (normalizedScheme, authority, path)
  }

  /** True if `requestedPath` equals `tableRootPath` or is a proper path prefix descendant. */
  private def isPathSameOrUnderTableRoot(tableRootPath: String, requestedPath: String): Boolean = {
    if (tableRootPath == requestedPath) {
      true
    } else if (tableRootPath.isEmpty) {
      // Table root is the whole storage account/bucket (no path); any path on it is under root.
      true
    } else {
      requestedPath.startsWith(tableRootPath + "/")
    }
  }

  /**
   * Canonical comparison key for cloud table locations (scheme family, authority, path).
   * Used for tests and debugging.
   */
  private[credential] def canonicalLocationKey(uri: URI): String = {
    val (s, a, p) = locationComponents(uri)
    val pathPart = if (p.isEmpty) "" else "/" + p
    s"$s://$a$pathPart"
  }
}
