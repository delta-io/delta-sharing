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

import org.apache.hadoop.conf.Configuration

import io.delta.sharing.server.model.{Credentials, TemporaryCredentials}

/**
 * Entry point for vending temporary cloud storage credentials for a given path.
 * Builds CredentialContext from the path and delegates to CloudCredentialVendor.
 *
 * Mirrors Unity Catalog's StorageCredentialVendor:
 * UC StorageCredentialVendor (path + privileges -> TemporaryCredentials).
 */
class StorageCredentialVendor(conf: Configuration) {

  private val cloudCredentialVendor = new CloudCredentialVendor(conf)

  /**
   * Vends temporary credentials for the given storage path with the given privileges.
   *
   * @param path storage location (e.g. s3a://bucket/prefix, abfss://...)
   * @param privileges SELECT for read-only; UPDATE for read-write (Delta Sharing uses SELECT)
   * @param validitySeconds credential validity in seconds
   * @return TemporaryCredentials containing cloud-specific credentials and expiration
   */
  def vendCredential(
      path: URI,
      privileges: Set[Privilege.Privilege],
      validitySeconds: Long): TemporaryCredentials = {
    if (path == null) {
      throw new IllegalArgumentException("Storage location is null.")
    }
    if (privileges == null || privileges.isEmpty) {
      throw new IllegalArgumentException("Privileges cannot be null or empty.")
    }
    val context = CredentialContext.create(path, privileges)
    val credentials: Credentials = cloudCredentialVendor.vendCredential(context, validitySeconds)
    TemporaryCredentials(credentials = credentials)
  }
}
