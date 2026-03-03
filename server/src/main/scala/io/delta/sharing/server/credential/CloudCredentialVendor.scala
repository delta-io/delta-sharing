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

import org.apache.hadoop.conf.Configuration

import io.delta.sharing.server.credential.aws.AwsCredentialVendor
import io.delta.sharing.server.credential.azure.AzureCredentialVendor
import io.delta.sharing.server.credential.gcp.GcpCredentialVendor
import io.delta.sharing.server.model.Credentials

/**
 * Dispatches credential vending to the appropriate cloud vendor by storage scheme.
 * Mirrors Unity Catalog's CloudCredentialVendor:
 * Unity Catalog CloudCredentialVendor (dispatch by scheme to AWS/Azure/GCP).
 */
class CloudCredentialVendor(conf: Configuration) {

  private val awsVendor = new AwsCredentialVendor(conf)
  private val azureVendor = new AzureCredentialVendor(conf)
  private val gcpVendor = new GcpCredentialVendor()

  def vendCredential(context: CredentialContext, validitySeconds: Long): Credentials = {
    context.storageScheme match {
      case StorageScheme.S3 =>
        awsVendor.vendAwsCredentials(context, validitySeconds)
      case StorageScheme.AzureWasb | StorageScheme.AzureAbfs =>
        azureVendor.vendAzureCredential(context, validitySeconds)
      case StorageScheme.GCS =>
        gcpVendor.vendGcpCredential(context, validitySeconds)
      case StorageScheme.File | StorageScheme.Null =>
        throw new UnsupportedOperationException(
          s"Temporary credentials are not supported for scheme: ${context.storageScheme}")
    }
  }
}
