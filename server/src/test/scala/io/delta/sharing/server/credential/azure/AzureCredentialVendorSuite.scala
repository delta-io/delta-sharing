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

package io.delta.sharing.server.credential.azure

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.scalatest.FunSuite

class AzureCredentialVendorSuite extends FunSuite {

  test("parseLocation abfss with userInfo") {
    val uri = new URI("abfss://mycontainer@myaccount.dfs.core.windows.net/table/path")
    val parts = AzureCredentialVendor.parseLocation(uri)
    assert(parts.accountNameFull == "myaccount.dfs.core.windows.net")
    assert(parts.accountShort == "myaccount")
    assert(parts.container == "mycontainer")
    assert(parts.pathPrefix == "table/path")
    assert(parts.endpointSuffix == "core.windows.net")
  }

  test("parseLocation wasbs with userInfo") {
    val uri = new URI("wasbs://container@account.blob.core.windows.net/foo/bar")
    val parts = AzureCredentialVendor.parseLocation(uri)
    assert(parts.accountNameFull == "account.blob.core.windows.net")
    assert(parts.accountShort == "account")
    assert(parts.container == "container")
    assert(parts.pathPrefix == "foo/bar")
    assert(parts.endpointSuffix == "blob.core.windows.net")
  }

  test("parseLocation abfss path-only container") {
    val uri = new URI("abfss://account.dfs.core.windows.net/container/table/path")
    val parts = AzureCredentialVendor.parseLocation(uri)
    assert(parts.accountShort == "account")
    assert(parts.container == "container")
    assert(parts.pathPrefix == "table/path")
  }

  test("parseLocation throws for non-Azure host") {
    val uri = new URI("abfss://account.other.net/container/path")
    intercept[IllegalArgumentException] {
      AzureCredentialVendor.parseLocation(uri)
    }
  }

  test("parseTenantFromEndpoint") {
    assert(AzureCredentialVendor.parseTenantFromEndpoint(null).isEmpty)
    assert(AzureCredentialVendor.parseTenantFromEndpoint("").isEmpty)
    assert(AzureCredentialVendor.parseTenantFromEndpoint(
      "https://login.microsoftonline.com/tenant-id-123/oauth2/v2.0/token")
      == Some("tenant-id-123"))
    assert(AzureCredentialVendor.parseTenantFromEndpoint(
      "https://other.com/foo").isEmpty)
  }

  test("getAadConfig from delta.sharing.azure keys") {
    val conf = new Configuration()
    conf.set(AzureCredentialVendor.CONF_TENANT_ID, "tenant1")
    conf.set(AzureCredentialVendor.CONF_CLIENT_ID, "client1")
    conf.set(AzureCredentialVendor.CONF_CLIENT_SECRET, "secret1")
    val result = AzureCredentialVendor.getAadConfig(conf, "myaccount.dfs.core.windows.net")
    assert(result == Some(("tenant1", "client1", "secret1")))
  }

  test("getAadConfig empty when key missing") {
    val conf = new Configuration()
    conf.set(AzureCredentialVendor.CONF_TENANT_ID, "tenant1")
    assert(AzureCredentialVendor.getAadConfig(conf, "account.dfs.core.windows.net").isEmpty)
  }

  test("getAadConfig account-scoped Hadoop OAuth keys") {
    val conf = new Configuration()
    val account = "myaccount.dfs.core.windows.net"
    conf.set(s"${AzureCredentialVendor.HADOOP_OAUTH_CLIENT_ID}.$account", "cid")
    conf.set(s"${AzureCredentialVendor.HADOOP_OAUTH_CLIENT_SECRET}.$account", "csecret")
    conf.set(s"${AzureCredentialVendor.HADOOP_OAUTH_CLIENT_ENDPOINT}.$account",
      "https://login.microsoftonline.com/tenant-abc/oauth2/v2.0/token")
    val result = AzureCredentialVendor.getAadConfig(conf, account)
    assert(result == Some(("tenant-abc", "cid", "csecret")))
  }

  // --- Managed identity config tests ---

  test("getAuthType returns None when not configured") {
    val conf = new Configuration()
    assert(AzureCredentialVendor.getAuthType(conf).isEmpty)
  }

  test("getAuthType returns managed_identity") {
    val conf = new Configuration()
    conf.set(AzureCredentialVendor.CONF_AUTH_TYPE, "managed_identity")
    assert(AzureCredentialVendor.getAuthType(conf) == Some("managed_identity"))
  }

  test("getAuthType returns client_credentials") {
    val conf = new Configuration()
    conf.set(AzureCredentialVendor.CONF_AUTH_TYPE, "client_credentials")
    assert(AzureCredentialVendor.getAuthType(conf) == Some("client_credentials"))
  }

  test("getAuthType returns shared_key") {
    val conf = new Configuration()
    conf.set(AzureCredentialVendor.CONF_AUTH_TYPE, "shared_key")
    assert(AzureCredentialVendor.getAuthType(conf) == Some("shared_key"))
  }

  test("getAuthType trims whitespace") {
    val conf = new Configuration()
    conf.set(AzureCredentialVendor.CONF_AUTH_TYPE, "  managed_identity  ")
    assert(AzureCredentialVendor.getAuthType(conf) == Some("managed_identity"))
  }

  test("getAuthType returns None for empty string") {
    val conf = new Configuration()
    conf.set(AzureCredentialVendor.CONF_AUTH_TYPE, "")
    assert(AzureCredentialVendor.getAuthType(conf).isEmpty)
  }

  test("getManagedIdentityClientId returns None when not configured") {
    val conf = new Configuration()
    assert(AzureCredentialVendor.getManagedIdentityClientId(conf).isEmpty)
  }

  test("getManagedIdentityClientId returns UAMI client ID") {
    val conf = new Configuration()
    conf.set(AzureCredentialVendor.CONF_MANAGED_IDENTITY_CLIENT_ID, "uami-client-id-123")
    assert(AzureCredentialVendor.getManagedIdentityClientId(conf) == Some("uami-client-id-123"))
  }

  test("getManagedIdentityClientId trims whitespace") {
    val conf = new Configuration()
    conf.set(AzureCredentialVendor.CONF_MANAGED_IDENTITY_CLIENT_ID, "  uami-123  ")
    assert(AzureCredentialVendor.getManagedIdentityClientId(conf) == Some("uami-123"))
  }

  test("vendAzureCredential throws for unknown auth type") {
    val conf = new Configuration()
    conf.set(AzureCredentialVendor.CONF_AUTH_TYPE, "invalid_type")
    val vendor = new AzureCredentialVendor(conf)
    val context = io.delta.sharing.server.credential.CredentialContext.create(
      new URI("abfss://container@account.dfs.core.windows.net/path"),
      Set(io.delta.sharing.server.credential.Privilege.SELECT)
    )
    val e = intercept[IllegalArgumentException] {
      vendor.vendAzureCredential(context, 3600)
    }
    assert(e.getMessage.contains("Unknown delta.sharing.azure.auth.type"))
    assert(e.getMessage.contains("invalid_type"))
  }

  test("vendAzureCredential throws for client_credentials without config") {
    val conf = new Configuration()
    conf.set(AzureCredentialVendor.CONF_AUTH_TYPE, "client_credentials")
    val vendor = new AzureCredentialVendor(conf)
    val context = io.delta.sharing.server.credential.CredentialContext.create(
      new URI("abfss://container@account.dfs.core.windows.net/path"),
      Set(io.delta.sharing.server.credential.Privilege.SELECT)
    )
    val e = intercept[RuntimeException] {
      vendor.vendAzureCredential(context, 3600)
    }
    assert(e.getMessage.contains("client_credentials"))
    assert(e.getMessage.contains("tenant/client/secret configuration is missing"))
  }
}
