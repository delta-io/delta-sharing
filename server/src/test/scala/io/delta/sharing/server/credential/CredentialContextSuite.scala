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

import org.scalatest.FunSuite

class CredentialContextSuite extends FunSuite {

  test("StorageScheme.fromUri") {
    assert(StorageScheme.fromUri(new URI("s3a://bucket/key")) == StorageScheme.S3)
    assert(StorageScheme.fromUri(new URI("s3://bucket/key")) == StorageScheme.S3)
    assert(StorageScheme.fromUri(
      new URI("wasb://c@account.blob.core.windows.net/p")) == StorageScheme.AzureWasb)
    assert(StorageScheme.fromUri(
      new URI("wasbs://c@account.blob.core.windows.net/p")) == StorageScheme.AzureWasb)
    assert(StorageScheme.fromUri(
      new URI("abfs://c@account.dfs.core.windows.net/p")) == StorageScheme.AzureAbfs)
    assert(StorageScheme.fromUri(new URI("gs://bucket/key")) == StorageScheme.GCS)
    assert(StorageScheme.fromUri(new URI("file:///tmp/path")) == StorageScheme.File)
    assert(StorageScheme.fromUri(new URI("http://example.com/path")) == StorageScheme.Null)
  }

  test("CredentialContext.storageBaseOf") {
    assert(CredentialContext.storageBaseOf(new URI("s3a://mybucket/prefix/key"))
      == new URI("s3a", null, "mybucket", -1, "/", null, null))
    assert(CredentialContext.storageBaseOf(
      new URI("abfss://container@account.dfs.core.windows.net/table/path"))
      == new URI("abfss", "container", "account.dfs.core.windows.net", -1, "/", null, null))
    assert(CredentialContext.storageBaseOf(new URI("gs://bucket/path"))
      == new URI("gs", null, "bucket", -1, "/", null, null))
  }

  test("CredentialContext.create") {
    val uri = new URI("s3a://bucket/table/prefix")
    val ctx = CredentialContext.create(uri, Set(Privilege.SELECT))
    assert(ctx.storageScheme == StorageScheme.S3)
    assert(ctx.storageBase.getHost == "bucket")
    assert(ctx.privileges == Set(Privilege.SELECT))
    assert(ctx.locations == Seq(uri))
  }

  test("CredentialContext.create with UPDATE privilege") {
    val uri = new URI("gs://mybucket/path")
    val ctx = CredentialContext.create(uri, Set(Privilege.SELECT, Privilege.UPDATE))
    assert(ctx.storageScheme == StorageScheme.GCS)
    assert(ctx.privileges == Set(Privilege.SELECT, Privilege.UPDATE))
  }
}
