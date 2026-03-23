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
import org.scalatest.FunSuite

class StorageCredentialVendorSuite extends FunSuite {

  test("vendCredential throws when path is null") {
    val vendor = new StorageCredentialVendor(new Configuration())
    intercept[IllegalArgumentException] {
      vendor.vendCredential(null, Set(Privilege.SELECT), 3600)
    }
    assert(intercept[IllegalArgumentException] {
      vendor.vendCredential(null, Set(Privilege.SELECT), 3600)
    }.getMessage.contains("Storage location is null"))
  }

  test("vendCredential throws when privileges is null") {
    val vendor = new StorageCredentialVendor(new Configuration())
    intercept[IllegalArgumentException] {
      vendor.vendCredential(new URI("s3a://bucket/path"), null, 3600)
    }
    assert(intercept[IllegalArgumentException] {
      vendor.vendCredential(new URI("s3a://bucket/path"), null, 3600)
    }.getMessage.contains("Privileges"))
  }

  test("vendCredential throws when privileges is empty") {
    val vendor = new StorageCredentialVendor(new Configuration())
    intercept[IllegalArgumentException] {
      vendor.vendCredential(new URI("s3a://bucket/path"), Set.empty, 3600)
    }
    assert(intercept[IllegalArgumentException] {
      vendor.vendCredential(new URI("s3a://bucket/path"), Set.empty, 3600)
    }.getMessage.contains("Privileges"))
  }

  test("vendCredential throws for File scheme") {
    val vendor = new StorageCredentialVendor(new Configuration())
    val e = intercept[UnsupportedOperationException] {
      vendor.vendCredential(new URI("file:///tmp/table"), Set(Privilege.SELECT), 3600)
    }
    assert(e.getMessage.contains("File") || e.getMessage.contains("not supported"))
  }

  test("vendCredential throws for unknown scheme") {
    val vendor = new StorageCredentialVendor(new Configuration())
    val e = intercept[UnsupportedOperationException] {
      vendor.vendCredential(new URI("http://example.com/path"), Set(Privilege.SELECT), 3600)
    }
    assert(e.getMessage.contains("not supported"))
  }
}
