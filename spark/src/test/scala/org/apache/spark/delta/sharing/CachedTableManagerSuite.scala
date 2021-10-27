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

package org.apache.spark.delta.sharing

import java.lang.ref.WeakReference

import org.apache.spark.SparkFunSuite
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.SpanSugar._

class CachedTableManagerSuite extends SparkFunSuite {

  test("cache") {
    val manager = new CachedTableManager(
      preSignedUrlExpirationMs = 10,
      refreshCheckIntervalMs = 10,
      refreshThresholdMs = 10,
      expireAfterAccessMs = 60000
    )
    try {
      val ref = new AnyRef
      manager.register(
        "test-table-path",
        Map("id1" -> "url1", "id2" -> "url2"),
        new WeakReference(ref),
        () => {
          Map("id1" -> "url1", "id2" -> "url2")
        })
      assert(manager.getPreSignedUrl("test-table-path", "id1")._1 == "url1")
      assert(manager.getPreSignedUrl("test-table-path", "id2")._1 == "url2")

      manager.register(
        "test-table-path2",
        Map("id1" -> "url1", "id2" -> "url2"),
        new WeakReference(ref),
        () => {
          Map("id1" -> "url3", "id2" -> "url4")
        })
      // We should get the new urls eventually
      eventually(timeout(10.seconds)) {
        assert(manager.getPreSignedUrl("test-table-path2", "id1")._1 == "url3")
        assert(manager.getPreSignedUrl("test-table-path2", "id2")._1 == "url4")
      }

      manager.register(
        "test-table-path3",
        Map("id1" -> "url1", "id2" -> "url2"),
        new WeakReference(new AnyRef),
        () => {
          Map("id1" -> "url3", "id2" -> "url4")
        })
      // We should remove the cached table eventually
      eventually(timeout(10.seconds)) {
        System.gc()
        intercept[IllegalStateException](manager.getPreSignedUrl("test-table-path3", "id1"))
        intercept[IllegalStateException](manager.getPreSignedUrl("test-table-path3", "id1"))
      }
    } finally {
      manager.stop()
    }
  }

  test("expireAfterAccessMs") {
    val manager = new CachedTableManager(
      preSignedUrlExpirationMs = 10,
      refreshCheckIntervalMs = 10,
      refreshThresholdMs = 10,
      expireAfterAccessMs = 10
    )
    try {
      val ref = new AnyRef
      manager.register(
        "test-table-path",
        Map("id1" -> "url1", "id2" -> "url2"),
        new WeakReference(ref),
        () => {
          Map("id1" -> "url1", "id2" -> "url2")
        })
      Thread.sleep(1000)
      // We should remove the cached table when it's not accessed
      intercept[IllegalStateException](manager.getPreSignedUrl("test-table-path", "id1"))
    } finally {
      manager.stop()
    }
  }
}
