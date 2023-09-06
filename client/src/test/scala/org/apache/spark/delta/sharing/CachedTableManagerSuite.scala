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

import io.delta.sharing.client.{DeltaSharingProfile, DeltaSharingProfileProvider}

private class TestDeltaSharingProfileProvider extends DeltaSharingProfileProvider {
  override def getProfile: DeltaSharingProfile = null

  override def getCustomTablePath(tablePath: String): String = "prefix." + tablePath
}

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
      val provider = new TestDeltaSharingProfileProvider
      manager.register(
        "test-table-path",
        Map("id1" -> "url1", "id2" -> "url2"),
        Seq(new WeakReference(ref)),
        provider,
        _ => {
          TableRefreshResult(Map("id1" -> "url1", "id2" -> "url2"), None, None)
        },
        refreshToken = None)
      assert(manager.getPreSignedUrl(provider.getCustomTablePath("test-table-path"),
        "id1")._1 == "url1")
      assert(manager.getPreSignedUrl(provider.getCustomTablePath("test-table-path"),
        "id2")._1 == "url2")

      manager.register(
        "test-table-path2",
        Map("id1" -> "url1", "id2" -> "url2"),
        Seq(new WeakReference(ref)),
        provider,
        _ => {
          TableRefreshResult(Map("id1" -> "url3", "id2" -> "url4"), None, None)
        },
        refreshToken = None)
      // We should get the new urls eventually
      eventually(timeout(10.seconds)) {
        assert(manager.getPreSignedUrl(provider.getCustomTablePath("test-table-path2"),
          "id1")._1 == "url3")
        assert(manager.getPreSignedUrl(provider.getCustomTablePath("test-table-path2"),
          "id2")._1 == "url4")
      }

      manager.register(
        "test-table-path3",
        Map("id1" -> "url1", "id2" -> "url2"),
        Seq(new WeakReference(new AnyRef)),
        provider,
        _ => {
          TableRefreshResult(Map("id1" -> "url3", "id2" -> "url4"), None, None)
        },
        refreshToken = None)
      // We should remove the cached table eventually
      eventually(timeout(10.seconds)) {
        System.gc()
        intercept[IllegalStateException](manager.getPreSignedUrl(
          provider.getCustomTablePath("test-table-path3"), "id1"))
        intercept[IllegalStateException](manager.getPreSignedUrl(
          provider.getCustomTablePath("test-table-path3"), "id1"))
      }

      manager.register(
        "test-table-path4",
        Map("id1" -> "url1", "id2" -> "url2"),
        Seq(new WeakReference(ref)),
        provider,
        _ => {
          TableRefreshResult(Map("id1" -> "url3", "id2" -> "url4"), None, None)
        },
        -1,
        refreshToken = None
      )
      // We should get new urls immediately because it's refreshed upon register
      assert(manager.getPreSignedUrl(provider.getCustomTablePath("test-table-path4"),
        "id1")._1 == "url3")
      assert(manager.getPreSignedUrl(provider.getCustomTablePath("test-table-path4"),
        "id2")._1 == "url4")
    } finally {
      manager.stop()
    }
  }

  test("refresh based on url expiration") {
    val manager = new CachedTableManager(
      preSignedUrlExpirationMs = 6000,
      refreshCheckIntervalMs = 1000,
      refreshThresholdMs = 1000,
      expireAfterAccessMs = 60000
    )
    try {
      val ref = new AnyRef
      val provider = new TestDeltaSharingProfileProvider
      var refreshTime = 0
      manager.register(
        "test-table-path",
        Map("id1" -> "url1", "id2" -> "url2"),
        Seq(new WeakReference(ref)),
        provider,
        _ => {
          refreshTime += 1
          TableRefreshResult(
            Map("id1" -> ("url" + refreshTime.toString), "id2" -> "url4"),
            Some(System.currentTimeMillis() + 1900),
            None
          )
        },
        System.currentTimeMillis() + 1900,
        None
      )
      // We should refresh at least 5 times within 10 seconds based on
      // (System.currentTimeMillis() + 1900).
      eventually(timeout(10.seconds)) {
        assert(manager.getPreSignedUrl(provider.getCustomTablePath("test-table-path"),
          "id1")._1 == "url5")
        assert(manager.getPreSignedUrl(provider.getCustomTablePath("test-table-path"),
          "id2")._1 == "url4")
      }

      var refreshTime2 = 0
      manager.register(
        "test-table-path2",
        Map("id1" -> "url1", "id2" -> "url2"),
        Seq(new WeakReference(ref)),
        provider,
        _ => {
          refreshTime2 += 1
          TableRefreshResult(
            Map("id1" -> ("url" + refreshTime2.toString), "id2" -> "url4"),
            Some(System.currentTimeMillis() + 4900),
            None
          )
        },
        System.currentTimeMillis() + 4900,
        None
      )
      // We should refresh 2 times within 10 seconds based on (System.currentTimeMillis() + 4900).
      eventually(timeout(10.seconds)) {
        assert(manager.getPreSignedUrl(provider.getCustomTablePath("test-table-path2"),
          "id1")._1 == "url2")
        assert(manager.getPreSignedUrl(provider.getCustomTablePath("test-table-path2"),
          "id2")._1 == "url4")
      }

      var refreshTime3 = 0
      manager.register(
        "test-table-path3",
        Map("id1" -> "url1", "id2" -> "url2"),
        Seq(new WeakReference(ref)),
        provider,
        _ => {
          refreshTime3 += 1
          TableRefreshResult(
            Map("id1" -> ("url" + refreshTime3.toString), "id2" -> "url4"),
            Some(System.currentTimeMillis() - 4900),
            None
          )
        },
        System.currentTimeMillis() + 6000,
        None
      )
      // We should refresh 1 times within 10 seconds based on (preSignedUrlExpirationMs = 6000).
      try {
        eventually(timeout(10.seconds)) {
          assert(manager.getPreSignedUrl(provider.getCustomTablePath("test-table-path3"),
            "id1")._1 == "url2")
          assert(manager.getPreSignedUrl(provider.getCustomTablePath("test-table-path3"),
            "id2")._1 == "url4")
        }
      } catch {
        case e: Throwable =>
          assert(e.getMessage.contains("did not equal"))
      }
    } finally {
      manager.stop()
    }
  }

  test("refresh using refresh token") {
    val manager = new CachedTableManager(
      preSignedUrlExpirationMs = 10,
      refreshCheckIntervalMs = 10,
      refreshThresholdMs = 10,
      expireAfterAccessMs = 60000
    )
    try {
      val ref = new AnyRef
      val provider = new TestDeltaSharingProfileProvider
      manager.register(
        "test-table-path",
        Map("id1" -> "url1", "id2" -> "url2"),
        Seq(new WeakReference(ref)),
        provider,
        refreshToken => {
          if (refreshToken.contains("refresh-token-1")) {
            TableRefreshResult(
              Map("id1" -> "url3", "id2" -> "url4"),
              None,
              Some("refresh-token-2")
            )
          } else if (refreshToken.contains("refresh-token-2")) {
            TableRefreshResult(
              Map("id1" -> "url5", "id2" -> "url6"),
              None,
              Some("refresh-token-2")
            )
          } else {
            fail("Expecting to refresh with a refresh token")
          }
        },
        refreshToken = Some("refresh-token-1")
      )
      // We should get url5 and url6 eventually.
      eventually(timeout(10.seconds)) {
        assert(manager.getPreSignedUrl(provider.getCustomTablePath("test-table-path"),
          "id1")._1 == "url5")
        assert(manager.getPreSignedUrl(provider.getCustomTablePath("test-table-path"),
          "id2")._1 == "url6")
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
      val provider = new TestDeltaSharingProfileProvider

      manager.register(
        "test-table-path",
        Map("id1" -> "url1", "id2" -> "url2"),
        Seq(new WeakReference(ref)),
        provider,
        _ => {
          TableRefreshResult(Map("id1" -> "url1", "id2" -> "url2"), None, None)
        },
        refreshToken = None)
      Thread.sleep(1000)
      // We should remove the cached table when it's not accessed
      intercept[IllegalStateException](manager.getPreSignedUrl(
        provider.getCustomTablePath("test-table-path"), "id1")
      )
    } finally {
      manager.stop()
    }
  }
}
