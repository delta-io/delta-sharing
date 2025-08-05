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
import java.util.concurrent.TimeUnit

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.test.SharedSparkSession
import org.scalatest.time.SpanSugar._

import io.delta.sharing.client.{DeltaSharingProfile, DeltaSharingProfileProvider}

private class TestDeltaSharingProfileProvider extends DeltaSharingProfileProvider {
  override def getProfile: DeltaSharingProfile = null

  override def getCustomTablePath(tablePath: String): String = "prefix." + tablePath
}

private class TestQuerySpecificProfileProviderWithQueryState(
    queryId: String,
    refresherWrapper: QuerySpecificCachedTable.RefresherWrapper)
  extends DeltaSharingProfileProvider {
  override def getCustomQueryId(): Option[String] = Some(queryId)

  override def getCustomRefresherWrapper(): Option[QuerySpecificCachedTable.RefresherWrapper] =
    Some(refresherWrapper)

  override def getProfile(): DeltaSharingProfile = null
}

class CachedTableManagerSuite extends SparkFunSuite with SharedSparkSession{
  private val preSignedUrlExpirationMs = TimeUnit.HOURS.toMillis(1)
  private val refreshCheckIntervalMs = TimeUnit.MINUTES.toMillis(1)
  private val refreshThresholdMs = TimeUnit.MINUTES.toMillis(15)
  private val expireAfterAccessMs = TimeUnit.HOURS.toMillis(1)

  private def createManager(): CachedTableManager = {
    new CachedTableManager(
      preSignedUrlExpirationMs,
      refreshCheckIntervalMs,
      refreshThresholdMs,
      expireAfterAccessMs
    )
  }

  private def createProfileProvider(
      queryId: String,
      refresherWrapper: QuerySpecificCachedTable.RefresherWrapper): DeltaSharingProfileProvider = {
    new TestQuerySpecificProfileProviderWithQueryState(queryId, refresherWrapper)
  }

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

  test("QuerySpecificCachedTable - basic registration and retrieval") {
    val spark = SparkSession.active
    spark.sessionState.conf.setConfString(
      "spark.delta.sharing.client.sparkParquetIOCache.enabled", "true")
    val manager = createManager()
    val tablePath = "test_table"
    val fileId = "file1"
    val url = "https://test.com/file1"
    val queryId = "query1"

    val refresherWrapper: QuerySpecificCachedTable.RefresherWrapper =
      (token, refresher) => refresher(token)

    val refresher: Option[String] => TableRefreshResult = _ =>
      TableRefreshResult(
        Map(fileId -> url), Some(System.currentTimeMillis() + preSignedUrlExpirationMs), None)

    val profileProvider = createProfileProvider(queryId, refresherWrapper)
    val ref = new WeakReference[AnyRef](new Object())

    val expectedExpiration = System.currentTimeMillis() + preSignedUrlExpirationMs
    manager.register(
      tablePath,
      Map(fileId -> url),
      Seq(ref),
      profileProvider,
      refresher,
      expectedExpiration,
      None
    )

    val (retrievedUrl, actualExpiration) = manager.getPreSignedUrl(tablePath, fileId)
    assert(retrievedUrl === url)
    // The actual expiration should be at least as large as the expected expiration
    assert(actualExpiration == expectedExpiration)
  }

  test("QuerySpecificCachedTable - multiple queries share same table") {
    val spark = SparkSession.active
    spark.sessionState.conf.setConfString(
      "spark.delta.sharing.client.sparkParquetIOCache.enabled", "true")
    val manager = createManager()
    val tablePath = "test_table"
    val fileId = "file1"
    val initialUrl = "https://test.com/file1"
    val refreshedUrl = "https://test.com/file1-refreshed"

    // Track which wrapper was used for refresh
    var lastUsedWrapper: String = "none"

    val refresherWrapper1: QuerySpecificCachedTable.RefresherWrapper =
      (token, refresher) => {
        lastUsedWrapper = "wrapper1"
        refresher(token)
      }

    val refresherWrapper2: QuerySpecificCachedTable.RefresherWrapper =
      (token, refresher) => {
        lastUsedWrapper = "wrapper2"
        refresher(token)
      }

    // Single shared refresh function
    val refresher: Option[String] => TableRefreshResult = _ => {
      val newExpiration = System.currentTimeMillis() + refreshThresholdMs + 100
      TableRefreshResult(
        Map(fileId -> refreshedUrl),
        Some(newExpiration),
        None)
    }

    // Register first query with short expiration
    val queryId1 = "query1"
    val profileProvider1 = createProfileProvider(queryId1, refresherWrapper1)
    val ref1 = new WeakReference[AnyRef](new Object())

    manager.register(
      tablePath,
      Map(fileId -> initialUrl),
      Seq(ref1),
      profileProvider1,
      refresher,
      System.currentTimeMillis() + refreshThresholdMs + 100, // Short expiration
      None
    )

    // Register second query with short expiration
    val queryId2 = "query2"
    val profileProvider2 = createProfileProvider(queryId2, refresherWrapper2)
    val ref2 = new WeakReference[AnyRef](new Object())

    manager.register(
      tablePath,
      Map(fileId -> initialUrl),
      Seq(ref2),
      profileProvider2,
      refresher,
      System.currentTimeMillis() + refreshThresholdMs + 100, // Short expiration
      None
    )

    assert(manager.size == 1)

    // Sleep to let the URLs expire
    Thread.sleep(200)

    // Trigger refresh - only one wrapper should be used
    manager.refresh()

    // Verify only one wrapper was used
    assert(lastUsedWrapper === "wrapper1" || lastUsedWrapper === "wrapper2")

    // Get URL - should be from the shared refresher
    val (retrievedUrl, _) = manager.getPreSignedUrl(tablePath, fileId)
    assert(retrievedUrl === refreshedUrl)

    // Clear first query's reference
    ref1.clear()

    // Sleep again to let the URLs expire
    Thread.sleep(200)

    // Trigger refresh again - now only second wrapper should be used
    manager.refresh()

    // Verify second wrapper was used
    assert(manager.size == 1)
    assert(lastUsedWrapper === "wrapper2")

    // URL should still be accessible
    val (retrievedUrl2, _) = manager.getPreSignedUrl(tablePath, fileId)
    assert(retrievedUrl2 === refreshedUrl)

    // Clear second query's reference
    ref2.clear()

    // Trigger refresh to clean up
    manager.refresh()
    // The cache should be empty
    assert(manager.size == 0)
  }

  test("QuerySpecificCachedTable - multiple queries on different tables with refresh states") {
    val spark = SparkSession.active
    spark.sessionState.conf.setConfString(
      "spark.delta.sharing.client.sparkParquetIOCache.enabled", "true")
    val manager = createManager()
    val tablePath1 = "test_table1"
    val tablePath2 = "test_table2"
    val fileId = "file1"
    val initialUrl1 = "https://test.com/file1"
    val initialUrl2 = "https://test.com/file2"
    val refreshedUrl1 = "https://test.com/file1-refreshed"
    val refreshedUrl2 = "https://test.com/file2-refreshed"

    // Track refresh states for each table
    var table1RefreshCount = 0
    var table2RefreshCount = 0

    val refresherWrapper1: QuerySpecificCachedTable.RefresherWrapper =
      (token, refresher) => {
        table1RefreshCount += 1
        refresher(token)
      }

    val refresherWrapper2: QuerySpecificCachedTable.RefresherWrapper =
      (token, refresher) => {
        table2RefreshCount += 1
        refresher(token)
      }

    // Different refresh functions for different tables
    val refresher1: Option[String] => TableRefreshResult = _ => {
      val newExpiration = System.currentTimeMillis() + refreshThresholdMs + 100
      TableRefreshResult(
        Map(fileId -> refreshedUrl1),
        Some(newExpiration),
        None)
    }

    val refresher2: Option[String] => TableRefreshResult = _ => {
      val newExpiration = System.currentTimeMillis() + refreshThresholdMs + 100
      TableRefreshResult(
        Map(fileId -> refreshedUrl2),
        Some(newExpiration),
        None)
    }

    // Register first query for first table with short expiration
    val queryId1 = "query1"
    val profileProvider1 = createProfileProvider(queryId1, refresherWrapper1)
    val ref1 = new WeakReference[AnyRef](new Object())

    manager.register(
      tablePath1,
      Map(fileId -> initialUrl1),
      Seq(ref1),
      profileProvider1,
      refresher1,
      System.currentTimeMillis() + refreshThresholdMs + 100, // Short expiration
      None
    )

    // Register second query for second table with short expiration
    val queryId2 = "query2"
    val profileProvider2 = createProfileProvider(queryId2, refresherWrapper2)
    val ref2 = new WeakReference[AnyRef](new Object())

    manager.register(
      tablePath2,
      Map(fileId -> initialUrl2),
      Seq(ref2),
      profileProvider2,
      refresher2,
      System.currentTimeMillis() + refreshThresholdMs + 100, // Short expiration
      None
    )

    assert(manager.size == 2)

    // Sleep to let the URLs expire
    Thread.sleep(200)

    // Trigger refresh - both tables should be refreshed since URLs have expired
    manager.refresh()

    // Verify both tables were refreshed
    assert(table1RefreshCount === 1)
    assert(table2RefreshCount === 1)

    // Get URLs - should be from their respective refreshers
    val (retrievedUrl1, _) = manager.getPreSignedUrl(tablePath1, fileId)
    val (retrievedUrl2, _) = manager.getPreSignedUrl(tablePath2, fileId)
    assert(retrievedUrl1 === refreshedUrl1)
    assert(retrievedUrl2 === refreshedUrl2)

    // Clear first query's reference
    ref1.clear()

    // Sleep again to let the URLs expire
    Thread.sleep(200)

    // Trigger refresh again - only second table should be refreshed
    manager.refresh()

    // Verify only second table was refreshed again
    assert(table1RefreshCount === 1) // Should not change
    assert(table2RefreshCount === 2) // Should increment

    // First table should be removed from cache
    assert(manager.size == 1)

    // First table's URL should no longer be accessible
    intercept[IllegalStateException] {
      manager.getPreSignedUrl(tablePath1, fileId)
    }

    // Second table's URL should still be accessible
    val (retrievedUrl2Again, _) = manager.getPreSignedUrl(tablePath2, fileId)
    assert(retrievedUrl2Again === refreshedUrl2)
  }

  test("QuerySpecificCachedTable - expiration handling") {
    val spark = SparkSession.active
    spark.sessionState.conf.setConfString(
      "spark.delta.sharing.client.sparkParquetIOCache.enabled", "true")
    val manager = createManager()
    val tablePath = "test_table"
    val fileId = "file1"
    val initialUrl = "https://test.com/file1"
    val refreshedUrl = "https://test.com/file1-refreshed"

    val refresherWrapper: QuerySpecificCachedTable.RefresherWrapper =
      (token, refresher) => refresher(token)

    val refresher: Option[String] => TableRefreshResult = _ =>
      TableRefreshResult(
        Map(fileId -> refreshedUrl),
        Some(System.currentTimeMillis() + preSignedUrlExpirationMs),
        None
      )

    val profileProvider = createProfileProvider("query1", refresherWrapper)
    val ref = new WeakReference[AnyRef](new Object())

    // Register with expired timestamp
    manager.register(
      tablePath,
      Map(fileId -> initialUrl),
      Seq(ref),
      profileProvider,
      refresher,
      System.currentTimeMillis(), // Already expired
      None
    )

    // Verify URL was refreshed immediately
    val (retrievedUrl, _) = manager.getPreSignedUrl(tablePath, fileId)
    assert(retrievedUrl === refreshedUrl)
  }

  test("QuerySpecificCachedTable - refresh token handling") {
    val spark = SparkSession.active
    spark.sessionState.conf.setConfString(
      "spark.delta.sharing.client.sparkParquetIOCache.enabled", "true")
    val manager = createManager()
    val tablePath = "test_table"
    val fileId = "file1"
    val initialUrl = "https://test.com/file1"
    val refreshedUrl = "https://test.com/file1-refreshed"
    val refreshToken = "token123"

    val refresherWrapper: QuerySpecificCachedTable.RefresherWrapper =
      (token, refresher) => refresher(token)

    var receivedTokens = Seq.empty[Option[String]]
    val refresher: Option[String] => TableRefreshResult = token => {
      receivedTokens = receivedTokens :+ token
      TableRefreshResult(
        Map(fileId -> refreshedUrl),
        Some(System.currentTimeMillis() + refreshThresholdMs + 100),
        Some(refreshToken)
      )
    }

    val profileProvider = createProfileProvider("query1", refresherWrapper)
    val ref = new WeakReference[AnyRef](new Object())

    // Register with no refresh token and short expiration to trigger refresh
    manager.register(
      tablePath,
      Map(fileId -> initialUrl),
      Seq(ref),
      profileProvider,
      refresher,
      System.currentTimeMillis() + refreshThresholdMs + 100,
      None
    )

    // Sleep to let the URLs expire
    Thread.sleep(200)

    // First refresh - should receive None as token
    manager.refresh()
    assert(receivedTokens === Seq(None))

    // Sleep again to let the URLs expire
    Thread.sleep(200)

    // Second refresh - should receive the token from first refresh
    manager.refresh()
    assert(receivedTokens === Seq(None, Some(refreshToken)))

    // Verify we got the refreshed URL
    val (retrievedUrl, _) = manager.getPreSignedUrl(tablePath, fileId)
    assert(retrievedUrl === refreshedUrl)
  }

  test("QuerySpecificCachedTable - multi-threaded register with 10 queries") {
    val spark = SparkSession.active
    spark.sessionState.conf.setConfString(
      "spark.delta.sharing.client.sparkParquetIOCache.enabled", "true")
    val manager = createManager()
    val tablePath = "test_table"
    val fileId = "file1"
    val initialUrl = "https://test.com/file1"
    val refreshedUrl = "https://test.com/file1-refreshed"

    val refresherWrapper: QuerySpecificCachedTable.RefresherWrapper =
      (token, refresher) => {
        refresher(token)
      }

    // Single shared refresh function
    val refresher: Option[String] => TableRefreshResult = _ => {
      val newExpiration = System.currentTimeMillis() + refreshThresholdMs + 100
      TableRefreshResult(
        Map(fileId -> refreshedUrl),
        Some(newExpiration),
        None)
    }

    val refs = (0 to 9).map { i =>
      new WeakReference[AnyRef](new Object())
    }
    val threads = (0 to 9).map { i =>
      val queryId = s"query$i"
      val profileProvider = createProfileProvider(queryId, refresherWrapper)
      new Thread(new Runnable {
        override def run(): Unit = {
          manager.register(
            tablePath,
            Map(fileId -> initialUrl),
            Seq(refs(i)),
            profileProvider,
            refresher,
            System.currentTimeMillis() + refreshThresholdMs + 100,
            None
          )
        }
      })
    }

    threads.foreach(_.start())
    threads.foreach(_.join())

    // Verify the cache size and URL
    assert(manager.size == 1)
    assert(manager.getQueryStateSize(tablePath) == 10)
    val (retrievedUrl1, _) = manager.getPreSignedUrl(tablePath, fileId)
    assert(retrievedUrl1 === initialUrl)

    // Clear some references
    refs.take(5).foreach(_.clear())
    manager.refresh()
    assert(manager.size == 1)
    assert(manager.getQueryStateSize(tablePath) == 5)
    val (retrievedUrl2, _) = manager.getPreSignedUrl(tablePath, fileId)
    assert(retrievedUrl2 === initialUrl)

    // Clear all references
    refs.foreach(_.clear())
    manager.refresh()
    assert(manager.size == 0)
  }

  test("QuerySpecificCachedTable - race condition between register and refresh") {
    val spark = SparkSession.active
    spark.sessionState.conf.setConfString(
      "spark.delta.sharing.client.sparkParquetIOCache.enabled", "true")
    val manager = new CachedTableManager(
      0, // force always refresh
      refreshCheckIntervalMs,
      refreshThresholdMs,
      expireAfterAccessMs
    )
    val tablePath = "test_table"
    val fileId = "file1"
    val initialUrl = "https://test.com/file1"
    val refreshedUrl = "https://test.com/file1-refreshed"

    val refresherWrapper: QuerySpecificCachedTable.RefresherWrapper =
      (token, refresher) => refresher(token)

    // Always return expired timestamp to force refresh
    val refresher: Option[String] => TableRefreshResult = _ =>
      TableRefreshResult(
        Map(fileId -> refreshedUrl),
        Some(System.currentTimeMillis()), // Force refresh
        None)

    // Create two sets of references for two phases of testing
    val refs1 = (0 to 9).map(_ => new WeakReference[AnyRef](new Object()))
    val refs2 = (0 to 9).map(_ => new WeakReference[AnyRef](new Object()))

    // Phase 1: Initial concurrent registration and refresh
    val registerThreads1 = (0 to 9).map { i =>
      val queryId = s"query$i"
      val profileProvider = createProfileProvider(queryId, refresherWrapper)

      new Thread(new Runnable {
        override def run(): Unit = {
          manager.register(
            tablePath,
            Map(fileId -> initialUrl),
            Seq(refs1(i)),
            profileProvider,
            refresher,
            System.currentTimeMillis(), // Force refresh
            None
          )
        }
      })
    }

    val refreshThread1 = new Thread(new Runnable {
      override def run(): Unit = {
        (0 to 40).foreach { _ => manager.refresh() }
      }
    })

    // Start phase 1
    registerThreads1.foreach(_.start())
    refreshThread1.start()
    registerThreads1.foreach(_.join())
    refreshThread1.join()

    // Verify phase 1 state
    assert(manager.size == 1)
    assert(manager.getQueryStateSize(tablePath) == 10)
    val (retrievedUrl1, _) = manager.getPreSignedUrl(tablePath, fileId)
    assert(retrievedUrl1 === refreshedUrl)

    // Phase 2: Clear some references and add new ones
    refs1.take(5).foreach(_.clear())

    val registerThreads2 = (0 to 9).map { i =>
      val queryId = s"query${i + 10}" // Different query IDs
      val profileProvider = createProfileProvider(queryId, refresherWrapper)

      new Thread(new Runnable {
        override def run(): Unit = {
          manager.register(
            tablePath,
            Map(fileId -> initialUrl),
            Seq(refs2(i)),
            profileProvider,
            refresher,
            System.currentTimeMillis(), // Force refresh
            None
          )
        }
      })
    }

    val refreshThread2 = new Thread(new Runnable {
      override def run(): Unit = {
        (0 to 40).foreach { _ => manager.refresh() }
      }
    })

    // Start phase 2
    registerThreads2.foreach(_.start())
    refreshThread2.start()
    registerThreads2.foreach(_.join())
    refreshThread2.join()

    // Verify phase 2 state
    assert(manager.size == 1)
    assert(manager.getQueryStateSize(tablePath) == 15) // 5 from phase 1 + 10 from phase 2
    val (retrievedUrl2, _) = manager.getPreSignedUrl(tablePath, fileId)
    assert(retrievedUrl2 === refreshedUrl)

    // Phase 3: Clear all references
    refs1.foreach(_.clear())
    refs2.foreach(_.clear())
    manager.refresh()
    assert(manager.size == 0)
  }

  test("registerQueryStatesInQuerySpecificCachedTable only adds new query states") {
    // Basic set up
    SparkSession.active.sessionState.conf.setConfString(
      "spark.delta.sharing.client.sparkParquetIOCache.enabled", "true")
    val manager = createManager()
    val tablePath = "test_table"
    val fileId = "file1"
    val url1 = "https://test.com/file1"
    val url2 = "https://test.com/file2"
    val queryId1 = "query1"
    val queryId2 = "query2"

    val refresherWrapper: QuerySpecificCachedTable.RefresherWrapper =
      (token, refresher) => refresher(token)
    val refresher: Option[String] => TableRefreshResult = _ =>
      TableRefreshResult(Map(fileId -> url2), Some(System.currentTimeMillis() + 60000), None)

    val profileProvider1 = createProfileProvider(queryId1, refresherWrapper)
    val profileProvider2 = createProfileProvider(queryId2, refresherWrapper)

    // Register initial QuerySpecificCachedTable with queryId1
    val ref1 = new WeakReference[AnyRef](new Object())
    manager.register(
      tablePath,
      Map(fileId -> url1),
      Seq(ref1),
      profileProvider1,
      refresher,
      System.currentTimeMillis() + refreshThresholdMs + 100,
      None
    )

    // Add queryId2 using the new method
    manager.registerQueryStatesInQuerySpecificCachedTable(
      tablePath,
      Seq(new WeakReference[AnyRef](new Object())),
      profileProvider2
    )

    // Check that both query states are present
    assert(manager.size == 1)
    assert(manager.getQueryStateSize(tablePath) == 2)

    val (retrievedUrl1, _) = manager.getPreSignedUrl(tablePath, fileId)
    assert(retrievedUrl1 === url1)

    // Sleep to let the URLs expire
    ref1.clear()
    Thread.sleep(200)
    manager.refresh()

    // Refresh still works
    assert(manager.getQueryStateSize(tablePath) == 1)
    val (retrievedUrl2, _) = manager.getPreSignedUrl(tablePath, fileId)
    assert(retrievedUrl2 === url2)
  }

  test("QuerySpecificCachedTable - keepUrlsAfterRefsGone functionality") {
    val spark = SparkSession.active
    spark.sessionState.conf.setConfString(
      "spark.delta.sharing.client.sparkParquetIOCache.enabled", "true")
    val manager = createManager()
    val tablePath = "test_table"
    val fileId = "file1"
    val initialUrl = "https://test.com/file1"
    val refreshedUrl = "https://test.com/file1-refreshed"

    val refresherWrapper: QuerySpecificCachedTable.RefresherWrapper =
      (token, refresher) => refresher(token)

    val refresher: Option[String] => TableRefreshResult = _ =>
      TableRefreshResult(
        Map(fileId -> refreshedUrl),
        Some(System.currentTimeMillis() + preSignedUrlExpirationMs),
        None
      )

    val profileProvider = createProfileProvider("query1", refresherWrapper)
    val ref = new WeakReference[AnyRef](new Object())

    // Register with keepUrlsAfterRefsGone = true
    manager.register(
      tablePath,
      Map(fileId -> initialUrl),
      Seq(ref),
      profileProvider,
      refresher,
      System.currentTimeMillis() + refreshThresholdMs + 100,
      None,
      keepUrlsAfterRefsGone = true
    )

    // Verify initial state
    assert(manager.size == 1)
    assert(manager.getQueryStateSize(tablePath) == 1)
    val (retrievedUrl1, _) = manager.getPreSignedUrl(tablePath, fileId)
    assert(retrievedUrl1 === initialUrl)

    // Clear the reference
    ref.clear()

    // Trigger refresh - should keep the URLs even though all refs are gone
    manager.refresh()

    // Verify URLs are still accessible
    assert(manager.size == 1)
    assert(manager.getQueryStateSize(tablePath) == 0) // No valid query states
    val (retrievedUrl2, _) = manager.getPreSignedUrl(tablePath, fileId)
    assert(retrievedUrl2 === initialUrl) // Should still have the original URL
  }

  test("QuerySpecificCachedTable - keepUrlsAfterRefsGone true but URLs about to expire") {
    val spark = SparkSession.active
    spark.sessionState.conf.setConfString(
      "spark.delta.sharing.client.sparkParquetIOCache.enabled", "true")
    val manager = createManager()
    val tablePath = "test_table"
    val fileId = "file1"
    val initialUrl = "https://test.com/file1"

    val refresherWrapper: QuerySpecificCachedTable.RefresherWrapper =
      (token, refresher) => refresher(token)

    val refresher: Option[String] => TableRefreshResult = _ =>
      TableRefreshResult(
        Map(fileId -> initialUrl),
        Some(System.currentTimeMillis() + preSignedUrlExpirationMs),
        None
      )

    val profileProvider = createProfileProvider("query1", refresherWrapper)
    val ref = new WeakReference[AnyRef](new Object())

    // Register with keepUrlsAfterRefsGone = true but URLs about to expire
    manager.register(
      tablePath,
      Map(fileId -> initialUrl),
      Seq(ref),
      profileProvider,
      refresher,
      System.currentTimeMillis() + refreshThresholdMs + 100,
      None,
      keepUrlsAfterRefsGone = true
    )

    // Verify initial state
    assert(manager.size == 1)
    assert(manager.getQueryStateSize(tablePath) == 1)

    // Clear the reference
    ref.clear()
    Thread.sleep(150)

    // Trigger refresh - should remove the table since URLs are about to expire and no valid states
    manager.refresh()

    // Verify table was removed
    assert(manager.size == 0)

    // Verify URLs are no longer accessible
    intercept[IllegalStateException] {
      manager.getPreSignedUrl(tablePath, fileId)
    }
  }

  test("QuerySpecificCachedTable - merge URLs and query states") {
    val spark = SparkSession.active
    spark.sessionState.conf.setConfString(
      "spark.delta.sharing.client.sparkParquetIOCache.enabled", "true")
    val manager = createManager()
    val tablePath = "test_table"
    val fileId1 = "file1"
    val fileId2 = "file2"
    val fileId3 = "file3"
    val initialUrl1 = "https://test.com/file1"
    val initialUrl2 = "https://test.com/file2"
    val initialUrl3 = "https://test.com/file3"
    val updatedUrl1 = "https://test.com/file1-updated"

    val refresherWrapper: QuerySpecificCachedTable.RefresherWrapper =
      (token, refresher) => refresher(token)

    // First registration with two files
    val refresher1: Option[String] => TableRefreshResult = _ =>
      TableRefreshResult(
        Map(fileId1 -> initialUrl1, fileId2 -> initialUrl2),
        Some(System.currentTimeMillis() + preSignedUrlExpirationMs),
        None
      )

    val profileProvider1 = createProfileProvider("query1", refresherWrapper)
    val ref1 = new WeakReference[AnyRef](new Object())

    manager.register(
      tablePath,
      Map(fileId1 -> initialUrl1, fileId2 -> initialUrl2),
      Seq(ref1),
      profileProvider1,
      refresher1,
      System.currentTimeMillis() + refreshThresholdMs + 100,
      None
    )

    // Verify initial state
    assert(manager.size == 1)
    assert(manager.getQueryStateSize(tablePath) == 1)

    // Second registration with updated URL for file1 and new file3
    val refresher2: Option[String] => TableRefreshResult = _ =>
      TableRefreshResult(
        Map(fileId1 -> updatedUrl1, fileId3 -> initialUrl3),
        Some(System.currentTimeMillis() + preSignedUrlExpirationMs),
        None
      )

    val profileProvider2 = createProfileProvider("query2", refresherWrapper)
    val ref2 = new WeakReference[AnyRef](new Object())

    manager.register(
      tablePath,
      Map(fileId1 -> updatedUrl1, fileId3 -> initialUrl3),
      Seq(ref2),
      profileProvider2,
      refresher2,
      System.currentTimeMillis() + refreshThresholdMs + 100,
      None
    )

    // Verify URLs were merged
    assert(manager.size == 1)
    assert(manager.getQueryStateSize(tablePath) == 2)

    // Verify merged URLs: file1 should be updated, file2 should remain, file3 should be added
    val (retrievedUrl1, _) = manager.getPreSignedUrl(tablePath, fileId1)
    val (retrievedUrl2, _) = manager.getPreSignedUrl(tablePath, fileId2)
    val (retrievedUrl3, _) = manager.getPreSignedUrl(tablePath, fileId3)
    assert(retrievedUrl1 === updatedUrl1) // Updated URL
    assert(retrievedUrl2 === initialUrl2) // Preserved URL
    assert(retrievedUrl3 === initialUrl3) // New URL

    // Third registration with same queryId as first - should merge refs
    val ref1Additional = new WeakReference[AnyRef](new Object())

    manager.register(
      tablePath,
      Map(fileId1 -> initialUrl1), // Different URL, should be overridden by merge
      Seq(ref1Additional),
      profileProvider1, // Same queryId as first registration
      refresher1,
      System.currentTimeMillis() + refreshThresholdMs + 100,
      None
    )

    // Query states should still be 2 (same queryId, so refs merged)
    assert(manager.size == 1)
    assert(manager.getQueryStateSize(tablePath) == 2)

    // URL for file1 should be the one from the latest registration
    val (retrievedUrl1Final, _) = manager.getPreSignedUrl(tablePath, fileId1)
    assert(retrievedUrl1Final === initialUrl1) // Latest registration wins

    ref1.clear()
    Thread.sleep(150)
    manager.refresh()
    assert(manager.size == 1)
    assert(manager.getQueryStateSize(tablePath) == 2) // ref1Additional is still in the cache

    ref1Additional.clear()
    Thread.sleep(150)
    manager.refresh()
    assert(manager.size == 1)
    assert(manager.getQueryStateSize(tablePath) == 1)
  }
}
