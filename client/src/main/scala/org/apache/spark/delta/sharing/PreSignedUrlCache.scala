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
// Use `org.apache.spark` so that we can access Spark's private RPC APIs
package org.apache.spark.delta.sharing

import java.lang.ref.WeakReference
import java.util.concurrent.TimeUnit

import scala.util.control.NonFatal

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcCallContext, RpcEndpoint, RpcEndpointRef, RpcEnv}
import org.apache.spark.util.{RpcUtils, ThreadUtils}

import io.delta.sharing.client.DeltaSharingProfileProvider

case class TableRefreshResult(
    idToUrl: Map[String, String],
    expirationTimestamp: Option[Long],
    refreshToken: Option[String]
)

/**
 * @param expiration the expiration time of the pre signed urls
 * @param idToUrl the file id to pre sign url map
 * @param refs the references that we track. When all of references in the table are gone, we will
 *             remove the cached table from our cache.
 * @param lastAccess When the table was accessed last time. We will remove old tables that are not
 *                   accessed after `expireAfterAccessMs` milliseconds.
 * @param refresher the function to generate a new file id to pre sign url map, with the new
 *                  expiration timestamp of the urls and the new refresh token.
 * @param refreshToken the optional refresh token that can be used by the refresher to retrieve
 *                     the same set of files with refreshed urls.
 */
class CachedTable(
    val expiration: Long,
    val idToUrl: Map[String, String],
    val refs: Seq[WeakReference[AnyRef]],
    @volatile var lastAccess: Long,
    val refresher: Option[String] => TableRefreshResult,
    val refreshToken: Option[String])

class CachedTableManager(
    val preSignedUrlExpirationMs: Long,
    refreshCheckIntervalMs: Long,
    val refreshThresholdMs: Long,
    expireAfterAccessMs: Long) extends Logging {

  private val cache = new java.util.concurrent.ConcurrentHashMap[String, CachedTable]()

  private val refreshThread = {
    val thread = ThreadUtils.newDaemonSingleThreadScheduledExecutor(
      "delta-sharing-pre-signed-url-refresh-thread")
    thread.scheduleWithFixedDelay(
      () => refresh(),
      refreshCheckIntervalMs,
      refreshCheckIntervalMs,
      TimeUnit.MILLISECONDS)
    thread
  }

  def isValidUrlExpirationTime(expiration: Option[Long]): Boolean = {
    // refreshThresholdMs is the buffer time for the refresh RPC.
    // It could also help the client from keeping refreshing endlessly.
    val isValid = expiration.isDefined && (
      expiration.get > (System.currentTimeMillis() + refreshThresholdMs))
    if (!isValid && expiration.isDefined) {
      val currentTs = System.currentTimeMillis()
      logWarning(s"Invalid url expiration timestamp(${expiration}, " +
        s"${new java.util.Date(expiration.get)}), refreshThresholdMs:$refreshThresholdMs, " +
        s"current timestamp(${currentTs}, ${new java.util.Date(currentTs)}).")
    }
    isValid
  }

  def refresh(): Unit = {
    import scala.collection.JavaConverters._
    val snapshot = cache.entrySet().asScala.toArray
    for (entry <- snapshot) {
      val tablePath = entry.getKey
      val cachedTable = entry.getValue
      if (cachedTable.refs.forall(_.get == null)) {
        logInfo(s"Removing table $tablePath from the pre signed url cache as there are" +
          " no references pointed to it")
        cache.remove(tablePath, cachedTable)
      } else if (cachedTable.lastAccess + expireAfterAccessMs < System.currentTimeMillis()) {
        logInfo(s"Removing table $tablePath from the pre signed url cache as it was not accessed " +
          s"after $expireAfterAccessMs ms")
        cache.remove(tablePath, cachedTable)
      } else if (cachedTable.expiration - System.currentTimeMillis() < refreshThresholdMs) {
        logInfo(s"Updating pre signed urls for $tablePath (expiration time: " +
          s"${new java.util.Date(cachedTable.expiration)})")
        try {
          val refreshRes = cachedTable.refresher(cachedTable.refreshToken)
          val newTable = new CachedTable(
            if (isValidUrlExpirationTime(refreshRes.expirationTimestamp)) {
              refreshRes.expirationTimestamp.get
            } else {
              preSignedUrlExpirationMs + System.currentTimeMillis()
            },
            refreshRes.idToUrl,
            cachedTable.refs,
            cachedTable.lastAccess,
            cachedTable.refresher,
            refreshRes.refreshToken
          )
          // Failing to replace the table is fine because if it did happen, we would retry after
          // `refreshCheckIntervalMs` milliseconds.
          cache.replace(tablePath, cachedTable, newTable)
        } catch {
          case NonFatal(e) =>
            logError(s"Failed to refresh pre signed urls for table $tablePath", e)
            if (cachedTable.expiration < System.currentTimeMillis()) {
              logInfo(s"Removing table $tablePath form cache as the pre signed url have expired")
              // Remove the cached table as pre signed urls have expired
              cache.remove(tablePath, cachedTable)
            } else {
              // If the pre signed urls haven't expired, we will keep it in cache so that we can
              // retry the refresh next time.
            }
        }
      }
    }
  }

  /** Returns `PreSignedUrl` from the cache. */
  def getPreSignedUrl(
      tablePath: String,
      fileId: String): PreSignedUrlCache.Rpc.GetPreSignedUrlResponse = {
    val cachedTable = cache.get(tablePath)
    if (cachedTable == null) {
      throw new IllegalStateException(s"table $tablePath was removed")
    }
    cachedTable.lastAccess = System.currentTimeMillis()
    val url = cachedTable.idToUrl.getOrElse(fileId, {
      throw new IllegalStateException(s"cannot find url for id $fileId in table $tablePath")
    })
    (url, cachedTable.expiration)
  }

  /**
   * Register a table path in the cache. The pre signed urls will be refreshed automatically to
   * support long running queries.
   *
   * @param tablePath the table path. This is usually the profile file path.
   * @param idToUrl the pre signed url map. This will be refreshed when the pre signed urls is going
   *                to expire.
   * @param refs A list of weak references which can be used to determine whether the cache is
   *             still needed. When all the weak references return null, we will remove the pre
   *             signed url cache of this table form the cache.
   * @param profileProvider a profile Provider that can provide customized refresher function.
   * @param refresher A function to re-generate pre signed urls for the table.
   * @param expirationTimestamp Optional, If set, it's a timestamp to indicate the expiration
   *                            timestamp of the idToUrl.
   * @param refreshToken an optional refresh token that can be used by the refresher to retrieve
   *                     the same set of files with refreshed urls.
   */
  def register(
      tablePath: String,
      idToUrl: Map[String, String],
      refs: Seq[WeakReference[AnyRef]],
      profileProvider: DeltaSharingProfileProvider,
      refresher: Option[String] => TableRefreshResult,
      expirationTimestamp: Long = System.currentTimeMillis() + preSignedUrlExpirationMs,
      refreshToken: Option[String]
    ): Unit = {
    val customTablePath = profileProvider.getCustomTablePath(tablePath)
    val customRefresher = profileProvider.getCustomRefresher(refresher)

    val (resolvedIdToUrl, resolvedExpiration, resolvedRefreshToken) =
      if (expirationTimestamp - System.currentTimeMillis() < refreshThresholdMs) {
        val refreshRes = customRefresher(refreshToken)
        if (isValidUrlExpirationTime(refreshRes.expirationTimestamp)) {
          (refreshRes.idToUrl, refreshRes.expirationTimestamp.get, refreshRes.refreshToken)
        } else {
          (
            refreshRes.idToUrl,
            System.currentTimeMillis() + preSignedUrlExpirationMs,
            refreshRes.refreshToken
          )
        }
      } else {
        (idToUrl, expirationTimestamp, refreshToken)
      }

    val cachedTable = new CachedTable(
      resolvedExpiration,
      idToUrl = resolvedIdToUrl,
      refs,
      System.currentTimeMillis(),
      customRefresher,
      resolvedRefreshToken
    )
    var oldTable = cache.putIfAbsent(customTablePath, cachedTable)
    if (oldTable == null) {
      // We insert a new entry to the cache
      return
    }
    // There is an existing entry so we try to merge it with the new one
    while (true) {
      val mergedTable = new CachedTable(
        // Pick up the min value because we will merge urls and we have to refresh when any of urls
        // expire
        cachedTable.expiration min oldTable.expiration,
        // Overwrite urls with the new registered ones because they are usually newer
        oldTable.idToUrl ++ cachedTable.idToUrl,
        // Try to avoid storing duplicate references
        refs.filterNot(ref => oldTable.refs.exists(_.get eq ref.get)) ++ oldTable.refs,
        lastAccess = System.currentTimeMillis(),
        customRefresher,
        cachedTable.refreshToken
      )
      if (cache.replace(customTablePath, oldTable, mergedTable)) {
        // Put the merged one to the cache
        return
      }
      // Failed to put the merged one
      oldTable = cache.get(customTablePath)
      if (oldTable == null) {
        // It was removed between `cache.replace` and `cache.get`
        oldTable = cache.putIfAbsent(customTablePath, cachedTable)
        if (oldTable == null) {
          // We insert a new entry to the cache
          return
        }
        // There was a new inserted one between `cache.get` and `cache.putIfAbsent`. Trying to
        // merge it.
      } else {
        // There was a new inserted one between `cache.replace` and `cache.get`. Trying to
        // merge it.
      }
    }
  }

  def stop(): Unit = {
    refreshThread.shutdownNow()
  }

  /**
   * Clear the cached pre signed urls. This is an internal API to clear the cache in case some users
   * config incorrect pre signed url expiration time and leave expired urls in the cache.
   */
  def clear(): Unit = {
    cache.clear()
  }
}

object CachedTableManager {

  private lazy val preSignedUrlExpirationMs = Option(SparkEnv.get)
    .flatMap(_.conf.getOption("spark.delta.sharing.preSignedUrl.expirationMs"))
    .map(_.toLong)
    .getOrElse(TimeUnit.HOURS.toMillis(1))

  private lazy val refreshCheckIntervalMs = Option(SparkEnv.get)
    .flatMap(_.conf.getOption("spark.delta.sharing.driver.refreshCheckIntervalMs"))
    .map(_.toLong)
    .getOrElse(TimeUnit.MINUTES.toMillis(1))

  private lazy val refreshThresholdMs = Option(SparkEnv.get)
    .flatMap(_.conf.getOption("spark.delta.sharing.driver.refreshThresholdMs"))
    .map(_.toLong)
    .getOrElse(TimeUnit.MINUTES.toMillis(15))

  private lazy val expireAfterAccessMs = Option(SparkEnv.get)
    .flatMap(_.conf.getOption("spark.delta.sharing.driver.accessThresholdToExpireMs"))
    .map(_.toLong)
    .getOrElse(TimeUnit.HOURS.toMillis(1))

  lazy val INSTANCE = new CachedTableManager(
    preSignedUrlExpirationMs = preSignedUrlExpirationMs,
    refreshCheckIntervalMs = refreshCheckIntervalMs,
    refreshThresholdMs = refreshThresholdMs,
    expireAfterAccessMs = expireAfterAccessMs)
}

/** An `RpcEndpoint` running in Spark driver to allow executors to fetch pre signed urls. */
class PreSignedUrlCacheEndpoint(override val rpcEnv: RpcEnv) extends RpcEndpoint {
  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case (tablePath: String, fileId: String) =>
      context.reply(CachedTableManager.INSTANCE.getPreSignedUrl(tablePath, fileId))
  }
}

/**
 * A pre signed url fetcher that monitors the pre signed url expiration time and fetches a new one
 * from the driver when it's expiring (under `refreshThresholdMs`).
 */
class PreSignedUrlFetcher(
    ref: RpcEndpointRef,
    tablePath: String,
    fileId: String,
    refreshThresholdMs: Long) extends Logging {

  private var preSignedUrl: PreSignedUrlCache.Rpc.GetPreSignedUrlResponse = _

  def getUrl(): String = {
    if (preSignedUrl == null ||
        preSignedUrl._2 - System.currentTimeMillis() < refreshThresholdMs) {
      if (preSignedUrl == null) {
        logInfo(s"Fetching pre signed url for $tablePath/$fileId for the first time")
      } else {
        logInfo(s"Fetching pre signed url for $tablePath/$fileId (expiration time: " +
          s"at ${new java.util.Date(preSignedUrl._2)})")
      }
      preSignedUrl =
        ref.askSync[PreSignedUrlCache.Rpc.GetPreSignedUrlResponse](tablePath -> fileId)
    }
    preSignedUrl._1
  }
}

object PreSignedUrlCache extends Logging {

  /**
   * Define the Rpc messages used by driver and executors. Note: as we are a third-party of Spark,
   * Spark's Rpc classloader may not have our classes, so we should not use our own Rpc classes.
   * Instead, we should reuse existing Scala classes, such as tuple.
   */
  object Rpc {
    type GetPreSignedUrl = (String, String)
    type GetPreSignedUrlResponse = (String, Long)
  }


  private val endpointName = "delta.sharing.PreSignedUrlCache"

  /**
   * Register `PreSignedUrlCacheEndpoint` with Spark so that it can be accessed in executors using
   * the endpoint name.
   */
  def registerIfNeeded(env: SparkEnv): Unit = {
    try {
      env.rpcEnv.setupEndpoint(endpointName, new PreSignedUrlCacheEndpoint(env.rpcEnv))
    } catch {
      case _: IllegalArgumentException =>
        // If `endpointName` has been registered, Spark will throw `IllegalArgumentException`. This
        // is safe to ignore
    }
  }

  /** Returns an `RpcEndpointRef` to talk to the driver to fetch pre signed urls. */
  def getEndpointRefInExecutor(env: SparkEnv): RpcEndpointRef = {
    RpcUtils.makeDriverRef(endpointName, env.conf, env.rpcEnv)
  }
}
