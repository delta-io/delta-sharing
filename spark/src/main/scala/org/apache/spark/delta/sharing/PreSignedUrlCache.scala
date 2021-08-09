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

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcCallContext, RpcEndpoint, RpcEndpointRef, RpcEnv}
import org.apache.spark.util.{RpcUtils, ThreadUtils}

import io.delta.sharing.spark.DeltaSharingFileSystem.DeltaSharingPath

class CachedTable(
    val expiration: Long,
    val idToUrl: Map[String, String],
    val references: Seq[WeakReference[AnyRef]],
    @volatile var lastAccess: Long,
    val refresher: () => Map[String, String])

class CachedTableManager(
    preSignedUrlExpirationMs: Long,
    refreshCheckIntervalMs: Long,
    refreshThresholdMs: Long,
    expireAfterAccessMs: Long) {

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

  def refresh(): Unit = {
    import scala.collection.JavaConverters._
    val snapshot = cache.entrySet().asScala.toArray
    for (entry <- snapshot) {
      val tablePath = entry.getKey
      val cachedTable = entry.getValue
      if (cachedTable.references.forall(_.get == null)
          || cachedTable.lastAccess + expireAfterAccessMs < System.currentTimeMillis()) {
        cache.remove(tablePath, cachedTable)
      } else if (cachedTable.expiration - System.currentTimeMillis() < refreshThresholdMs) {
        val newTable = new CachedTable(
          System.currentTimeMillis(),
          cachedTable.refresher(),
          cachedTable.references,
          cachedTable.lastAccess,
          cachedTable.refresher
        )
        cache.replace(tablePath, cachedTable, newTable)
      }
    }
  }

  /** Returns `PreSignedUrl` from the cache. */
  def getPreSignedUrl(path: DeltaSharingPath): PreSignedUrl = {
    val cachedTable = cache.get(path.tablePath)
    if (cachedTable == null) {
      throw new IllegalStateException(
        s"table ${path.tablePath} was removed")
    }
    cachedTable.lastAccess = System.currentTimeMillis()
    val url = cachedTable.idToUrl.getOrElse(path.fileId, {
      throw new IllegalStateException(
        s"cannot find url for id ${path.fileId} in table ${path.tablePath}")
    })
    PreSignedUrl(url, cachedTable.expiration)
  }

  /**
   * Register a table path in the cache. The pre signed urls will be refreshed automatically to
   * support long running queries.
   *
   * @param tablePath the table path. This is usually the profile file path.
   * @param idToUrl the pre signed url map. This will be refreshed when the pre signed urls is going
   *                to expire.
   * @param reference A weak reference which can be used to determine whether the cache is still
   *                  needed. When the weak reference returns null, we will remove the pre signed
   *                  url cache of this table form the cache.
   * @param refresher A function to re-generate pre signed urls for the table.
   */
  def register(
      tablePath: String,
      idToUrl: Map[String, String],
      reference: WeakReference[AnyRef],
      refresher: () => Map[String, String]): Unit = {
    var cachedTable = new CachedTable(
      preSignedUrlExpirationMs + System.currentTimeMillis(),
      idToUrl,
      Seq(reference),
      System.currentTimeMillis(),
      refresher
    )
    var oldTable = cache.putIfAbsent(tablePath, cachedTable)
    if (oldTable == null) {
      return
    }
    while (true) {
      cachedTable = new CachedTable(
        cachedTable.expiration min oldTable.expiration,
        cachedTable.idToUrl ++ oldTable.idToUrl,
        reference +: oldTable.references,
        System.currentTimeMillis(),
        refresher
      )
      if (cache.replace(tablePath, oldTable, cachedTable)) {
        return
      }
      oldTable = cache.get(tablePath)
      if (oldTable == null) {
        oldTable = cache.putIfAbsent(tablePath, cachedTable)
        if (oldTable == null) {
          return
        }
      }
    }
  }

  def stop(): Unit = {
    refreshThread.shutdownNow()
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
    .getOrElse(TimeUnit.MINUTES.toMillis(60))

  lazy val INSTANCE = new CachedTableManager(
    preSignedUrlExpirationMs = preSignedUrlExpirationMs,
    refreshCheckIntervalMs = refreshCheckIntervalMs,
    refreshThresholdMs = refreshThresholdMs,
    expireAfterAccessMs = expireAfterAccessMs)
}

/** The RPC request to fetch `PreSignedUrl` from the driver. */
case class GetPreSignedUrl(path: DeltaSharingPath)

/** The PRC response that is returned to executors. */
case class PreSignedUrl(url: String, expirationMs: Long)

/** An `RpcEndpoint` running in Spark driver to allow executors to fetch pre signed urls. */
class PreSignedUrlCacheEndpoint(override val rpcEnv: RpcEnv) extends RpcEndpoint {
  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case GetPreSignedUrl(path) => context.reply(CachedTableManager.INSTANCE.getPreSignedUrl(path))
  }
}

/**
 * A pre signed url fetcher that monitors the pre signed url expiration time and fetches a new one
 * from the driver when it's expiring (under `refreshThresholdMs`).
 */
class PreSignedUrlFetcher(
    ref: RpcEndpointRef,
    val path: DeltaSharingPath,
    refreshThresholdMs: Long) {

  private var preSignedUrl: PreSignedUrl = _

  def getUrl(): String = {
    if (preSignedUrl == null ||
        preSignedUrl.expirationMs - System.currentTimeMillis() < refreshThresholdMs) {
      preSignedUrl = ref.askSync[PreSignedUrl](GetPreSignedUrl(path))
    }
    preSignedUrl.url
  }
}

object PreSignedUrlCache extends Logging {

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
