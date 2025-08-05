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
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.{RpcUtils, ThreadUtils}

import io.delta.sharing.client.DeltaSharingProfileProvider
import io.delta.sharing.client.util.ConfUtils

case class TableRefreshResult(
    idToUrl: Map[String, String],
    expirationTimestamp: Option[Long],
    refreshToken: Option[String]
)

/**
 * Base class representing a cached table with common properties.
 *
 * @param expiration the expiration time of the pre signed urls
 * @param idToUrl the file id to pre sign url map
 * @param lastAccess When the table was accessed last time. We will remove old tables that are not
 *                   accessed after `expireAfterAccessMs` milliseconds.
 * @param refreshToken the optional refresh token that can be used by the refresher to retrieve
 *                     the same set of files with refreshed urls.
 * @param refresher the function to generate a new file id to pre sign url map, with the new
 *                  expiration timestamp of the urls and the new refresh token.
 */
abstract class BaseCachedTable(
    val expiration: Long,
    val idToUrl: Map[String, String],
    @volatile var lastAccess: Long,
    val refreshToken: Option[String],
    val refresher: Option[String] => TableRefreshResult
)

/**
 * Represents a cached table entry with a single refresher function.
 * All the queries sent to the same table can share the same refresher to update file URLs.
 *
 * Example:
 * If two queries share the same cached table entry, they will both rely on the same `refresher`
 * function to update the file URLs. When the `refresher` is invoked, it will generate a new
 * mapping of file IDs to pre-signed URLs, along with a new expiration timestamp and refresh token.
 * This ensures that both queries benefit from the updated URLs without duplicating the refresh
 * logic or state.
 *
 * @param refs the references that we track. When all of references in the table are gone, we will
 *             remove the cached table from our cache.
 */
class CachedTable(
    expiration: Long,
    idToUrl: Map[String, String],
    lastAccess: Long,
    refreshToken: Option[String],
    refresher: Option[String] => TableRefreshResult,
    val refs: Seq[WeakReference[AnyRef]]
) extends BaseCachedTable(expiration, idToUrl, lastAccess, refreshToken, refresher)

/**
 * Represents a cached table entry with a unique refresher for each query. This design ensures that
 * even if two queries are identical in structure, their refreshers can maintain independent states.
 * The refreshFunction is responsible for refreshing the pre-signed URLs for the table, while the
 * RefresherWrapper allows additional customization or state management during the refresh process.
 * This is necessary because the server may require specific states in the refresher function to
 * refresh the pre-signed URLs. The refresh results can be shared across the same query for the same
 * table.
 *
 * Example:
 * Query 1: SELECT * FROM table WHERE col1 = 'value1'
 * Query 2: SELECT * FROM table WHERE col1 = 'value1'
 * Both queries can share the same cache entry for the table, but their refreshers maintain
 * independent states to handle server-specific requirements for refreshing pre-signed URLs.
 * When Query 2 ends, and Query 1 keeps running, we need to maintain Query 1's refresher state
 * to ensure it continues to refresh the pre-signed URLs as needed.
 *
 * @param queryStates A mapping of query identifiers to their associated weak references
 *                        and refresher wrappers.
 * @param keepUrlsAfterRefsGone If true, URLs will be kept in cache even when all references
 *                              are gone, and will only be cleaned up based on cache expiry time.
 */
class QuerySpecificCachedTable(
    expiration: Long,
    idToUrl: Map[String, String],
    lastAccess: Long,
    refreshToken: Option[String],
    refresher: Option[String] => TableRefreshResult,
    val queryStates: Map[
      String,
      (Seq[WeakReference[AnyRef]], QuerySpecificCachedTable.RefresherWrapper)
    ],
    val keepUrlsAfterRefsGone: Boolean = false
) extends BaseCachedTable(expiration, idToUrl, lastAccess, refreshToken, refresher)

object QuerySpecificCachedTable {
  // A type alias for a function that wraps a refresher function.
  // The wrapper takes an optional refresh token and a refresher function,
  // and returns a `TableRefreshResult`.
  type RefresherWrapper =
    (Option[String], Option[String] => TableRefreshResult) => TableRefreshResult
}

class CachedTableManager(
    val preSignedUrlExpirationMs: Long,
    refreshCheckIntervalMs: Long,
    val refreshThresholdMs: Long,
    expireAfterAccessMs: Long) extends Logging {

  private val cache = new java.util.concurrent.ConcurrentHashMap[String, BaseCachedTable]()

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

  // Returning how many entries are in the cache.
  // This method is mainly for testing purpose.
  def size(): Int = {
    cache.size
  }

  // Returns the number of query states in the cache for a given table path.
  // This method is mainly for testing purpose.
  def getQueryStateSize(tablePath: String): Int = {
    val cachedTable = cache.get(tablePath)
    if (cachedTable == null) {
      throw new IllegalStateException(s"table $tablePath was removed")
    }
    cachedTable match {
      case table: QuerySpecificCachedTable => table.queryStates.size
      case _ => 0
    }
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

  /**
   * Refreshes the pre-signed URLs in the cache. This method iterates through the cache entries,
   * checks their validity, and updates or removes them as necessary.
   */
  def refresh(): Unit = {
    import scala.collection.JavaConverters._

    // Take a snapshot of the cache entries to avoid concurrent modification issues
    val snapshot = cache.entrySet().asScala.toArray

    snapshot.foreach { entry =>
      val tablePath = entry.getKey
      val cachedTable = entry.getValue

      // Checks if a cached table has expired based on last access time.
      if (cachedTable.lastAccess + expireAfterAccessMs < System.currentTimeMillis()) {
        logInfo(s"Removing table $tablePath from the pre signed url cache as it was not accessed " +
          s"after $expireAfterAccessMs ms")
        cache.remove(tablePath, cachedTable)
      } else {
        cachedTable match {
          case table: CachedTable =>
            handleCachedTableRefresh(tablePath, table)
          case table: QuerySpecificCachedTable =>
            handleQuerySpecificCachedTableRefresh(tablePath)
          case _ =>
            // We should never have a table that is not CachedTable or QuerySpecificCachedTable
            // Also, refresh happens in a background thread, throw an exception doesn't help.
            logWarning(s"Unknown table type for $tablePath, type ${cachedTable.getClass}.")
        }
      }
    }
  }

  private def logDifferencesBetweenUrls(
      refreshedUrls: Map[String, String],
      cachedUrls: Map[String, String]
  ): Unit = {
    // If the refresher returns a different list of fileId, we will log a warning.
    if (refreshedUrls.size != cachedUrls.size) {
      logWarning(s"freshen urls size ${refreshedUrls.size} is not equal to " +
        s"cached urls size ${cachedUrls.size}")
    }
    val onlyInRefresh = refreshedUrls.keySet.diff(cachedUrls.keySet)
    val onlyInCached = cachedUrls.keySet.diff(refreshedUrls.keySet)
    if (onlyInRefresh.nonEmpty || onlyInCached.nonEmpty) {
      if (onlyInRefresh.nonEmpty) {
        logWarning(s"Keys only in refreshRes.idToUrl: ${onlyInRefresh.mkString(", ")}")
      }
      if (onlyInCached.nonEmpty) {
        logWarning(s"Keys only in cachedTable.idToUrl: ${onlyInCached.mkString(", ")}")
      }
    }
  }

  /** Refreshes a `CachedTable` if necessary. */
  private def handleCachedTableRefresh(tablePath: String, cachedTable: CachedTable): Unit = {
    if (cachedTable.refs.forall(_.get == null)) {
      // If all the references are gone, we will remove the table from the cache.
      logInfo(s"Removing table $tablePath from the pre signed url cache as there are" +
        " no references pointed to it")
      cache.remove(tablePath, cachedTable)
    } else if (cachedTable.expiration - System.currentTimeMillis() < refreshThresholdMs) {
      // If the pre signed urls are going to expire, we will refresh them.
      logInfo(s"Updating pre signed urls for $tablePath (expiration time: " +
        s"${new java.util.Date(cachedTable.expiration)}), token:${cachedTable.refreshToken}")
      try {
        val refreshRes = cachedTable.refresher(cachedTable.refreshToken)
        logDifferencesBetweenUrls(refreshRes.idToUrl, cachedTable.idToUrl)

        val newTable = new CachedTable(
          expiration =
            if (isValidUrlExpirationTime(refreshRes.expirationTimestamp)) {
              refreshRes.expirationTimestamp.get
            } else {
              preSignedUrlExpirationMs + System.currentTimeMillis()
            },
          idToUrl = refreshRes.idToUrl,
          refs = cachedTable.refs,
          lastAccess = cachedTable.lastAccess,
          refresher = cachedTable.refresher,
          refreshToken = refreshRes.refreshToken
        )
        // Failing to replace the table is fine because if it did happen, we would retry after
        // `refreshCheckIntervalMs` milliseconds.
        cache.replace(tablePath, cachedTable, newTable)
        logInfo(s"Updated pre signed urls for $tablePath with size ${refreshRes.idToUrl.size}")
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

  /** Refreshes a `QuerySpecificCachedTable` if necessary. */
  private def handleQuerySpecificCachedTableRefresh(tablePath: String): Unit = {
    // Use `computeIfPresent` to ensure atomicity of the operation and prevent interference
    // with concurrent register threads.
    cache.computeIfPresent(tablePath, (_, cachedTable) => cachedTable match {
      case querySpecificCachedTable: QuerySpecificCachedTable =>
        // Filter out query states where at least one reference is still valid (not garbage
        // collected).
        val validStates = querySpecificCachedTable.queryStates.filter {
          case (_, (refs, _)) => refs.exists(_.get != null)
        }

        if (validStates.isEmpty) {
          // If all the references are gone, check if we should keep the URLs
          if (!querySpecificCachedTable.keepUrlsAfterRefsGone) {
            // If we're not keeping URLs after refs are gone, remove the table
            logInfo(s"Removing table $tablePath as no valid query mappings remain.")
            null
          } else if (
            querySpecificCachedTable.expiration - System.currentTimeMillis() < refreshThresholdMs) {
            // If we're keeping URLs, but they're about to expire, and we can't refresh them,
            // remove the table since we can't refresh without valid states
            logInfo(s"Removing table $tablePath as URLs are about to expire and no valid query " +
              "mappings remain to refresh them.")
            null
          } else {
            // Keep the URLs since they're still valid for a while
            logInfo(s"Keeping URLs for $tablePath after all references are gone, URLs are still " +
              "valid.")
            new QuerySpecificCachedTable(
              querySpecificCachedTable.expiration,
              querySpecificCachedTable.idToUrl,
              querySpecificCachedTable.lastAccess,
              querySpecificCachedTable.refreshToken,
              querySpecificCachedTable.refresher,
              validStates,
              querySpecificCachedTable.keepUrlsAfterRefsGone
            )
          }
        } else if (
          querySpecificCachedTable.expiration - System.currentTimeMillis() < refreshThresholdMs
        ) {
          // If the pre signed urls are going to expire, we will refresh them.
          // At this point, we know we have valid states since we handled empty states above.
          try {
            val (_, (_, refresherWrapper)) = validStates.head
            // Run the refresh function with the wrapper to get the new pre-signed URLs
            val refreshRes = refresherWrapper(
              querySpecificCachedTable.refreshToken,
              querySpecificCachedTable.refresher
            )
            logDifferencesBetweenUrls(refreshRes.idToUrl, querySpecificCachedTable.idToUrl)
            logInfo(
              s"Updated pre-signed URLs for $tablePath with size ${refreshRes.idToUrl.size}, " +
                s"validStates: ${validStates.size}."
            )
            new QuerySpecificCachedTable(
              expiration =
                if (isValidUrlExpirationTime(refreshRes.expirationTimestamp)) {
                  refreshRes.expirationTimestamp.get
                } else {
                  preSignedUrlExpirationMs + System.currentTimeMillis()
                },
              idToUrl = refreshRes.idToUrl,
              lastAccess = querySpecificCachedTable.lastAccess,
              refreshToken = refreshRes.refreshToken,
              refresher = querySpecificCachedTable.refresher,
              queryStates = validStates,
              keepUrlsAfterRefsGone = querySpecificCachedTable.keepUrlsAfterRefsGone
            )
          } catch {
            case NonFatal(e) =>
              logError(s"Failed to refresh pre-signed URLs for table $tablePath", e)
              if (querySpecificCachedTable.expiration < System.currentTimeMillis()) {
                logInfo(s"Removing table $tablePath as the pre-signed URL has expired.")
                null
              } else {
                // Update query states
                new QuerySpecificCachedTable(
                  querySpecificCachedTable.expiration,
                  querySpecificCachedTable.idToUrl,
                  querySpecificCachedTable.lastAccess,
                  querySpecificCachedTable.refreshToken,
                  querySpecificCachedTable.refresher,
                  validStates,
                  querySpecificCachedTable.keepUrlsAfterRefsGone
                )
              }
          }
        } else {
          // Update query states
          new QuerySpecificCachedTable(
            querySpecificCachedTable.expiration,
            querySpecificCachedTable.idToUrl,
            querySpecificCachedTable.lastAccess,
            querySpecificCachedTable.refreshToken,
            querySpecificCachedTable.refresher,
            validStates,
            querySpecificCachedTable.keepUrlsAfterRefsGone
          )
        }

      case _ =>
        logWarning(s"Unexpected table type for $tablePath. Expected QuerySpecificCachedTable.")
        cachedTable
    })
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
      logInfo(s"${cachedTable.idToUrl.size} urls in cache " +
        s"with expiration ${new java.util.Date(cachedTable.expiration)}")
      throw new IllegalStateException(s"cannot find url for id $fileId in table $tablePath")
    })
    (url, cachedTable.expiration)
  }

  /**
   * Adds new query states to an existing QuerySpecificCachedTable entry in the cache.
   *
   * This method is used when a new query (with its own queryId and refresher wrapper)
   * wants to share the same cached table entry as other queries, but only needs to add
   * its own state (references and refresher wrapper) without modifying the rest of the
   * table's cache entry. If the table is not a QuerySpecificCachedTable, or does not exist,
   * this method is a no-op.
   *
   * The method is atomic and thread-safe, using computeIfPresent to avoid race conditions
   * with concurrent register or refresh operations.
   *
   * @param tablePath The path of the table whose query states should be updated.
   * @param refs A sequence of weak references for the new query, used to track its usage.
   * @param profileProvider The profile provider, which supplies the queryId and refresher wrapper
   *                       for the new query state.
   */
  def registerQueryStatesInQuerySpecificCachedTable(
    tablePath: String,
    refs: Seq[WeakReference[AnyRef]],
    profileProvider: DeltaSharingProfileProvider
  ): Unit = {
    val queryId = profileProvider.getCustomQueryId()
      .getOrElse(throw new IllegalStateException("Query ID is not defined."))
    val refresherWrapper = profileProvider.getCustomRefresherWrapper().getOrElse(
      throw new IllegalStateException("refresherWrapper is not defined.")
    )
    val customTablePath = profileProvider.getCustomTablePath(tablePath)

    cache.computeIfPresent(customTablePath, (_, existingTable) => existingTable match {
      case q: QuerySpecificCachedTable =>
        val newQueryStates = q.queryStates + (queryId -> (refs, refresherWrapper))
        new QuerySpecificCachedTable(
          q.expiration,
          q.idToUrl,
          q.lastAccess,
          q.refreshToken,
          q.refresher,
          newQueryStates,
          keepUrlsAfterRefsGone = true
        )
      case _ => existingTable
    })
  }

  /**
   * Registers a query-specific cached table in the cache. This method ensures that each query
   * maintains its own refresher state, even if multiple queries share the same table.
   * If the table is not already in the cache, a new entry is created. If the table is
   * already in the cache, the existing entry is updated with the new query-specific refresher.
   *
   * This design allows independent state management for each query while enabling shared
   * caching of file URLs across identical queries.
   *
   * @param tablePath The path of the table to be cached.
   * @param idToUrl A mapping of file IDs to their corresponding pre-signed URLs.
   * @param refs A sequence of weak references used to track the table's usage.
   * @param refresher A function to refresh the pre-signed URLs for the table.
   * @param expirationTimestamp The expiration timestamp of the pre-signed URLs.
   * @param refreshToken An optional token used to refresh the pre-signed URLs.
   * @param profileProvider A provider for query-specific profile and refresher details.
   * @param keepUrlsAfterRefsGone If true, URLs will be kept in cache even when all references
   *                              are gone.
   */
  private def registerQuerySpecificCachedTable(
    tablePath: String,
    idToUrl: Map[String, String],
    refs: Seq[WeakReference[AnyRef]],
    refresher: Option[String] => TableRefreshResult,
    expirationTimestamp: Long,
    refreshToken: Option[String],
    profileProvider: DeltaSharingProfileProvider,
    keepUrlsAfterRefsGone: Boolean = false
  ): Unit = {
    val queryId = profileProvider.getCustomQueryId()
      .getOrElse(throw new IllegalStateException("Query ID is not defined."))
    val refresherWrapper = profileProvider.getCustomRefresherWrapper().getOrElse(
      throw new IllegalStateException("refresherWrapper is not defined.")
    )
    if (refs.size != 1) {
      logWarning(s"Multiple references sharing the same QuerySpecificCachedTable: ${refs.size}")
    }

    // If the pre signed urls are going to expire, we will refresh them.
    val (resolvedIdToUrl, resolvedExpiration, resolvedRefreshToken) =
      if (expirationTimestamp - System.currentTimeMillis() < refreshThresholdMs) {
        val refreshRes = refresherWrapper(refreshToken, refresher)
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

    // Use compute to gaurantee atomicity of the operation
    cache.compute(tablePath, (_, existingTable) => {
      if (existingTable == null) {
        // If the table is not in cache, we will create a new entry
        logInfo(
          s"Registered a new QuerySpecificCachedTable in cache for table $tablePath, " +
          s"queryId ${queryId}."
        )
        new QuerySpecificCachedTable(
          expiration = resolvedExpiration,
          idToUrl = resolvedIdToUrl,
          lastAccess = System.currentTimeMillis(),
          refreshToken = resolvedRefreshToken,
          refresher = refresher,
          queryStates = Map(queryId -> (refs, refresherWrapper)),
          keepUrlsAfterRefsGone = keepUrlsAfterRefsGone
        )
      } else {
        // If the table is already in cache, we will update the existing entry
        val querySpecificCachedTable = existingTable match {
          case cached: QuerySpecificCachedTable => cached
          case _ =>
            // This is a safeguard that should never occur.
            throw new IllegalStateException(
              s"Cache entry type mismatch: existing type is ${existingTable.getClass}, " +
              s"expected type is QuerySpecificCachedTable."
            )
        }

        // Merge idToUrl maps - new URLs take precedence for same file IDs.
        // This merging is needed by streaming queries due to complex interactions
        // in DataSharingSource.
        val mergedIdToUrl = querySpecificCachedTable.idToUrl ++ resolvedIdToUrl

        // Log information about the merge
        val existingIds = querySpecificCachedTable.idToUrl.keySet
        val newIds = resolvedIdToUrl.keySet
        val addedIds = newIds -- existingIds
        val updatedIds = newIds.intersect(existingIds)
        val preservedIds = existingIds -- newIds

        if (addedIds.nonEmpty || updatedIds.nonEmpty || preservedIds.nonEmpty) {
          logInfo(
            s"Merging URLs for table $tablePath. " +
            s"Total URLs: ${mergedIdToUrl.size} (existing: ${existingIds.size}, " +
            s"new: ${newIds.size}, added: ${addedIds.size}, updated: ${updatedIds.size}, " +
            s"preserved: ${preservedIds.size}). Query ID: $queryId."
          )
        }

        // Merge query states - if queryId already exists, merge the refs
        val existingQueryState = querySpecificCachedTable.queryStates.get(queryId)
        val mergedQueryStates = existingQueryState match {
          case Some((existingRefs, existingWrapper)) =>
            // Merge refs, avoiding duplicates
            val mergedRefs = refs.filterNot { ref =>
              existingRefs.exists(_.get eq ref.get)
            } ++ existingRefs
            querySpecificCachedTable.queryStates + (queryId -> (mergedRefs, refresherWrapper))
          case None =>
            // New query state
            querySpecificCachedTable.queryStates + (queryId -> (refs, refresherWrapper))
        }

        logInfo(
          s"Registered to an existing QuerySpecificCachedTable in cache for table $tablePath, " +
          s"queryId ${queryId}, total queryStates: ${mergedQueryStates.size}, " +
          s"refs size after merge: ${mergedQueryStates(queryId)._1.size}."
        )

        new QuerySpecificCachedTable(
          expiration = if (preservedIds.nonEmpty) {
            // Old entry has URLs not in new entry, use min expiration
            resolvedExpiration min querySpecificCachedTable.expiration
          } else {
            // All old URLs are being replaced, use new expiration
            resolvedExpiration
          },
          idToUrl = mergedIdToUrl,
          lastAccess = System.currentTimeMillis(),
          refreshToken = resolvedRefreshToken,
          refresher = refresher,
          queryStates = mergedQueryStates,
          keepUrlsAfterRefsGone = keepUrlsAfterRefsGone
        )
      }
    })
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
   * @param keepUrlsAfterRefsGone If true, for QuerySpecificCachedTable, URLs will be kept in cache
   *                              even when all references are gone, and will only be cleaned up
   *                              based on cache expiry time.
   */
  def register(
      tablePath: String,
      idToUrl: Map[String, String],
      refs: Seq[WeakReference[AnyRef]],
      profileProvider: DeltaSharingProfileProvider,
      refresher: Option[String] => TableRefreshResult,
      expirationTimestamp: Long = System.currentTimeMillis() + preSignedUrlExpirationMs,
      refreshToken: Option[String],
      keepUrlsAfterRefsGone: Boolean = false
    ): Unit = {
    val customTablePath = profileProvider.getCustomTablePath(tablePath)

    val parquetIOCacheEnabled = try {
      ConfUtils.sparkParquetIOCacheEnabled(SparkSession.active.sessionState.conf)
    } catch {
      case _: Exception =>
        // This is a safeguard in case SparkSession is not available
        logWarning("Failed to get sparkParquetIOCacheEnabled, using default value.")
        false
    }

    if (parquetIOCacheEnabled && profileProvider.getCustomQueryId().isDefined) {
      return registerQuerySpecificCachedTable(
        tablePath = customTablePath,
        idToUrl = idToUrl,
        refs = refs,
        refresher = refresher,
        expirationTimestamp = expirationTimestamp,
        refreshToken = refreshToken,
        profileProvider,
        keepUrlsAfterRefsGone = keepUrlsAfterRefsGone)
    }

    val customRefresher = profileProvider.getCustomRefresher(refresher)
    val (resolvedIdToUrl, resolvedExpiration, resolvedRefreshToken) =
      if (expirationTimestamp - System.currentTimeMillis() < refreshThresholdMs) {
        val refreshRes = customRefresher(refreshToken)
        logInfo(s"Refreshed urls during cache register with old expiration " +
          s"${new java.util.Date(expirationTimestamp)}, new expiration " +
          s"${refreshRes.expirationTimestamp.map(new java.util.Date(_)).getOrElse("None")}, " +
          s"lines ${refreshRes.idToUrl.size}")

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
      expiration = resolvedExpiration,
      idToUrl = resolvedIdToUrl,
      refs = refs,
      lastAccess = System.currentTimeMillis(),
      refresher = customRefresher,
      refreshToken = resolvedRefreshToken
    )
    var oldTable = cache.putIfAbsent(customTablePath, cachedTable)
    if (oldTable == null) {
      logInfo(s"Registered a new entry in cache for table $customTablePath.")
      return
    }
    val oldCachedTable = oldTable match {
      case cached: CachedTable => cached
      case _ =>
        // This is a safeguard that should never occur.
        throw new IllegalStateException(
          s"Cache entry type mismatch: existing type is ${oldTable.getClass}, " +
          s"expected type is CachedTable."
        )
    }

    // There is an existing entry so we try to merge it with the new one
    while (true) {
      val mergedTable = new CachedTable(
        // Pick up the min value because we will merge urls and we have to refresh when any of urls
        // expire
        expiration = cachedTable.expiration min oldTable.expiration,
        // Overwrite urls with the new registered ones because they are usually newer
        idToUrl = oldTable.idToUrl ++ cachedTable.idToUrl,
        // Try to avoid storing duplicate references
        refs = refs.filterNot(ref =>
          oldCachedTable.refs.exists(_.get eq ref.get)
        ) ++ oldCachedTable.refs,
        lastAccess = System.currentTimeMillis(),
        refresher = customRefresher,
        refreshToken = cachedTable.refreshToken
      )
      if (cache.replace(customTablePath, oldTable, mergedTable)) {
        // Put the merged one to the cache
        logInfo(s"Registered to an existing entry in cache for table $customTablePath.")
        return
      }
      // Failed to put the merged one
      oldTable = cache.get(customTablePath)
      if (oldTable == null) {
        // It was removed between `cache.replace` and `cache.get`
        oldTable = cache.putIfAbsent(customTablePath, cachedTable)
        if (oldTable == null) {
          // We insert a new entry to the cache
          logInfo(s"Registered a new entry in cache for table $customTablePath on 2nd try.")
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
