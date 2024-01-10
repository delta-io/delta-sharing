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

package io.delta.sharing.client

import java.net.{URI, URLDecoder, URLEncoder}
import java.util.concurrent.TimeUnit

import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable
import org.apache.http.{HttpHost, HttpRequest}
import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.client.config.RequestConfig
import org.apache.http.conn.routing.HttpRoute
import org.apache.http.impl.client.{BasicCredentialsProvider, HttpClientBuilder}
import org.apache.http.impl.conn.{DefaultRoutePlanner, DefaultSchemePortResolver}
import org.apache.http.protocol.HttpContext
import org.apache.spark.SparkEnv
import org.apache.spark.delta.sharing.{PreSignedUrlCache, PreSignedUrlFetcher}

import io.delta.sharing.client.model.FileAction
import io.delta.sharing.client.util.ConfUtils

/** Read-only file system for delta paths. */
private[sharing] class DeltaSharingFileSystem extends FileSystem {

  import DeltaSharingFileSystem._

  lazy private val numRetries = ConfUtils.numRetries(getConf)
  lazy private val maxRetryDurationMillis = ConfUtils.maxRetryDurationMillis(getConf)
  lazy private val timeoutInSeconds = ConfUtils.timeoutInSeconds(getConf)
  lazy private val httpClient = createHttpClient()

  private[sharing] def createHttpClient() = {
    val proxyConfigOpt = ConfUtils.getProxyConfig(getConf)
    val maxConnections = ConfUtils.maxConnections(getConf)
    val config = RequestConfig.custom()
      .setConnectTimeout(timeoutInSeconds * 1000)
      .setConnectionRequestTimeout(timeoutInSeconds * 1000)
      .setSocketTimeout(timeoutInSeconds * 1000).build()

    val clientBuilder = HttpClientBuilder.create()
      .setMaxConnTotal(maxConnections)
      .setMaxConnPerRoute(maxConnections)
      .setDefaultRequestConfig(config)
      // Disable the default retry behavior because we have our own retry logic.
      // See `RetryUtils.runWithExponentialBackoff`.
      .disableAutomaticRetries()

    // Set proxy if provided.
    proxyConfigOpt.foreach { proxyConfig =>

      val proxy = new HttpHost(proxyConfig.host, proxyConfig.port)
      clientBuilder.setProxy(proxy)

      if (proxyConfig.noProxyHosts.nonEmpty) {
        val routePlanner = new DefaultRoutePlanner(DefaultSchemePortResolver.INSTANCE) {
          override def determineRoute(target: HttpHost,
                                      request: HttpRequest,
                                      context: HttpContext): HttpRoute = {
            if (proxyConfig.noProxyHosts.contains(target.getHostName)) {
              // Direct route (no proxy)
              new HttpRoute(target)
            } else {
              // Route via proxy
              new HttpRoute(target, proxy)
            }
          }
        }
        clientBuilder.setRoutePlanner(routePlanner)
      }
    }
    clientBuilder.build()
  }

  private lazy val refreshThresholdMs = getConf.getLong(
    "spark.delta.sharing.executor.refreshThresholdMs",
    TimeUnit.MINUTES.toMillis(10))

  private lazy val preSignedUrlCacheRef = PreSignedUrlCache.getEndpointRefInExecutor(SparkEnv.get)

  override def getScheme: String = SCHEME

  override def getUri(): URI = URI.create(s"$SCHEME:///")

  // open a file path with the format below:
  // ```
  // delta-sharing:///<url encoded table path>/<url encoded file id>/<size>
  // ```
  override def open(f: Path, bufferSize: Int): FSDataInputStream = {
    val path = DeltaSharingFileSystem.decode(f)
    val fetcher =
      new PreSignedUrlFetcher(preSignedUrlCacheRef, path.tablePath, path.fileId, refreshThresholdMs)
    if (getConf.getBoolean("spark.delta.sharing.loadDataFilesInMemory", false)) {
      // `InMemoryHttpInputStream` loads the content into the memory immediately, so we don't need
      // to refresh urls.
      new FSDataInputStream(new InMemoryHttpInputStream(new URI(fetcher.getUrl())))
    } else {
      new FSDataInputStream(
        new RandomAccessHttpInputStream(
          httpClient,
          fetcher,
          path.fileSize,
          statistics,
          numRetries,
          maxRetryDurationMillis
        )
      )
    }
  }

  override def create(
      f: Path,
      permission: FsPermission,
      overwrite: Boolean,
      bufferSize: Int,
      replication: Short,
      blockSize: Long,
      progress: Progressable): FSDataOutputStream =
    throw new UnsupportedOperationException("create")

  override def append(f: Path, bufferSize: Int, progress: Progressable): FSDataOutputStream =
    throw new UnsupportedOperationException("append")

  override def rename(src: Path, dst: Path): Boolean =
    throw new UnsupportedOperationException("rename")

  override def delete(f: Path, recursive: Boolean): Boolean =
    throw new UnsupportedOperationException("delete")

  override def listStatus(f: Path): Array[FileStatus] =
    throw new UnsupportedOperationException("listStatus")

  override def setWorkingDirectory(new_dir: Path): Unit =
    throw new UnsupportedOperationException("setWorkingDirectory")

  override def getWorkingDirectory: Path = new Path(getUri)

  override def mkdirs(f: Path, permission: FsPermission): Boolean =
    throw new UnsupportedOperationException("mkdirs")

  override def getFileStatus(f: Path): FileStatus = {
    val resolved = makeQualified(f)
    new FileStatus(decode(resolved).fileSize, false, 0, 1, 0, f)
  }

  override def finalize(): Unit = {
    try super.finalize() finally close()
  }

  override def close(): Unit = {
    try super.close() finally httpClient.close()
  }
}

private[sharing] object DeltaSharingFileSystem {

  val SCHEME = "delta-sharing"

  case class DeltaSharingPath(tablePath: String, fileId: String, fileSize: Long) {

    /**
     * Convert `DeltaSharingPath` to a `Path` in the following format:
     *
     * ```
     * delta-sharing:///<url encoded table path>/<url encoded file id>/<size>
     * ```
     *
     * This format can be decoded by `DeltaSharingFileSystem.decode`.
     */
    def toPath: Path = {
      val encodedTablePath = URLEncoder.encode(tablePath, "UTF-8")
      val encodedFileId = URLEncoder.encode(fileId, "UTF-8")
      new Path(s"$SCHEME:///$encodedTablePath/$encodedFileId/$fileSize")
    }
  }

  def encode(tablePath: String, action: FileAction): Path = {
    DeltaSharingPath(tablePath, action.id, action.size).toPath
  }

  def decode(path: Path): DeltaSharingPath = {
    val encodedPath = path.toString
      .stripPrefix(s"$SCHEME:///")
      .stripPrefix(s"$SCHEME:/")
    val Array(encodedTablePath, encodedFileId, sizeString) = encodedPath.split("/")
    DeltaSharingPath(
      URLDecoder.decode(encodedTablePath, "UTF-8"),
      URLDecoder.decode(encodedFileId, "UTF-8"),
      sizeString.toLong)
  }
}
