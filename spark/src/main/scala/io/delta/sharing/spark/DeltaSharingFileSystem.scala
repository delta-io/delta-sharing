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

package io.delta.sharing.spark

import java.net.{URI, URLDecoder, URLEncoder}
import java.util.concurrent.TimeUnit

import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable
import org.apache.http.client.config.RequestConfig
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.spark.SparkEnv
import org.apache.spark.delta.sharing.{PreSignedUrlCache, PreSignedUrlFetcher}
import org.apache.spark.network.util.JavaUtils

import io.delta.sharing.spark.model.AddFile

/** Read-only file system for delta paths. */
private[sharing] class DeltaSharingFileSystem extends FileSystem {
  import DeltaSharingFileSystem._

  lazy private val numRetries = {
    val numRetries = getConf.getInt("spark.delta.sharing.network.numRetries", 10)
    if (numRetries < 0) {
      throw new IllegalArgumentException(
        "spark.delta.sharing.network.numRetries must not be negative")
    }
    numRetries
  }

  lazy private val timeoutInSeconds = {
    val timeoutStr = getConf.get("spark.delta.sharing.network.timeout", "120s")
    val timeoutInSeconds = JavaUtils.timeStringAs(timeoutStr, TimeUnit.SECONDS)
    if (timeoutInSeconds < 0) {
      throw new IllegalArgumentException(
        "spark.delta.sharing.network.timeout must not be negative")
    }
    if (timeoutInSeconds > Int.MaxValue) {
      throw new IllegalArgumentException(
        s"spark.delta.sharing.network.timeout is too big: $timeoutStr")
    }
    timeoutInSeconds.toInt
  }

  lazy private val httpClient = {
    val maxConnections = getConf.getInt("spark.delta.sharing.network.maxConnections", 64)
    if (maxConnections < 0) {
      throw new IllegalArgumentException(
        "spark.delta.sharing.network.maxConnections must not be negative")
    }
    val config = RequestConfig.custom()
      .setConnectTimeout(timeoutInSeconds * 1000)
      .setConnectionRequestTimeout(timeoutInSeconds * 1000)
      .setSocketTimeout(timeoutInSeconds * 1000).build()
    HttpClientBuilder.create()
      .setMaxConnTotal(maxConnections)
      .setMaxConnPerRoute(maxConnections)
      .setDefaultRequestConfig(config)
      // Disable the default retry behavior because we have our own retry logic.
      // See `RetryUtils.runWithExponentialBackoff`.
      .disableAutomaticRetries()
      .build()
  }

  private lazy val refreshThresholdMs = getConf.getLong(
    "spark.delta.sharing.executor.refreshThresholdMs",
    TimeUnit.MINUTES.toMillis(10))

  private lazy val preSignedUrlCacheRef = PreSignedUrlCache.getEndpointRefInExecutor(SparkEnv.get)

  override def getScheme: String = SCHEME

  override def getUri(): URI = URI.create(s"$SCHEME:///")

  override def open(f: Path, bufferSize: Int): FSDataInputStream = {
    val path = DeltaSharingFileSystem.decode(f)
    val fetcher = new PreSignedUrlFetcher(preSignedUrlCacheRef, path, refreshThresholdMs)
    if (getConf.getBoolean("spark.delta.sharing.loadDataFilesInMemory", false)) {
      // `InMemoryHttpInputStream` loads the content into the memory immediately, so we don't need
      // to refresh urls.
      new FSDataInputStream(new InMemoryHttpInputStream(new URI(fetcher.getUrl())))
    } else {
      new FSDataInputStream(
        new RandomAccessHttpInputStream(httpClient, fetcher, statistics, numRetries))
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

object DeltaSharingFileSystem {

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

  def encode(tablePath: Path, addFile: AddFile): Path = {
    DeltaSharingPath(tablePath.toString, addFile.id, addFile.size).toPath
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
