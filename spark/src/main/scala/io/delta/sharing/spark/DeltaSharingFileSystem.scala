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

import java.net.URI
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Base64

import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable
import org.apache.http.impl.client.HttpClientBuilder

/** Read-only file system for delta paths. */
private[sharing] class DeltaSharingFileSystem extends FileSystem {
  import DeltaSharingFileSystem._

  private lazy val httpClient = {
    val maxConnections = getConf.getInt("spark.delta.sharing.maxConnections", 64)
    HttpClientBuilder.create()
      .setMaxConnTotal(maxConnections)
      .setMaxConnPerRoute(maxConnections)
      // Disable the default retry behavior because we have our own retry logic.
      // See `RetryUtils.runWithExponentialBackoff`.
      .disableAutomaticRetries()
      .build()
  }

  override def getScheme: String = SCHEME

  override def getUri(): URI = URI.create(s"$SCHEME:///")

  override def open(f: Path, bufferSize: Int): FSDataInputStream = {
    val resolved = makeQualified(f)
    val (uri, len) = restoreUri(resolved)
    if (getConf.getBoolean("spark.delta.sharing.loadDataFilesInMemory", false)) {
      new FSDataInputStream(new InMemoryHttpInputStream(uri))
    } else {
      new FSDataInputStream(new RandomAccessHttpInputStream(httpClient, uri, len, statistics))
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
    val (_, len) = restoreUri(resolved)
    new FileStatus(len, false, 0, 1, 0, f)
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

  /**
   * Create a delta-sharing path for the uri and the file size. The file size will be encoded
   * in the path in order to implement `DeltaSharingFileSystem.getFileStatus`.
   */
  def createPath(uri: URI, size: Long): Path = {
    val uriWithSize = s"$uri#size=$size"
    val encoded = new String(Base64.getUrlEncoder.encode(uriWithSize.getBytes(UTF_8)), UTF_8)
    new Path(new URI(s"$SCHEME:///$encoded"))
  }

  /** Restore the original URI and the file size from the delta-sharing path. */
  def restoreUri(path: Path): (URI, Long) = {
    val encoded = path.toUri.toString
      .stripPrefix(s"$SCHEME:///")
      .stripPrefix(s"$SCHEME:/")
      .getBytes(UTF_8)
    val uriWithSize = new String(Base64.getUrlDecoder.decode(encoded), UTF_8)
    val i = uriWithSize.lastIndexOf("#size=")
    if (i < 0) {
      throw new IllegalArgumentException(s"$path is not a valid delta-sharing path")
    }
    val uri = uriWithSize.substring(0, i)
    val size = uriWithSize.substring(i + "#size=".length).toLong
    (new URI(uri), size)
  }
}
