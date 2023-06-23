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

import java.io.ByteArrayInputStream
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
import org.apache.spark.storage.BlockId

import io.delta.sharing.spark.model.FileAction

/** Read-only file system for delta sharing log. */
private[sharing] class DeltaSharingLogFileSystem extends FileSystem {
  import DeltaSharingLogFileSystem._

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
    // scalastyle:off println
    consolePrintln(s"----[linzhou]----delta-sharing-log, open:${f.toString}")
    if (f.toString ==
      "delta-sharing-log:/cdf_table_cdf_enabled/_delta_log/00000000000000000000.json") {
      val blockManager = SparkEnv.get.blockManager
      val blockId = BlockId(s"test_randomeQuery_cdf_table_cdf_enabled_0.json")
      val content = blockManager.getSingle[String](blockId).get
      Console.println(s"----[linzhou]----FileSystem 0.json content:${content}")

      return new FSDataInputStream(new SeekableByteArrayInputStream(
        content.getBytes(), "000.json"))
    }
    consolePrintln(s"----[linzhou]----returning emptry for :${f.toString}")
    new FSDataInputStream(new SeekableByteArrayInputStream("".getBytes(), f.toString))
  }

  def consolePrintln(str: String): Unit = if (true) { Console.println(str) }

  override def create(
                       f: Path,
                       permission: FsPermission,
                       overwrite: Boolean,
                       bufferSize: Int,
                       replication: Short,
                       blockSize: Long,
                       progress: Progressable): FSDataOutputStream = {
    consolePrintln(s"----[linzhou]----create:${f}")
    throw new UnsupportedOperationException("create")
  }

  override def append(f: Path, bufferSize: Int, progress: Progressable): FSDataOutputStream = {
    consolePrintln(s"----[linzhou]----append:${f}")
    throw new UnsupportedOperationException("append")
  }

  override def rename(src: Path, dst: Path): Boolean = {
    consolePrintln(s"----[linzhou]----rename:${src}")
    throw new UnsupportedOperationException("rename")
  }

  override def delete(f: Path, recursive: Boolean): Boolean = {
    consolePrintln(s"----[linzhou]----delete:${f}")
    throw new UnsupportedOperationException("delete")
  }

  override def exists(f: Path): Boolean = {
    consolePrintln(s"----[linzhou]----exists:${f}")
    return f.toString == "delta-sharing-log:/cdf_table_cdf_enabled/_delta_log"
  }

  override def listStatus(f: Path): Array[FileStatus] = {
    consolePrintln(s"----[linzhou]----listStatus:${f}")
    if (f.toString == "delta-sharing-log:/cdf_table_cdf_enabled/_delta_log") {
      val blockManager = SparkEnv.get.blockManager
      val blockId = BlockId(s"test_randomeQuery_cdf_table_cdf_enabled_0.json_size")
      val size = blockManager.getSingle[Long](blockId).get
      consolePrintln(s"----[linzhou]----size:${size}")
      val a = Array(
        new FileStatus(size, false, 0, 1, 0, new Path(
          "delta-sharing-log:/cdf_table_cdf_enabled/_delta_log/00000000000000000000.json"))
      )
      consolePrintln(s"----[linzhou]----listing:${a.toList}")
      return a
    }
    throw new UnsupportedOperationException(s"listStatus:${f}")
  }

  override def listStatusIterator(f: Path): RemoteIterator[FileStatus] = {
    consolePrintln(s"----[linzhou]----listStatusIterator:${f}")
    throw new UnsupportedOperationException("listStatusIterator")
  }

  override def setWorkingDirectory(new_dir: Path): Unit =
    throw new UnsupportedOperationException("setWorkingDirectory")

  override def getWorkingDirectory: Path = new Path(getUri)

  override def mkdirs(f: Path, permission: FsPermission): Boolean = {
    consolePrintln(s"----[linzhou]----mkdirs:${f},${permission}")
    throw new UnsupportedOperationException("mkdirs")
  }

  override def getFileStatus(f: Path): FileStatus = {
    consolePrintln(s"----[linzhou]----getFileStatus:${f}")
    new FileStatus(0, false, 0, 1, 0, f)
  }

  override def finalize(): Unit = {
    consolePrintln(s"----[linzhou]----finalize")
    try super.finalize() finally close()
  }

  override def close(): Unit = {
    try super.close() finally httpClient.close()
  }
}

private[sharing] object DeltaSharingLogFileSystem {

  val SCHEME = "delta-sharing-log"

  case class DeltaSharingPath(tablePath: String, fileId: String, fileSize: Long) {

    /**
     * Convert `DeltaSharingPath` to a `Path` in the following format:
     *
     * ```
     * delta-sharing-log:///<url encoded table path>/<url encoded file id>/<size>
     * ```
     *
     * This format can be decoded by `DeltaSharingLogFileSystem.decode`.
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

  /**
   * A ByteArrayInputStream that implements interfaces required by FSDataInputStream.
   */
  private class SeekableByteArrayInputStream(bytes: Array[Byte], fileName: String)
    extends ByteArrayInputStream(bytes) with Seekable with PositionedReadable {
    assert(available == bytes.length)

    def consolePrintln(str: String): Unit = if (false) { Console.println(str) }

    override def seek(pos: Long): Unit = {
      consolePrintln(s"----[linzhou]------seek pos: $pos, avail:$available, for $fileName")
      if (mark != 0) {
        consolePrintln(s"----[linzhou]------seek exception, mark: $mark")
        throw new IllegalStateException("Cannot seek if mark is set")
      }
      consolePrintln(s"----[linzhou]------seek reset")
      reset()
      skip(pos)
    }

    override def seekToNewSource(pos: Long): Boolean = {
      consolePrintln(s"----[linzhou]------seekToNewSource, for $fileName")
      false  // there aren't multiple sources available
    }

    override def getPos(): Long = {
      consolePrintln(s"----[linzhou]------getPos, for $fileName")
      bytes.length - available
    }

    override def read(buffer: Array[Byte], offset: Int, length: Int): Int = {
      consolePrintln(s"----[linzhou]------read pos:$pos, off: $offset, len: $length, for $fileName")
      //      if (pos >= bytes.length) {
      //        return -1
      //      }
      //      val readSize = math.min(length, bytes.length - pos).toInt
      //      System.arraycopy(bytes, pos.toInt, buffer, offset, readSize)
      val readSize = super.read(buffer, offset, length)
      consolePrintln(s"----[linzhou]------after read pos:$pos, readSize: $readSize")
      readSize
    }

    override def read(pos: Long, buffer: Array[Byte], offset: Int, length: Int): Int = {
      consolePrintln(s"----[linzhou]------read-pos input:$pos, offset: $offset, for $fileName")
      if (pos >= bytes.length) {
        return -1
      }
      val readSize = math.min(length, bytes.length - pos).toInt
      System.arraycopy(bytes, pos.toInt, buffer, offset, readSize)
      readSize
    }

    override def readFully(pos: Long, buffer: Array[Byte], offset: Int, length: Int): Unit = {
      consolePrintln(s"----[linzhou]--------readFully, offset:${offset}, for $fileName")
      System.arraycopy(bytes, pos.toInt, buffer, offset, length)
    }

    override def readFully(pos: Long, buffer: Array[Byte]): Unit = {
      consolePrintln(s"----[linzhou]--------readFully, for $fileName")
      System.arraycopy(bytes, pos.toInt, buffer, 0, buffer.length)
    }
  }
}
