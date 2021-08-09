// scalastyle:off headerCheck
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * This file contains code from the Apache Hadoop project (original license above).
 * It contains modifications, which are licensed as follows:
 */

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

import java.io.{EOFException, InputStream, IOException}
import java.nio.charset.StandardCharsets.UTF_8

import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.{FileSystem, FSExceptionMessages, FSInputStream}
import org.apache.http.HttpStatus
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.{HttpGet, HttpRequestBase}
import org.apache.http.conn.EofSensorInputStream
import org.apache.spark.delta.sharing.PreSignedUrlFetcher
import org.apache.spark.internal.Logging

import io.delta.sharing.spark.util.{RetryUtils, UnexpectedHttpStatus}

/**
 * This is a special input stream to provide random access over HTTP. This class requires the server
 * side to support HTTP Range header.
 */
private[sharing] class RandomAccessHttpInputStream(
    client: HttpClient,
    fetcher: PreSignedUrlFetcher,
    stats: FileSystem.Statistics,
    numRetries: Int) extends FSInputStream with Logging {

  private val contentLength = fetcher.path.fileSize
  private var closed = false
  private var pos = 0L
  private var currentStream: InputStream = null
  private var uri: String = null

  private def assertNotClosed(): Unit = {
    if (closed) {
      throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED)
    }
    val newUrl = fetcher.getUrl()
    if (uri != newUrl) {
      // Abort the current open stream so that we will re-open a new stream using the new url
      uri = newUrl
      abortCurrentStream()
    }
  }

  override def seek(pos: Long): Unit = synchronized {
    if (this.pos != pos) {
      assertNotClosed()
      reopen(pos)
    }
  }

  override def getPos: Long = synchronized {
    pos
  }

  override def seekToNewSource(targetPos: Long): Boolean = {
    // We don't support this feature
    false
  }

  override def read(): Int = synchronized {
    assertNotClosed()
    if (currentStream == null) {
      reopen(pos)
    }
    val byte = currentStream.read()
    if (byte >= 0) {
      pos += 1
    }
    if (stats != null && byte >= 0) {
      stats.incrementBytesRead(1)
    }
    byte
  }

  private def createHttpRequest(start: Long): HttpRequestBase = {
    val request = new HttpGet(uri)
    val rangeValue = s"bytes=$start-${contentLength - 1L}"
    request.addHeader("Range", rangeValue)
    request
  }

  override def read(buf: Array[Byte], off: Int, len: Int): Int = synchronized {
    assertNotClosed()
    if (currentStream == null) {
      reopen(pos)
    }
    val byteRead = currentStream.read(buf, off, len)
    if (byteRead > 0) {
      pos += byteRead
    }
    if (stats != null && byteRead > 0) {
      stats.incrementBytesRead(byteRead)
    }
    byteRead
  }

  private def reopen(pos: Long): Unit = {
    if (currentStream != null) {
      logDebug(s"Aborting old stream to open at pos $pos")
      abortCurrentStream()
    }
    if (pos < 0L) {
      throw new EOFException(FSExceptionMessages.NEGATIVE_SEEK + " " + pos)
    } else if (contentLength > 0L && pos > this.contentLength - 1L) {
      throw new EOFException(FSExceptionMessages.CANNOT_SEEK_PAST_EOF + " " + pos)
    } else {
      logDebug(s"Opening file $uri at pos $pos")

     val entity = RetryUtils.runWithExponentialBackoff(numRetries) {
        val httpRequest = createHttpRequest(pos)
        val response = client.execute(httpRequest)
        val status = response.getStatusLine()
        val entity = response.getEntity()
        val statusCode = status.getStatusCode
        if (statusCode != HttpStatus.SC_PARTIAL_CONTENT) {
          // Note: we will still fail if the server returns 200 because it means the server doesn't
          // support HTTP Range header.
          val errorBody = if (entity == null) {
            ""
          } else {
            val input = entity.getContent()
            try {
              IOUtils.toString(input, UTF_8)
            } finally {
              input.close()
            }
          }
          throw new UnexpectedHttpStatus(
            s"HTTP request failed with status: $status $errorBody",
            statusCode)
        }
        entity
      }
      currentStream = entity.getContent()
      this.pos = pos
    }
  }

  override def available(): Int = synchronized {
    assertNotClosed()
    currentStream.available()
  }

  /**
   * Aborts `currentStream` without reading any more data. Apache `HttpClient` tries to read the
   * rest of bytes in `Close` in order to reuse the connection. However, it's not efficient when we
   * need to discard a lot of bytes. This method provides a way to not reuse the connection when the
   * remaining bytes are still a lot. See `EofSensorInputStream` for more details.
   */
  private def abortCurrentStream(): Unit = {
    if (currentStream != null) {
      currentStream match {
        case e: EofSensorInputStream => e.abortConnection()
        case _ => currentStream.close()
      }
      currentStream = null
    }
  }

  override def close(): Unit = synchronized {
    if (!closed) {
      super.close()
      closed = true
      if (currentStream != null) {
        if (contentLength - pos <= 4096) {
          // Close, rather than abort, so that the http connection can be reused.
          currentStream.close()
          currentStream = null
        } else {
          // Abort, rather than just close, the underlying stream. Otherwise, the remaining bytes
          // are read while closing the stream.
          abortCurrentStream()
        }
      }
    }
  }
}
