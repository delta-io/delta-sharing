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

import java.io.{ByteArrayInputStream, InputStream}
import java.net.{SocketException, SocketTimeoutException}
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicInteger

import org.apache.hadoop.fs.FileSystem
import org.apache.http.{HttpEntity, HttpStatus, ProtocolVersion}
import org.apache.http.client.HttpClient
import org.apache.http.message.BasicHttpResponse
import org.apache.spark.SparkFunSuite
import org.apache.spark.delta.sharing.PreSignedUrlFetcher
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar

import io.delta.sharing.client.util.{RetryUtils, UnexpectedHttpStatus}

class RandomAccessHttpInputStreamSuite extends SparkFunSuite with MockitoSugar {

  private def createResponse(status: Int): BasicHttpResponse = {
    new BasicHttpResponse(new ProtocolVersion("HTTP", 1, 1), status, "")
  }

  private def createMockClient(status: Int): HttpClient = {
    val client = mock[HttpClient]
    when(client.execute(any())).thenReturn(createResponse(status))
    client
  }

  private def createMockFetcher(uri: String): PreSignedUrlFetcher = {
    val fetcher = mock[PreSignedUrlFetcher]
    when(fetcher.getUrl()).thenReturn(uri)
    fetcher
  }

  /**
   * Build a mocked `HttpClient` whose 206 responses return a fresh `InputStream` for each call,
   * supplied by `contentFactory`. This mirrors how `RandomAccessHttpInputStream` reopens the
   * stream on each retry attempt.
   */
  private def createPartialContentClient(contentFactory: () => InputStream): HttpClient = {
    val entity = mock[HttpEntity]
    when(entity.getContent).thenAnswer(_ => contentFactory())
    val response = new BasicHttpResponse(
      new ProtocolVersion("HTTP", 1, 1), HttpStatus.SC_PARTIAL_CONTENT, "")
    response.setEntity(entity)
    val client = mock[HttpClient]
    when(client.execute(any())).thenReturn(response)
    client
  }

  /** Run `body` with a no-op `RetryUtils.sleeper` to keep tests fast. */
  private def withInstantRetrySleep[T](body: => T): T = {
    val previous = RetryUtils.sleeper
    RetryUtils.sleeper = (_: Long) => ()
    try body finally RetryUtils.sleeper = previous
  }

  test("Failed HTTP requests should not show URI") {
    val uri = "test.uri"
    val stream = new RandomAccessHttpInputStream(
      createMockClient(HttpStatus.SC_OK),
      createMockFetcher(uri),
      1000L,
      new FileSystem.Statistics("idbfs"),
      10
    )
    val error = intercept[UnexpectedHttpStatus] {
      stream.seek(100L)
    }
    assert(!error.getMessage().contains(uri))
  }

  test("read(buf, off, len) retries on transient stream errors when enabled") {
    val data = "hello-world".getBytes(StandardCharsets.UTF_8)
    val callCount = new AtomicInteger(0)
    val client = createPartialContentClient { () =>
      if (callCount.getAndIncrement() == 0) {
        new InputStream {
          override def read(): Int = throw new SocketTimeoutException("transient failure")
          override def read(b: Array[Byte], off: Int, len: Int): Int =
            throw new SocketTimeoutException("transient failure")
        }
      } else {
        new ByteArrayInputStream(data)
      }
    }
    val stream = new RandomAccessHttpInputStream(
      client,
      createMockFetcher("test.uri"),
      data.length.toLong,
      new FileSystem.Statistics("idbfs"),
      numRetries = 3,
      maxRetryDuration = Long.MaxValue,
      logPreSignedUrlAccess = false,
      retryStreamReadOnError = true
    )

    val buf = new Array[Byte](data.length)
    withInstantRetrySleep {
      val n = stream.read(buf, 0, data.length)
      assert(n == data.length)
    }
    assert(new String(buf, StandardCharsets.UTF_8) == "hello-world")
    assert(stream.getPos == data.length.toLong)
    // First attempt failed and reopened the stream once, second attempt succeeded.
    assert(callCount.get() == 2)
  }

  test("read(buf, off, len) does not retry stream errors by default") {
    val data = "hello-world".getBytes(StandardCharsets.UTF_8)
    val callCount = new AtomicInteger(0)
    val client = createPartialContentClient { () =>
      callCount.incrementAndGet()
      new InputStream {
        override def read(): Int = throw new SocketTimeoutException("transient failure")
        override def read(b: Array[Byte], off: Int, len: Int): Int =
          throw new SocketTimeoutException("transient failure")
      }
    }
    val stream = new RandomAccessHttpInputStream(
      client,
      createMockFetcher("test.uri"),
      data.length.toLong,
      new FileSystem.Statistics("idbfs"),
      numRetries = 3
    )

    val buf = new Array[Byte](data.length)
    intercept[SocketTimeoutException] {
      stream.read(buf, 0, data.length)
    }
    // Stream was opened exactly once; the read failure was propagated without retry.
    assert(callCount.get() == 1)
    assert(stream.getPos == 0L)
  }

  test("read(buf, off, len) recovers after partial bytes followed by connection reset") {
    val data = "hello-world-this-is-a-longer-payload".getBytes(StandardCharsets.UTF_8)
    val splitAt = 10
    val callCount = new AtomicInteger(0)
    // First underlying stream serves `data[0, splitAt)` then throws SocketException
    // ("Connection reset") on the next read. Second stream serves the remainder.
    val client = createPartialContentClient { () =>
      val attempt = callCount.getAndIncrement()
      if (attempt == 0) {
        val backing = new ByteArrayInputStream(data, 0, splitAt)
        new InputStream {
          override def read(): Int = {
            val b = backing.read()
            if (b == -1) throw new SocketException("Connection reset")
            b
          }
          override def read(b: Array[Byte], off: Int, len: Int): Int = {
            val n = backing.read(b, off, len)
            if (n == -1) throw new SocketException("Connection reset")
            n
          }
        }
      } else {
        new ByteArrayInputStream(data, splitAt, data.length - splitAt)
      }
    }

    val stream = new RandomAccessHttpInputStream(
      client,
      createMockFetcher("test.uri"),
      data.length.toLong,
      new FileSystem.Statistics("idbfs"),
      numRetries = 3,
      maxRetryDuration = Long.MaxValue,
      logPreSignedUrlAccess = false,
      retryStreamReadOnError = true
    )

    val buf = new Array[Byte](data.length)
    withInstantRetrySleep {
      var totalRead = 0
      while (totalRead < data.length) {
        val n = stream.read(buf, totalRead, data.length - totalRead)
        assert(n > 0, s"read returned $n after $totalRead bytes")
        totalRead += n
      }
      assert(totalRead == data.length)
    }
    assert(new String(buf, StandardCharsets.UTF_8) ==
      new String(data, StandardCharsets.UTF_8))
    assert(stream.getPos == data.length.toLong)
    // Exactly two underlying streams opened: original (delivered prefix then errored)
    // and the post-retry stream (delivered the remainder from the advanced pos).
    assert(callCount.get() == 2)
  }

  test("read() (single byte) retries on transient stream errors when enabled") {
    val data = Array[Byte]('A'.toByte)
    val callCount = new AtomicInteger(0)
    val client = createPartialContentClient { () =>
      if (callCount.getAndIncrement() == 0) {
        new InputStream {
          override def read(): Int = throw new SocketTimeoutException("transient failure")
        }
      } else {
        new ByteArrayInputStream(data)
      }
    }
    val stream = new RandomAccessHttpInputStream(
      client,
      createMockFetcher("test.uri"),
      data.length.toLong,
      new FileSystem.Statistics("idbfs"),
      numRetries = 3,
      maxRetryDuration = Long.MaxValue,
      logPreSignedUrlAccess = false,
      retryStreamReadOnError = true
    )

    withInstantRetrySleep {
      assert(stream.read() == 'A'.toInt)
    }
    assert(callCount.get() == 2)
    assert(stream.getPos == 1L)
  }

  test("persistent failures use a single retry loop (no nested (n+1)^2 blowup)") {
    val numRetries = 3
    val executeCount = new AtomicInteger(0)
    val client = mock[HttpClient]
    // Always fail the underlying HTTP request with a retryable transient error. Because the
    // read path calls `reopen(retry = false)`, only the outer `doStreamRead` loop retries.
    when(client.execute(any())).thenAnswer { _ =>
      executeCount.incrementAndGet()
      throw new SocketTimeoutException("persistent failure")
    }
    val stream = new RandomAccessHttpInputStream(
      client,
      createMockFetcher("test.uri"),
      1024L,
      new FileSystem.Statistics("idbfs"),
      numRetries = numRetries,
      maxRetryDuration = Long.MaxValue,
      logPreSignedUrlAccess = false,
      retryStreamReadOnError = true
    )

    val buf = new Array[Byte](16)
    withInstantRetrySleep {
      intercept[SocketTimeoutException] {
        stream.read(buf, 0, buf.length)
      }
    }
    // A single retry loop: each outer attempt does one (non-retrying) reopen, so total HTTP
    // attempts are exactly `numRetries + 1` (4), not the `(numRetries + 1)^2 = 16` blowup.
    assert(executeCount.get() == numRetries + 1,
      s"Expected ${numRetries + 1} HTTP attempts, got ${executeCount.get()}")
  }
}
