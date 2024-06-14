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

import org.apache.hadoop.fs.FileSystem
import org.apache.http.{HttpStatus, ProtocolVersion}
import org.apache.http.client.HttpClient
import org.apache.http.message.BasicHttpResponse
import org.apache.spark.SparkFunSuite
import org.apache.spark.delta.sharing.PreSignedUrlFetcher
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar

import io.delta.sharing.spark.RandomAccessHttpInputStream
import io.delta.sharing.spark.util.UnexpectedHttpStatus

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
}
