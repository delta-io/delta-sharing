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

package io.delta.sharing.server

import java.net.{HttpURLConnection, URL}

import com.linecorp.armeria.server.Server
import org.apache.commons.io.IOUtils
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class TestViewSharingServerSuite extends FunSuite with BeforeAndAfterAll {
  private var server: Server = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    server = TestViewSharingServer.start(port = 0)
  }

  override def afterAll(): Unit = {
    try {
      if (server != null) {
        server.stop().get()
      }
    } finally {
      super.afterAll()
    }
  }

  test("versionlessCDF is only returned for versionless CDF responses") {
    def getResponseHeaders(
        objectName: String,
        path: String,
        responseFormat: String): Map[String, String] = {
      val url = new URL(
        s"http://localhost:${server.activeLocalPort()}/delta-sharing/shares/view_share/" +
          s"schemas/default/tables/$objectName/$path")
      val connection = url.openConnection().asInstanceOf[HttpURLConnection]
      connection.setRequestProperty(
        DeltaSharingService.DELTA_SHARING_CAPABILITIES_HEADER,
        s"responseformat=$responseFormat;versionlessCDF=true")
      try {
        assert(connection.getResponseCode == 200)
        val headers = Map(
          "capabilities" -> connection.getHeaderField(
            DeltaSharingService.DELTA_SHARING_CAPABILITIES_HEADER),
          "version" -> connection.getHeaderField(DeltaSharingService.DELTA_TABLE_VERSION_HEADER))
        connection.getInputStream.close()
        headers
      } finally {
        connection.disconnect()
      }
    }

    Seq("parquet", "delta").foreach { responseFormat =>
      val responseHeaders = Seq("view", "table").map { objectName =>
        objectName -> getResponseHeaders(
          objectName,
          "changes?startingTimestamp=2022-05-09T00:00:00Z",
          responseFormat)
      }.toMap
      val viewMetadataHeaders = getResponseHeaders("view", "metadata", responseFormat)

      assert(responseHeaders("view")("capabilities") ==
        s"responseformat=$responseFormat;versionlessCDF=true")
      assert(responseHeaders("view")("version") == null)
      assert(responseHeaders("table")("capabilities") == s"responseformat=$responseFormat")
      assert(responseHeaders("table")("version") == "1")
      assert(viewMetadataHeaders("capabilities") == s"responseformat=$responseFormat")
      assert(viewMetadataHeaders("version") == "1")
    }
  }

  Seq(TestViewSharingServer.VIEW_FILE_PATH, TestViewSharingServer.TABLE_FILE_PATH).foreach { path =>
    test(s"$path supports full downloads and byte ranges") {
      def download(range: Option[String] = None): (Int, String, Array[Byte]) = {
        val connection = new URL(
          s"http://localhost:${server.activeLocalPort()}$path?X-Amz-Signature=test")
          .openConnection().asInstanceOf[HttpURLConnection]
        range.foreach(connection.setRequestProperty("Range", _))
        try {
          val status = connection.getResponseCode
          val stream = if (status >= 400) connection.getErrorStream else connection.getInputStream
          val bytes = if (stream == null) Array.emptyByteArray else {
            try {
              IOUtils.toByteArray(stream)
            } finally {
              stream.close()
            }
          }
          (status, connection.getHeaderField("Content-Range"), bytes)
        } finally {
          connection.disconnect()
        }
      }

      val (status, contentRange, bytes) = download()
      assert(status == 200)
      assert(contentRange == null)
      assert(bytes.take(4).sameElements("PAR1".getBytes("UTF-8")))
      assert(bytes.takeRight(4).sameElements("PAR1".getBytes("UTF-8")))

      Seq(
        ("bytes=0-3", 0, 3),
        ("bytes=4-", 4, bytes.length - 1),
        ("bytes=-8", bytes.length - 8, bytes.length - 1),
        (s"bytes=4-${bytes.length + 10}", 4, bytes.length - 1),
        (s"bytes=-${bytes.length + 10}", 0, bytes.length - 1)
      ).foreach { case (range, first, last) =>
        val (rangeStatus, rangeHeader, rangeBytes) = download(Some(range))
        assert(rangeStatus == 206)
        assert(rangeHeader == s"bytes $first-$last/${bytes.length}")
        assert(rangeBytes.sameElements(bytes.slice(first, last + 1)))
      }

      Seq(s"bytes=${bytes.length}-", "bytes=4-3", "bytes=-0", "bytes=bad", "bytes=-",
        "bytes=999999999999999999999-", "bytes=0-1,4-5").foreach { range =>
        val (rangeStatus, rangeHeader, _) = download(Some(range))
        assert(rangeStatus == 416)
        assert(rangeHeader == s"bytes */${bytes.length}")
      }
    }
  }
}
