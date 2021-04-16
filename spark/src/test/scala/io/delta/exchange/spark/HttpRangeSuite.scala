package io.delta.exchange.spark

import java.net.{HttpURLConnection, URL}
import java.util.concurrent.TimeUnit.NANOSECONDS

import org.apache.commons.io.IOUtils
import org.apache.spark.SparkFunSuite

import scala.collection.mutable.ArrayBuffer

class HttpRangeSuite extends SparkFunSuite {

  def time[T](f: => T): T = {
    val start = System.nanoTime()
    val ret = f
    val end = System.nanoTime()
    // scalastyle:off println
    println(s"Time taken: ${NANOSECONDS.toMillis(end - start)} ms")
    // scalastyle:on println
    ret
  }

  def testSignedUrl(url: URL): Unit = {
    def getRange(start: Long, end: Long): Array[Byte] = {
      val conn = url.openConnection().asInstanceOf[HttpURLConnection]
      // http end is inclusive.
      conn.setRequestProperty("Range", s"bytes=${start}-${end - 1}")
      val input = conn.getInputStream
      try {
        IOUtils.toByteArray(input)
      } finally {
        input.close()
      }
    }

    // Read the entire file content
    val allBytes = IOUtils.toByteArray(url)
    val size = allBytes.length
    // Read 10 bytes each time and concat them
    var bytes = ArrayBuffer[Byte]()
    var pos = 0
    while (pos < size) {
      bytes ++= getRange(pos, math.min(pos + 10, size))
      pos += 10
    }
    // Compare the content to verify `getRange` is correct
    assert(allBytes.toList == bytes.toList)
  }

  test("aws") {
    assume(sys.env.get("DELTA_EXCHANGE_TEST_AWS_ACCESS_KEY").nonEmpty)

    import TestResource.AWS._

    val objectKey = "ryan_delta_remote_test/" +
      "17d3543c-1a6f-480b-9f1b-22ef64a711b0/" +
      "part-00001-b5811302-fc01-40d1-acc0-68cdfa032dad-c000.snappy.parquet"
    val url = signer.sign(bucket, objectKey)
    testSignedUrl(url)

    time {
      for (_ <- 0 until 1000) {
        signer.sign(bucket, objectKey)
      }
    }
  }

  test("azure") {
    assume(sys.env.get("DELTA_EXCHANGE_TEST_AZURE_STORAGE_ACCOUNT").nonEmpty)

    import TestResource.Azure._

    val testContainer = s"ryan-delta-remote-test"
    val blobName =
      "206cd5dd-c967-4a3d-96c2-76b48f5b10a3/" +
        "part-00000-93bc5f44-47c4-4c00-8b41-f3e68fae70f0-c000.snappy.parquet"
    val url = signer.sign(testContainer, blobName)
    testSignedUrl(url)

    time {
      for (_ <- 0 until 1000) {
        signer.sign(testContainer, blobName)
      }
    }
  }

  test("gcp") {
    assume(sys.env.get("DELTA_EXCHANGE_TEST_GCP_KEY").nonEmpty)

    import TestResource.GCP._

    val objectName =
      "ryan_delta_remote_test/part-00000-3fb3462d-a0ca-4f18-8cba-638a6c8af16e-c000.snappy.parquet"
    val url = signer.sign(bucket, objectName)
    testSignedUrl(url)

    time {
      for (_ <- 0 until 1000) {
        signer.sign(bucket, objectName)
      }
    }
  }
}
