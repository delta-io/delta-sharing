package io.delta.exchange.spark

import java.net.URI

import org.apache.spark.SparkFunSuite

class DeltaFileSystemSuite extends SparkFunSuite {
  test("encode and decode") {
    val uri = new URI("https://delta.io/foo")
    assert(DeltaFileSystem.restoreUri(DeltaFileSystem.createPath(uri, 100)) == (uri, 100))

    val uri2 = new URI("file:///foo")
    assert(DeltaFileSystem.restoreUri(DeltaFileSystem.createPath(uri2, 200)) == (uri2, 200))
  }
}
