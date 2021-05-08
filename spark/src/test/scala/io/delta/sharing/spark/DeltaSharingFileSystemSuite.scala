package io.delta.sharing.spark

import java.net.URI

import org.apache.spark.SparkFunSuite

class DeltaSharingFileSystemSuite extends SparkFunSuite {
  import DeltaSharingFileSystem._

  test("encode and decode") {
    val uri = new URI("https://delta.io/foo")
    assert(restoreUri(createPath(uri, 100)) == (uri, 100))

    val uri2 = new URI("file:///foo")
    assert(restoreUri(createPath(uri2, 200)) == (uri2, 200))
  }
}
