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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkFunSuite

class DeltaSharingFileSystemSuite extends SparkFunSuite {
  import DeltaSharingFileSystem._

  test("encode and decode") {
    val uri = new URI("https://delta.io/foo")
    assert(restoreUri(createPath(uri, 100)) == (uri, 100))
    assert(restoreUri(new Path(createPath(uri, 100).toString)) == (uri, 100))

    val uri2 = new URI("file:///foo")
    assert(restoreUri(createPath(uri2, 200)) == (uri2, 200))
    assert(restoreUri(new Path(createPath(uri2, 100).toString)) == (uri2, 100))
  }

  test("file system should be cached") {
    val uri = new URI("https://delta.io/foo")
    val path = createPath(uri, 100)
    val conf = new Configuration
    val fs = path.getFileSystem(conf)
    assert(fs.isInstanceOf[DeltaSharingFileSystem])
    assert(fs eq path.getFileSystem(conf))
  }
}
