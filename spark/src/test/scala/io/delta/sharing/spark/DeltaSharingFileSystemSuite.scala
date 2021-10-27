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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkFunSuite

import io.delta.sharing.spark.model.AddFile

class DeltaSharingFileSystemSuite extends SparkFunSuite {
  import DeltaSharingFileSystem._

  test("encode and decode") {
    val tablePath = new Path("https://delta.io/foo")
    val addFile = AddFile("unused", "id", Map.empty, 100)
    assert(decode(encode(tablePath, addFile)) ==
      DeltaSharingPath("https://delta.io/foo", "id", 100))
    // Converting the encoded string to a string and re-create Path should still work
    assert(decode(new Path(encode(tablePath, addFile).toString)) ==
      DeltaSharingPath("https://delta.io/foo", "id", 100))
  }

  test("file system should be cached") {
    val tablePath = new Path("https://delta.io/foo")
    val addFile = AddFile("unused", "id", Map.empty, 100)
    val path = encode(tablePath, addFile)
    val conf = new Configuration
    val fs = path.getFileSystem(conf)
    assert(fs.isInstanceOf[DeltaSharingFileSystem])
    assert(fs eq path.getFileSystem(conf))
  }
}
