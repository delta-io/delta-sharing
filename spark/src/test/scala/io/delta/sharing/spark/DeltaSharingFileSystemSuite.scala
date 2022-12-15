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
import org.apache.spark.SparkFunSuite

import io.delta.sharing.spark.model.{AddCDCFile, AddFile, AddFileForCDF, FileAction, RemoveFile}

class DeltaSharingFileSystemSuite extends SparkFunSuite {
  import DeltaSharingFileSystem._

  test("encode and decode") {
    val tablePath = "https://delta.io/foo"

    val actions: Seq[FileAction] = Seq(
      AddFile("unused", "id", Map.empty, 100),
      AddFileForCDF("unused_cdf", "id_cdf", Map.empty, 200, 1, 2),
      AddCDCFile("unused_cdc", "id_cdc", Map.empty, 300, 1, 2),
      RemoveFile("unused_rem", "id_rem", Map.empty, 400, 1, 2)
    )

    actions.foreach ( action => {
      assert(decode(encode(tablePath, action)) ==
        DeltaSharingPath("https://delta.io/foo", action.id, action.size))
    })
  }

  test("file system should be cached") {
    val tablePath = "https://delta.io/foo"
    val actions: Seq[FileAction] = Seq(
      AddFile("unused", "id", Map.empty, 100),
      AddFileForCDF("unused_cdf", "id_cdf", Map.empty, 200, 1, 2),
      AddCDCFile("unused_cdc", "id_cdc", Map.empty, 300, 1, 2),
      RemoveFile("unused_rem", "id_rem", Map.empty, 400, 1, 2)
    )

    actions.foreach( action => {
      val path = encode(tablePath, action)
      val conf = new Configuration
      val fs = path.getFileSystem(conf)
      assert(fs.isInstanceOf[DeltaSharingFileSystem])
      assert(fs eq path.getFileSystem(conf))
    })
  }
}
