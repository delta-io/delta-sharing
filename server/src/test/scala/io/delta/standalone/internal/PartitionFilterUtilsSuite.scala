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

package io.delta.standalone.internal

import io.delta.standalone.internal.actions.AddFile
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.scalatest.FunSuite

class PartitionFilterUtilsSuite extends FunSuite {

  import PartitionFilterUtils._

  test("evaluatePredicate") {
    val schema = StructType.fromDDL("c1 INT, c2 INT").json
    val add1 = AddFile("foo1", Map("c2" -> "0"), 1, 1, true)
    val add2 = AddFile("foo2", Map("c2" -> "1"), 1, 1, true)
    val addFiles = Seq(add1, add2).zipWithIndex
    assert(Seq(addFiles(0)) == evaluatePredicate(schema, "c2" :: Nil, "c2 = 0" :: Nil, addFiles))
    assert(Seq(addFiles(1)) == evaluatePredicate(schema, "c2" :: Nil, "c2 = 1" :: Nil, addFiles))
    assert(Seq(addFiles(1)) == evaluatePredicate(schema, "c2" :: Nil, "c2 > 0" :: Nil, addFiles))
    assert(Seq(addFiles(0)) == evaluatePredicate(schema, "c2" :: Nil, "c2 < 1" :: Nil, addFiles))
    assert(Seq(addFiles(1)) == evaluatePredicate(schema, "c2" :: Nil, "c2 >= 1" :: Nil, addFiles))
    assert(Seq(addFiles(0)) == evaluatePredicate(schema, "c2" :: Nil, "c2 <= 0" :: Nil, addFiles))
    assert(Seq(addFiles(1)) == evaluatePredicate(schema, "c2" :: Nil, "c2 <> 0" :: Nil, addFiles))
    assert(Seq(addFiles(0)) == evaluatePredicate(schema, "c2" :: Nil, "c2 <> 1" :: Nil, addFiles))
    assert(Nil == evaluatePredicate(schema, "c2" :: Nil, "c2 is null" :: Nil, addFiles))
    assert(addFiles == evaluatePredicate(schema, "c2" :: Nil, "c2 is not null" :: Nil, addFiles))
    assert(addFiles == evaluatePredicate(schema, "c2" :: Nil, "c2 is not null" :: Nil, addFiles))

    // Unsupported expression
    assert(addFiles == evaluatePredicate(schema, "c2" :: Nil, "c2 = 0 + 1" :: Nil, addFiles))
  }
}
