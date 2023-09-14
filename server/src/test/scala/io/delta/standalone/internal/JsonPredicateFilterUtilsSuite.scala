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
import org.scalatest.FunSuite

import io.delta.sharing.server.util.JsonUtils

class JsonPredicateFilterUtilsSuite extends FunSuite {

  import JsonPredicateFilterUtils._

  test("evaluatePredicate") {
    val add1 = AddFile("foo1", Map("c2" -> "0"), 1, 1, true)
    val add2 = AddFile("foo2", Map("c2" -> "1"), 1, 1, true)
    val addFiles = Seq(add1, add2).zipWithIndex

    val hints1 =
      """{"op":"and","children":[
           |  {"op":"not","children":[
           |    {"op":"isNull","children":[
           |      {"op":"column","name":"c2","valueType":"int"}]}]},
           |  {"op":"equal","children":[
           |    {"op":"column","name":"c2","valueType":"int"},
           |    {"op":"literal","value":"0","valueType":"int"}]}
           |]}""".stripMargin.replaceAll("\n", "").replaceAll(" ", "")
    assert(Seq(addFiles(0)) == evaluatePredicate(Some(hints1), false, addFiles))
    assert(Seq(addFiles(0)) == evaluatePredicate(Some(hints1), true, addFiles))

    val hints2 =
      """{"op":"and","children":[
           |  {"op":"not","children":[
           |    {"op":"isNull","children":[
           |      {"op":"column","name":"c2","valueType":"int"}]}]},
           |  {"op":"equal","children":[
           |    {"op":"column","name":"c2","valueType":"int"},
           |    {"op":"literal","value":"1","valueType":"int"}]}
           |]}""".stripMargin.replaceAll("\n", "").replaceAll(" ", "")
    assert(Seq(addFiles(1)) == evaluatePredicate(Some(hints2), false, addFiles))
    assert(Seq(addFiles(1)) == evaluatePredicate(Some(hints2), true, addFiles))

    val hints3 =
      """{"op":"and","children":[
           |  {"op":"not","children":[
           |    {"op":"isNull","children":[
           |      {"op":"column","name":"c2","valueType":"int"}]}]},
           |  {"op":"lessThan","children":[
           |    {"op":"column","name":"c2","valueType":"int"},
           |    {"op":"literal","value":"2","valueType":"int"}]}
           |]}""".stripMargin.replaceAll("\n", "").replaceAll(" ", "")
    assert(addFiles == evaluatePredicate(Some(hints3), false, addFiles))
    assert(addFiles == evaluatePredicate(Some(hints3), true, addFiles))

    // Unsupported expression
    val hints4 =
      """{"op":"UNSUPPORTED","children":[
           |  {"op":"not","children":[
           |    {"op":"isNull","children":[
           |      {"op":"column","name":"c2","valueType":"int"}]}]},
           |  {"op":"lessThan","children":[
           |    {"op":"column","name":"c2","valueType":"int"},
           |    {"op":"literal","value":"2","valueType":"int"}]}
           |]}""".stripMargin.replaceAll("\n", "").replaceAll(" ", "")
    assert(addFiles == evaluatePredicate(Some(hints4), false, addFiles))
    assert(addFiles == evaluatePredicate(Some(hints4), true, addFiles))
  }

  test("evaluatePredicateV2") {
    // We will use the following stats.
    // stats1: Column age values in range [0, 20]
    // stats2: Column age values in range [15, 30]
    val stats1 =
      """{"numRecords":1,"minValues":{"age":"0"},"maxValues":{"age":"50"}
          |}""".stripMargin.replaceAll("\n", "").replaceAll(" ", "")
    val stats2 =
      """{"numRecords":1,"minValues":{"age":"40"},"maxValues":{"age":"100"}
          |}""".stripMargin.replaceAll("\n", "").replaceAll(" ", "")

    // We will use the following partition values.
    val part1 = Map("hireDate" -> "2022-01-01")
    val part2 = Map("hireDate" -> "2022-02-01")

    val add1 = AddFile("file1", part1, 1, 1, true, stats1)
    val add2 = AddFile("file2", part2, 1, 1, true, stats2)
    val addFiles = Seq(add1, add2).zipWithIndex

    // Hints that only match stats1.
    val hints1 =
      """{"op":"and","children":[
           |  {"op":"not","children":[
           |    {"op":"isNull","children":[
           |      {"op":"column","name":"age","valueType":"int"}]}]},
           |  {"op":"equal","children":[
           |    {"op":"column","name":"age","valueType":"int"},
           |    {"op":"literal","value":"10","valueType":"int"}]}
           |]}""".stripMargin.replaceAll("\n", "").replaceAll(" ", "")
    assert(Seq(addFiles(0)) == evaluatePredicate(Some(hints1), true, addFiles))

    // Hints that only match stats2.
    val hints2 =
      """{"op":"and","children":[
           |  {"op":"not","children":[
           |    {"op":"isNull","children":[
           |      {"op":"column","name":"age","valueType":"int"}]}]},
           |  {"op":"equal","children":[
           |    {"op":"column","name":"age","valueType":"int"},
           |    {"op":"literal","value":"90","valueType":"int"}]}
           |]}""".stripMargin.replaceAll("\n", "").replaceAll(" ", "")
    assert(Seq(addFiles(1)) == evaluatePredicate(Some(hints2), true, addFiles))

    // Hints that match stats1 and stats2.
    val hints3 =
      """{"op":"and","children":[
           |  {"op":"not","children":[
           |    {"op":"isNull","children":[
           |      {"op":"column","name":"age","valueType":"int"}]}]},
           |  {"op":"lessThan","children":[
           |    {"op":"column","name":"age","valueType":"int"},
           |    {"op":"literal","value":"55","valueType":"int"}]}
           |]}""".stripMargin.replaceAll("\n", "").replaceAll(" ", "")
    assert(addFiles == evaluatePredicate(Some(hints3), true, addFiles))

    // Hints that match stats1,stats2 and part1.
    val hints4 =
      """{"op":"and","children":[
           |  {"op":"not","children":[
           |    {"op":"isNull","children":[
           |      {"op":"column","name":"age","valueType":"int"}]}]},
           |  {"op":"lessThan","children":[
           |    {"op":"column","name":"age","valueType":"int"},
           |    {"op":"literal","value":"55","valueType":"int"}]},
           |  {"op":"lessThanOrEqual","children":[
           |    {"op":"column","name":"hireDate","valueType":"date"},
           |    {"op":"literal","value":"2022-01-02","valueType":"date"}]}
           |]}""".stripMargin.replaceAll("\n", "").replaceAll(" ", "")
    assert(Seq(addFiles(0)) == evaluatePredicate(Some(hints4), true, addFiles))

    // Unsupported expression
    val hints5 =
      """{"op":"UNSUPPORTED","children":[
           |  {"op":"not","children":[
           |    {"op":"isNull","children":[
           |      {"op":"column","name":"c2","valueType":"int"}]}]},
           |  {"op":"lessThan","children":[
           |    {"op":"column","name":"c2","valueType":"int"},
           |    {"op":"literal","value":"2","valueType":"int"}]}
           |]}""".stripMargin.replaceAll("\n", "").replaceAll(" ", "")
    assert(addFiles == evaluatePredicate(Some(hints5), true, addFiles))
  }
}
