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
    val addFiles = add1 :: add2 :: Nil

    val hints1 =
      """{"op":"and","children":[
           |  {"op":"not","children":[
           |    {"op":"isNull","children":[
           |      {"op":"column","name":"c2","valueType":"int"}]}]},
           |  {"op":"equal","children":[
           |    {"op":"column","name":"c2","valueType":"int"},
           |    {"op":"literal","value":"0","valueType":"int"}]}
           |]}""".stripMargin.replaceAll("\n", "").replaceAll(" ", "")
    assert(add1 :: Nil == evaluatePredicate(Some(hints1), addFiles))

    val hints2 =
      """{"op":"and","children":[
           |  {"op":"not","children":[
           |    {"op":"isNull","children":[
           |      {"op":"column","name":"c2","valueType":"int"}]}]},
           |  {"op":"equal","children":[
           |    {"op":"column","name":"c2","valueType":"int"},
           |    {"op":"literal","value":"1","valueType":"int"}]}
           |]}""".stripMargin.replaceAll("\n", "").replaceAll(" ", "")
    assert(add2 :: Nil == evaluatePredicate(Some(hints2), addFiles))

    val hints3 =
      """{"op":"and","children":[
           |  {"op":"not","children":[
           |    {"op":"isNull","children":[
           |      {"op":"column","name":"c2","valueType":"int"}]}]},
           |  {"op":"lessThan","children":[
           |    {"op":"column","name":"c2","valueType":"int"},
           |    {"op":"literal","value":"2","valueType":"int"}]}
           |]}""".stripMargin.replaceAll("\n", "").replaceAll(" ", "")
    assert(addFiles == evaluatePredicate(Some(hints3), addFiles))

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
    assert(addFiles == evaluatePredicate(Some(hints4), addFiles))
  }
}
