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

package io.delta.sharing.filters

import org.apache.spark.SparkFunSuite

class UnsupportedOpPrunerSuite extends SparkFunSuite {

  private def col(name: String) = ColumnOp(name, OpDataTypes.IntType)
  private def lit(v: String) = LiteralOp(v, OpDataTypes.IntType)
  private def eq(c: String, v: String) = EqualOp(Seq(col(c), lit(v)))

  private def pruneOp(op: BaseOp): Option[BaseOp] = UnsupportedOpPruner.prune(op)._1
  private def pruneHadUnsupported(op: BaseOp): Boolean = UnsupportedOpPruner.prune(op)._2

  test("leaf op with no UnsupportedOp is unchanged") {
    val op = eq("c1", "1")
    assert(pruneOp(op) == Some(op))
    assert(pruneHadUnsupported(op) == false)
  }

  test("UnsupportedOp alone is pruned to None") {
    assert(pruneOp(UnsupportedOp()) == None)
    assert(pruneHadUnsupported(UnsupportedOp()) == true)
  }

  test("AND: unsupported child is dropped, supported sibling kept") {
    val op = AndOp(Seq(eq("c1", "1"), UnsupportedOp()))
    assert(pruneOp(op) == Some(eq("c1", "1")))
    assert(pruneHadUnsupported(op) == true)
  }

  test("AND: all children unsupported prunes to None") {
    val op = AndOp(Seq(UnsupportedOp(), UnsupportedOp()))
    assert(pruneOp(op) == None)
    assert(pruneHadUnsupported(op) == true)
  }

  test("AND: multiple supported children with one unsupported keeps the rest as AND") {
    val op = AndOp(Seq(eq("c1", "1"), eq("c2", "2"), UnsupportedOp()))
    assert(pruneOp(op) == Some(AndOp(Seq(eq("c1", "1"), eq("c2", "2")))))
    assert(pruneHadUnsupported(op) == true)
  }

  test("OR: any unsupported child prunes the entire OR") {
    val op = OrOp(Seq(eq("c1", "1"), UnsupportedOp()))
    assert(pruneOp(op) == None)
    assert(pruneHadUnsupported(op) == true)
  }

  test("OR: no unsupported children is unchanged") {
    val op = OrOp(Seq(eq("c1", "1"), eq("c2", "2")))
    assert(pruneOp(op) == Some(op))
    assert(pruneHadUnsupported(op) == false)
  }

  test("NOT: unsupported child prunes the entire NOT") {
    val op = NotOp(Seq(UnsupportedOp()))
    assert(pruneOp(op) == None)
    assert(pruneHadUnsupported(op) == true)
  }

  test("NOT: supported child is unchanged") {
    val op = NotOp(Seq(eq("c1", "1")))
    assert(pruneOp(op) == Some(op))
    assert(pruneHadUnsupported(op) == false)
  }

  test("AND containing OR with unsupported: OR branch is dropped, AND sibling kept") {
    // AND(c1=1, OR(c2=2, unsupported)) -> AND drops the OR branch -> c1=1
    val op = AndOp(Seq(eq("c1", "1"), OrOp(Seq(eq("c2", "2"), UnsupportedOp()))))
    assert(pruneOp(op) == Some(eq("c1", "1")))
    assert(pruneHadUnsupported(op) == true)
  }

  test("AND containing NOT with unsupported: NOT branch is dropped, AND sibling kept") {
    // AND(c1=1, NOT(unsupported)) -> AND drops the NOT branch -> c1=1
    val op = AndOp(Seq(eq("c1", "1"), NotOp(Seq(UnsupportedOp()))))
    assert(pruneOp(op) == Some(eq("c1", "1")))
    assert(pruneHadUnsupported(op) == true)
  }

  test("fully supported AND is unchanged") {
    val op = AndOp(Seq(eq("c1", "1"), eq("c2", "2")))
    assert(pruneOp(op) == Some(op))
    assert(pruneHadUnsupported(op) == false)
  }
}
