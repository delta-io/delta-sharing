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

// scalastyle:off println

package io.delta.sharing.spark.filters

import org.apache.spark.SparkFunSuite

import io.delta.sharing.spark.util.JsonUtils

class JsonPredicateSuite extends SparkFunSuite {

  test("LiteralOp test") {
    def test_op(op: BaseOp): Unit = {
      op.validate()
      val ctx = EvalContext(Map.empty)
      val (value: String, valueType: String) = op.eval(ctx)
      assert(value == "2021-04-29")
      assert(valueType == "date")
    }
    val op = LiteralOp(value = "2021-04-29", valueType = "date")
    test_op(op)

    // Check that we can convert to json and back.
    val op_from_json = JsonUtils.fromJson[BaseOp](JsonUtils.toJson[BaseOp](op))
    test_op(op_from_json)

    // Test validation failures.
    intercept[IllegalArgumentException] {
      LiteralOp(value = "2021-04-29", valueType = "junk").validate()
    }
    intercept[IllegalArgumentException] {
      LiteralOp(value = null, valueType = "int").validate()
    }
  }

  test("ColumnOp test") {
    def test_op(op: BaseOp): Unit = {
      op.validate()
      val ctx = EvalContext(Map("hireDate" -> "2021-04-28"))
      val (value: String, valueType: String) = op.eval(ctx)
      assert(value == "2021-04-28")
      assert(valueType == "date")
    }
    val op = ColumnOp(name = "hireDate", valueType = "date")
    test_op(op)

    // Check that we can convert to json and back.
    val op_from_json = JsonUtils.fromJson[BaseOp](JsonUtils.toJson[BaseOp](op))
    test_op(op_from_json)

    // Test validation failures.
    intercept[IllegalArgumentException] {
      ColumnOp(name = "hireDate", valueType = "junk").validate()
    }
    intercept[IllegalArgumentException] {
      ColumnOp(name = null, valueType = "int").validate()
    }
  }

  test("EqualOp test") {
    def test_op(op: NonLeafOp): Unit = {
      op.validate()
      val ctx1 = EvalContext(Map("hireDate" -> "2021-04-28"))
      assert(op.evalExpectBoolean(ctx1) == false)
      val ctx2 = EvalContext(Map("hireDate" -> "2021-04-29"))
      assert(op.evalExpectBoolean(ctx2) == true)
    }

    val op = EqualOp(Seq(
      ColumnOp(name = "hireDate", valueType = "date"),
      LiteralOp(value = "2021-04-29", valueType = "date")
    ))
    test_op(op)
    
    // Check that we can convert to json and back.
    val op_from_json = JsonUtils.fromJson[NonLeafOp](JsonUtils.toJson[NonLeafOp](op))
    test_op(op_from_json)

    // Test validation failures.
    intercept[IllegalArgumentException] {
      EqualOp(Seq(
        ColumnOp(name = "hireDate", valueType = "date"),
        LiteralOp(value = "2021-04-29", valueType = "date"),
        LiteralOp(value = "2021-04-29", valueType = "date")
      )).validate()
    }
  }

  test("LessThanOp test") {
    def test_op(op: NonLeafOp): Unit = {
      op.validate()
      assert(op.evalExpectBoolean(EvalContext(Map("hireDate" -> "2021-04-28"))) == true)
      assert(op.evalExpectBoolean(EvalContext(Map("hireDate" -> "2021-04-29"))) == false)
      assert(op.evalExpectBoolean(EvalContext(Map("hireDate" -> "2021-04-30"))) == false)
    }

    val op = LessThanOp(Seq(
      ColumnOp(name = "hireDate", valueType = "date"),
      LiteralOp(value = "2021-04-29", valueType = "date")
    ))
    test_op(op)
    
    // Check that we can convert to json and back.
    val op_from_json = JsonUtils.fromJson[NonLeafOp](JsonUtils.toJson[NonLeafOp](op))
    test_op(op_from_json)

    // Test validation failures.
    intercept[IllegalArgumentException] {
      LessThanOp(Seq(
        ColumnOp(name = "hireDate", valueType = "date")
      )).validate()
    }
  }

  test("AndOp test") {
    def test_op(op: NonLeafOp): Unit = {
      op.validate()
      assert(op.evalExpectBoolean(
        EvalContext(Map("hireDate" -> "2021-04-29", "id" -> "21"))) == true
      )
      assert(op.evalExpectBoolean(EvalContext(Map("hireDate" -> "2021-04-29"))) == false)
      assert(op.evalExpectBoolean(EvalContext(Map("id" -> "21"))) == false)
      assert(op.evalExpectBoolean(
        EvalContext(Map("hireDate" -> "2021-04-28", "id" -> "21"))) == false
      )
      assert(op.evalExpectBoolean(
        EvalContext(Map("hireDate" -> "2021-04-29", "id" -> "25"))) == false
      )
    }

    val op = AndOp(Seq(
      EqualOp(Seq(
        ColumnOp(name = "hireDate", valueType = "date"),
        LiteralOp(value = "2021-04-29", valueType = "date")
      )),
      LessThanOp(Seq(
        ColumnOp(name = "id", valueType = "int"),
        LiteralOp(value = "25", valueType = "int")
      ))
    ))
    test_op(op)

    // Check that we can convert to json and back.
    val op_from_json = JsonUtils.fromJson[NonLeafOp](JsonUtils.toJson[NonLeafOp](op))
    test_op(op_from_json)

    // Test validation failures.
    intercept[IllegalArgumentException] {
      AndOp(Seq(
        EqualOp(Seq(
          ColumnOp(name = "hireDate", valueType = "date"),
          LiteralOp(value = "2021-04-29", valueType = "date")
        ))
      )).validate()
    }
  }

  test("OrOp test") {
    def test_op(op: NonLeafOp): Unit = {
      op.validate()
      assert(op.evalExpectBoolean(
        EvalContext(Map("hireDate" -> "2021-04-29", "id" -> "21"))) == true
      )
      assert(op.evalExpectBoolean(EvalContext(Map("hireDate" -> "2021-04-29"))) == true)
      assert(op.evalExpectBoolean(EvalContext(Map("id" -> "21"))) == true)
      assert(op.evalExpectBoolean(
        EvalContext(Map("hireDate" -> "2021-04-28", "id" -> "21"))) == true
      )
      assert(op.evalExpectBoolean(
        EvalContext(Map("hireDate" -> "2021-04-29", "id" -> "25"))) == true
      )
      assert(op.evalExpectBoolean(EvalContext(Map("hireDate" -> "2021-04-28"))) == false)
      assert(op.evalExpectBoolean(
        EvalContext(Map("hireDate" -> "2021-04-28", "id" -> "25"))) == false
      )
    }

    val op = OrOp(Seq(
      EqualOp(Seq(
        ColumnOp(name = "hireDate", valueType = "date"),
        LiteralOp(value = "2021-04-29", valueType = "date")
      )),
      LessThanOp(Seq(
        ColumnOp(name = "id", valueType = "int"),
        LiteralOp(value = "25", valueType = "int")
      ))
    ))
    test_op(op)

    // Check that we can convert to json and back.
    val op_from_json = JsonUtils.fromJson[NonLeafOp](JsonUtils.toJson[NonLeafOp](op))
    test_op(op_from_json)

    // Test validation failures.
    intercept[IllegalArgumentException] {
      OrOp(Seq.empty).validate()
    }
  }

  test("NotOp test") {
    def test_op(op: NonLeafOp): Unit = {
      op.validate()
      assert(op.evalExpectBoolean(EvalContext(Map("hireDate" -> "2021-04-28"))) == true)
      assert(op.evalExpectBoolean(EvalContext(Map("hireDate" -> "2021-04-29"))) == false)
    }

    val op = NotOp(Seq(
      EqualOp(Seq(
        ColumnOp(name = "hireDate", valueType = "date"),
        LiteralOp(value = "2021-04-29", valueType = "date")
      ))
    ))
    test_op(op)
    
    // Check that we can convert to json and back.
    val op_from_json = JsonUtils.fromJson[NonLeafOp](JsonUtils.toJson[NonLeafOp](op))
    test_op(op_from_json)

    // Test validation failures.
    intercept[IllegalArgumentException] {
      NotOp(Seq.empty).validate()
    }
  }

  test("Null test") {
    def test_op(op: NonLeafOp): Unit = {
      op.validate()
      assert(op.evalExpectBoolean(EvalContext(Map.empty)) == true)
      assert(op.evalExpectBoolean(EvalContext(Map("hireDate" -> "2021-04-28"))) == false)
    }

    val op = IsNullOp(Seq(ColumnOp(name = "hireDate", valueType = "date")))
    test_op(op)

    val op_from_json = JsonUtils.fromJson[NonLeafOp](JsonUtils.toJson[NonLeafOp](op))
    test_op(op_from_json)

    // Test validation failures.
    intercept[IllegalArgumentException] {
      IsNullOp(Seq.empty).validate()
    }
  }

  test("stress test") {
    val op = AndOp(Seq(
      GreaterThanOp(Seq(
        ColumnOp(name = "date", valueType = "date"),
        LiteralOp(value = "2021-04-29", valueType = "date")
      )),
      LessThanOp(Seq(
        ColumnOp(name = "date", valueType = "date"),
        LiteralOp(value = "2021-05-29", valueType = "date")
      )),
    ))
    val dates = Seq(
      "2021-04-28",
      "2021-04-29",
      "2021-04-30",
      "2021-05-01",
      "2021-05-03",
      "2021-05-04",
      "2021-05-30",
      "2021-05-31",
      "2021-06-01"
    )

    val numEval = 1000000

    val start = System.currentTimeMillis()
    for (i <- 1 to numEval) {
      op.evalExpectBoolean(EvalContext(Map("date" -> dates(i % dates.size))))
    }
    val t = System.currentTimeMillis() - start
    println("Stress test took " + t + " millis")
  }
}
