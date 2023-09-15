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

import io.delta.sharing.client.util.JsonUtils

class JsonPredicateSuite extends SparkFunSuite {
  /**
   * A wrapper around op evaluation.
   *
   * @param expectError specifies if we expect an error.
   * An error implies that the file represented by ctx will not get filtered.
   */
  def evalExpectBoolean(op: NonLeafOp, ctx: EvalContext, expectError: Boolean = false): Boolean = {
    try {
      val res = op.evalExpectBoolean(ctx)
      if (expectError) {
        throw new IllegalArgumentException("Expected error for " + op)
      }
      res
    } catch {
      case e: IllegalArgumentException =>
        if (expectError) {
          // This implies we include the file represented by the context.
          true
        } else {
          throw e
        }
    }
  }

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
    val op_json = JsonUtils.toJson[BaseOp](op)
    val expected_json = """{"op":"literal","value":"2021-04-29","valueType":"date"}"""
    assert(op_json == expected_json)
    val op_from_json = JsonUtils.fromJson[BaseOp](op_json)
    test_op(op_from_json)

    // Test validation failures.
    assert(intercept[IllegalArgumentException] {
      LiteralOp(value = "2021-04-29", valueType = "junk").validate()
    }.getMessage.contains("Unsupported type"))

    assert(intercept[IllegalArgumentException] {
      LiteralOp(value = null, valueType = "int").validate()
    }.getMessage.contains("Value must be specified"))
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
    val op_json = JsonUtils.toJson[BaseOp](op)
    val expected_json = """{"op":"column","name":"hireDate","valueType":"date"}"""
    assert(op_json == expected_json)
    val op_from_json = JsonUtils.fromJson[BaseOp](op_json)
    test_op(op_from_json)

    // Test validation failures.
    assert(intercept[IllegalArgumentException] {
      ColumnOp(name = "hireDate", valueType = "junk").validate()
    }.getMessage.contains("Unsupported type"))
    assert(intercept[IllegalArgumentException] {
      ColumnOp(name = null, valueType = "int").validate()
    }.getMessage.contains("Name must be specified"))
  }

  test("EqualOp test") {
    def test_op(op: NonLeafOp): Unit = {
      op.validate()
      val ctx1 = EvalContext(Map("hireDate" -> "2021-04-28"))
      assert(evalExpectBoolean(op, ctx1) == false)
      val ctx2 = EvalContext(Map("hireDate" -> "2021-04-29"))
      assert(evalExpectBoolean(op, ctx2) == true)
      assert(evalExpectBoolean(op, EvalContext(Map.empty), true) == true)
    }

    val op = EqualOp(Seq(
      ColumnOp(name = "hireDate", valueType = "date"),
      LiteralOp(value = "2021-04-29", valueType = "date")
    ))
    test_op(op)
    // Check that we can convert to json and back.
    val op_json = JsonUtils.toJson[BaseOp](op)
    val expected_json =
      """{"op":"equal",
         |"children":[
         |  {"op":"column","name":"hireDate","valueType":"date"},
         |  {"op":"literal","value":"2021-04-29","valueType":"date"}]
         |}""".stripMargin.replaceAll("\n", "").replaceAll(" ", "")
    assert(op_json == expected_json)
    val op_from_json = JsonUtils.fromJson[NonLeafOp](op_json)
    test_op(op_from_json)

    // Test validation failures.
    assert(intercept[IllegalArgumentException] {
      EqualOp(Seq(
        ColumnOp(name = "hireDate", valueType = "date"),
        LiteralOp(value = "2021-04-29", valueType = "date"),
        LiteralOp(value = "2021-04-29", valueType = "date")
      )).validate()
    }.getMessage.contains("expected 2 but found 3"))
  }

  test("LessThanOp test") {
    def test_op(op: NonLeafOp): Unit = {
      op.validate()
      assert(evalExpectBoolean(op, EvalContext(Map("hireDate" -> "2021-04-28"))) == true)
      assert(evalExpectBoolean(op, EvalContext(Map("hireDate" -> "2021-04-29"))) == false)
      assert(evalExpectBoolean(op, EvalContext(Map("hireDate" -> "2021-04-30"))) == false)
    }

    val op = LessThanOp(Seq(
      ColumnOp(name = "hireDate", valueType = "date"),
      LiteralOp(value = "2021-04-29", valueType = "date")
    ))
    test_op(op)

    // Check that we can convert to json and back.
    val op_json = JsonUtils.toJson[BaseOp](op)
    val expected_json =
      """{"op":"lessThan",
         |"children":[
         |  {"op":"column","name":"hireDate","valueType":"date"},
         |  {"op":"literal","value":"2021-04-29","valueType":"date"}]
         |}""".stripMargin.replaceAll("\n", "").replaceAll(" ", "")
    assert(op_json == expected_json)
    val op_from_json = JsonUtils.fromJson[NonLeafOp](op_json)
    test_op(op_from_json)

    // Test validation failures.
    assert(intercept[IllegalArgumentException] {
      LessThanOp(Seq(
        ColumnOp(name = "hireDate", valueType = "date")
      )).validate()
    }.getMessage.contains("expected 2 but found 1"))
  }

  test("AndOp test") {
    def test_op(op: NonLeafOp): Unit = {
      op.validate()
      assert(evalExpectBoolean(
        op,
        EvalContext(Map("hireDate" -> "2021-04-29", "id" -> "21"))) == true
      )
      assert(evalExpectBoolean(op, EvalContext(Map("hireDate" -> "2021-04-29")), true) == true)
      assert(evalExpectBoolean(op, EvalContext(Map("id" -> "21")), true) == true)
      assert(evalExpectBoolean(
        op,
        EvalContext(Map("hireDate" -> "2021-04-28", "id" -> "21"))) == false
      )
      assert(evalExpectBoolean(
        op,
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
    val op_json = JsonUtils.toJson[BaseOp](op)
    val expected_json =
      """{"op":"and","children":[
         |  {"op":"equal","children":[
         |    {"op":"column","name":"hireDate","valueType":"date"},
         |    {"op":"literal","value":"2021-04-29","valueType":"date"}]},
         |  {"op":"lessThan","children":[
         |    {"op":"column","name":"id","valueType":"int"},
         |    {"op":"literal","value":"25","valueType":"int"}]}
         |]}""".stripMargin.replaceAll("\n", "").replaceAll(" ", "")
    assert(op_json == expected_json)
    val op_from_json = JsonUtils.fromJson[NonLeafOp](op_json)
    test_op(op_from_json)

    // Test validation failures.
    assert(intercept[IllegalArgumentException] {
      AndOp(Seq(
        EqualOp(Seq(
          ColumnOp(name = "hireDate", valueType = "date"),
          LiteralOp(value = "2021-04-29", valueType = "date")
        ))
      )).validate()
    }.getMessage.contains("expected at least 2 but found 1"))

    // Hierarchical validation.
    assert(intercept[IllegalArgumentException] {
      AndOp(Seq(
        EqualOp(Seq(
          ColumnOp(name = "hireDate", valueType = "date")
        )),
        LessThanOp(Seq(
          ColumnOp(name = "id", valueType = "int"),
          LiteralOp(value = "25", valueType = "int")
        ))
      )).validate()
    }.getMessage.contains("expected 2 but found 1"))
  }

  test("OrOp test") {
    def test_op(op: NonLeafOp): Unit = {
      op.validate()
      assert(evalExpectBoolean(
        op,
        EvalContext(Map("hireDate" -> "2021-04-29", "id" -> "21"))) == true
      )
      assert(evalExpectBoolean(op, EvalContext(Map("hireDate" -> "2021-04-29")), true) == true)
      assert(evalExpectBoolean(op, EvalContext(Map("id" -> "21")), true) == true)
      assert(evalExpectBoolean(
        op,
        EvalContext(Map("hireDate" -> "2021-04-28", "id" -> "21"))) == true
      )
      assert(evalExpectBoolean(
        op,
        EvalContext(Map("hireDate" -> "2021-04-29", "id" -> "25"))) == true
      )
      assert(evalExpectBoolean(op, EvalContext(Map("hireDate" -> "2021-04-28")), true) == true)
      assert(evalExpectBoolean(
        op,
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
    val op_json = JsonUtils.toJson[BaseOp](op)
    val expected_json =
      """{"op":"or","children":[
         |  {"op":"equal","children":[
         |    {"op":"column","name":"hireDate","valueType":"date"},
         |    {"op":"literal","value":"2021-04-29","valueType":"date"}]},
         |  {"op":"lessThan","children":[
         |    {"op":"column","name":"id","valueType":"int"},
         |    {"op":"literal","value":"25","valueType":"int"}]}
         |]}""".stripMargin.replaceAll("\n", "").replaceAll(" ", "")
    assert(op_json == expected_json)
    val op_from_json = JsonUtils.fromJson[NonLeafOp](op_json)
    test_op(op_from_json)

    // Test validation failures.
    assert(intercept[IllegalArgumentException] {
      OrOp(Seq.empty).validate()
    }.getMessage.contains("expected at least 2 but found 0"))
  }

  test("NotOp test") {
    def test_op(op: NonLeafOp): Unit = {
      op.validate()
      assert(evalExpectBoolean(op, EvalContext(Map("hireDate" -> "2021-04-28"))) == true)
      assert(evalExpectBoolean(op, EvalContext(Map("hireDate" -> "2021-04-29"))) == false)
    }

    val op = NotOp(Seq(
      EqualOp(Seq(
        ColumnOp(name = "hireDate", valueType = "date"),
        LiteralOp(value = "2021-04-29", valueType = "date")
      ))
    ))
    test_op(op)

    // Check that we can convert to json and back.
    val op_json = JsonUtils.toJson[BaseOp](op)
    val expected_json =
      """{"op":"not","children":[
         |  {"op":"equal","children":[
         |    {"op":"column","name":"hireDate","valueType":"date"},
         |    {"op":"literal","value":"2021-04-29","valueType":"date"}]}
         |]}""".stripMargin.replaceAll("\n", "").replaceAll(" ", "")
    assert(op_json == expected_json)
    val op_from_json = JsonUtils.fromJson[NonLeafOp](op_json)
    test_op(op_from_json)

    // Test validation failures.
    intercept[IllegalArgumentException] {
      NotOp(Seq.empty).validate()
    }
  }

  test("Null test") {
    def test_op(op: NonLeafOp): Unit = {
      op.validate()
      assert(evalExpectBoolean(op, EvalContext(Map.empty)) == true)
      assert(evalExpectBoolean(op, EvalContext(Map("hireDate" -> "2021-04-28"))) == false)
    }

    val op = IsNullOp(Seq(ColumnOp(name = "hireDate", valueType = "date")))
    test_op(op)

    // Check that we can convert to json and back.
    val op_json = JsonUtils.toJson[BaseOp](op)
    val expected_json =
      """{"op":"isNull","children":[
         |  {"op":"column","name":"hireDate","valueType":"date"}
         |]}""".stripMargin.replaceAll("\n", "").replaceAll(" ", "")
    assert(op_json == expected_json)
    val op_from_json = JsonUtils.fromJson[NonLeafOp](op_json)
    test_op(op_from_json)

    // Test validation failures.
    intercept[IllegalArgumentException] {
      IsNullOp(Seq.empty).validate()
    }
  }

  test("ColumnOp boolean test") {
    def test_op(op: BaseOp): Unit = {
      op.validate()
      assert(op.evalExpectBoolean(EvalContext(Map("isActive" -> "true"))) == true)
      assert(op.evalExpectBoolean(EvalContext(Map("isActive" -> "false"))) == false)
    }
    val op = ColumnOp(name = "isActive", valueType = "bool")
    test_op(op)
  }

  test("Float test") {
    val op = ColumnOp(name = "cost", valueType = "float")
    op.validate(true)

    // Check that we can convert to json and back.
    val op_json = JsonUtils.toJson[BaseOp](op)
    val expected_json = """{"op":"column","name":"cost","valueType":"float"}"""
    assert(op_json == expected_json)
    val op_from_json = JsonUtils.fromJson[BaseOp](op_json)
    op_from_json.validate(true)
  }

  test("Double test") {
    val op = LiteralOp(value = "2.0005", valueType = "double")
    op.validate(true)

    // Check that we can convert to json and back.
    val op_json = JsonUtils.toJson[BaseOp](op)
    val expected_json = """{"op":"literal","value":"2.0005","valueType":"double"}"""
    assert(op_json == expected_json)
    val op_from_json = JsonUtils.fromJson[BaseOp](op_json)
    op_from_json.validate(true)
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
      ))
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
      evalExpectBoolean(op, EvalContext(Map("date" -> dates(i % dates.size))))
    }
    val t = System.currentTimeMillis() - start

    // scalastyle:off println
    println("Stress test took " + t + " millis")
  }
}
