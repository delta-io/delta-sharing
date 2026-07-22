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
import org.apache.spark.sql.catalyst.expressions.{
  And => SqlAnd,
  Attribute => SqlAttribute,
  AttributeReference => SqlAttributeReference,
  Cast => SqlCast,
  EqualNullSafe => SqlEqualNullSafe,
  EqualTo => SqlEqualTo,
  Expression => SqlExpression,
  GetStructField,
  GreaterThan => SqlGreaterThan,
  GreaterThanOrEqual => SqlGreaterThanOrEqual,
  In => SqlIn,
  IsNotNull => SqlIsNotNull,
  IsNull => SqlIsNull,
  LessThan => SqlLessThan,
  LessThanOrEqual => SqlLessThanOrEqual,
  Literal => SqlLiteral,
  Not => SqlNot,
  Or => SqlOr
}
import org.apache.spark.sql.types.{
  BooleanType => SqlBooleanType,
  DataType => SqlDataType,
  DateType => SqlDateType,
  DoubleType => SqlDoubleType,
  FloatType => SqlFloatType,
  IntegerType => SqlIntegerType,
  LongType => SqlLongType,
  StringType => SqlStringType,
  StructField,
  StructType,
  TimestampType => SqlTimestampType
}

class OpConverterSuite extends SparkFunSuite {

  test("equal test") {
    val sqlColumn = SqlAttributeReference("userId", SqlIntegerType)()
    val sqlLiteral = SqlLiteral(23, SqlIntegerType)
    val sqlEq = SqlEqualTo(sqlColumn, sqlLiteral)
    val op = OpConverter.convert(Seq(sqlEq)).get.asInstanceOf[EqualOp]

    op.validate()
    assert(op.children(0).asInstanceOf[ColumnOp].valueType == OpDataTypes.IntType)
    assert(op.children(1).asInstanceOf[LiteralOp].valueType == OpDataTypes.IntType)
    assert(op.evalExpectBoolean(EvalContext(Map("userId" -> "23"))) == true)
    assert(op.evalExpectBoolean(EvalContext(Map("userId" -> "24"))) == false)
  }

  test("cast test") {
    val sqlColumn = SqlAttributeReference("hireDate", SqlStringType)()
    val sqlLiteral = SqlLiteral("2020-01-02")

    // Treat date as strings.
    val sqlStringEq = SqlEqualTo(sqlColumn, sqlLiteral)
    val op = OpConverter.convert(Seq(sqlStringEq)).get.asInstanceOf[EqualOp]
    op.validate()
    assert(op.children(0).asInstanceOf[ColumnOp].valueType == OpDataTypes.StringType)
    assert(op.children(1).asInstanceOf[LiteralOp].valueType == OpDataTypes.StringType)
    assert(op.evalExpectBoolean(EvalContext(Map("hireDate" -> "2020-01-02"))) == true)
    assert(op.evalExpectBoolean(EvalContext(Map("hireDate" -> "2020-01-03"))) == false)

    // Treat date as date.
    val sqlDateEq = SqlEqualTo(SqlCast(sqlColumn, SqlDateType), SqlCast(sqlLiteral, SqlDateType))
    val op2 = OpConverter.convert(Seq(sqlDateEq)).get.asInstanceOf[EqualOp]
    op2.validate()
    assert(op2.children(0).asInstanceOf[ColumnOp].valueType == OpDataTypes.DateType)
    assert(op2.children(1).asInstanceOf[LiteralOp].valueType == OpDataTypes.DateType)
    assert(op2.evalExpectBoolean(EvalContext(Map("hireDate" -> "2020-01-02"))) == true)
    assert(op2.evalExpectBoolean(EvalContext(Map("hireDate" -> "2020-01-03"))) == false)
  }

  test("multi level cast test") {
    def test_op(op: BaseOp): Unit = {
      op.validate()
      assert(op.evalExpectBoolean(EvalContext(Map("id" -> "45"))) == true)
      assert(op.evalExpectBoolean(EvalContext(Map("id" -> "50"))) == false)
    }

    val sqlColumn = SqlAttributeReference("id", SqlStringType)()
    val sqlLiteral = SqlLiteral("45")

    val colCastAsInt = SqlCast(sqlColumn, SqlIntegerType)
    val litCastAsInt = SqlCast(sqlLiteral, SqlIntegerType)
    val sqlOp1 = SqlEqualTo(colCastAsInt, litCastAsInt)
    val op1 = OpConverter.convert(Seq(sqlOp1)).get
    test_op(op1)

    val sqlOp2 = SqlEqualTo(
      SqlCast(colCastAsInt, SqlLongType),
      SqlCast(litCastAsInt, SqlLongType)
    )
    val op2 = OpConverter.convert(Seq(sqlOp2)).get
    test_op(op2)
  }

  test("lessThan test") {
    val sqlColumn = SqlAttributeReference("cost", SqlLongType)()
    val sqlLiteral = SqlLiteral(24L, SqlLongType)
    val sqlLT = SqlLessThan(sqlColumn, sqlLiteral)

    val op = OpConverter.convert(Seq(sqlLT)).get
    op.validate()
    assert(op.evalExpectBoolean(EvalContext(Map("cost" -> "23"))) == true)
    assert(op.evalExpectBoolean(EvalContext(Map("cost" -> "24"))) == false)
    assert(op.evalExpectBoolean(EvalContext(Map("cost" -> "25"))) == false)
  }

  test("lessThanOrEqual test") {
    val sqlColumn = SqlAttributeReference("cost", SqlLongType)()
    val sqlLiteral = SqlLiteral(24L, SqlLongType)
    val sqlLessThanOrEq = SqlLessThanOrEqual(sqlColumn, sqlLiteral)

    val op = OpConverter.convert(Seq(sqlLessThanOrEq)).get
    op.validate()
    assert(op.evalExpectBoolean(EvalContext(Map("cost" -> "23"))) == true)
    assert(op.evalExpectBoolean(EvalContext(Map("cost" -> "24"))) == true)
    assert(op.evalExpectBoolean(EvalContext(Map("cost" -> "25"))) == false)
  }

  test("greaterThan test") {
    val sqlColumn = SqlAttributeReference("hireDate", SqlStringType)()
    val sqlLiteral = SqlLiteral("2020-01-02")
    val sqlGT = SqlGreaterThan(
      SqlCast(sqlColumn, SqlDateType),
      SqlCast(sqlLiteral, SqlDateType)
    )

    val op = OpConverter.convert(Seq(sqlGT)).get
    op.validate()
    assert(op.evalExpectBoolean(EvalContext(Map("hireDate" -> "2020-01-01"))) == false)
    assert(op.evalExpectBoolean(EvalContext(Map("hireDate" -> "2020-01-02"))) == false)
    assert(op.evalExpectBoolean(EvalContext(Map("hireDate" -> "2020-01-03"))) == true)
  }

  test("greaterThanOrEqual test") {
    val sqlColumn = SqlAttributeReference("hireDate", SqlStringType)()
    val sqlLiteral = SqlLiteral("2020-01-02")
    val sqlGTE = SqlGreaterThanOrEqual(
      SqlCast(sqlColumn, SqlDateType),
      SqlCast(sqlLiteral, SqlDateType)
    )

    val op = OpConverter.convert(Seq(sqlGTE)).get
    op.validate()
    assert(op.evalExpectBoolean(EvalContext(Map("hireDate" -> "2020-01-01"))) == false)
    assert(op.evalExpectBoolean(EvalContext(Map("hireDate" -> "2020-01-02"))) == true)
    assert(op.evalExpectBoolean(EvalContext(Map("hireDate" -> "2020-01-03"))) == true)
  }

  test("null test") {
    val sqlColumn = SqlAttributeReference("hireDate", SqlStringType)()
    val sqlIsNull = SqlIsNull(sqlColumn)

    val op = OpConverter.convert(Seq(sqlIsNull)).get
    op.validate()
    assert(op.evalExpectBoolean(EvalContext(Map.empty)) == true)
    assert(op.evalExpectBoolean(EvalContext(Map("hireDate" -> "2020-01-02"))) == false)
  }

  test("not null test") {
    val sqlColumn = SqlAttributeReference("hireDate", SqlStringType)()
    val sqlIsNotNull = SqlIsNotNull(sqlColumn)

    val op = OpConverter.convert(Seq(sqlIsNotNull)).get
    op.validate()
    assert(op.evalExpectBoolean(EvalContext(Map.empty)) == false)
    assert(op.evalExpectBoolean(EvalContext(Map("hireDate" -> "2020-01-02"))) == true)
  }

  test("and test") {
    def test_op(op: BaseOp): Unit = {
      op.validate()
      assert(op.evalExpectBoolean(
        EvalContext(Map("hireDate" -> "2020-01-02", "cost" -> "24"))) == true
      )
      assert(op.evalExpectBoolean(
        EvalContext(Map("hireDate" -> "2020-01-03", "cost" -> "23"))) == true
      )
      assert(op.evalExpectBoolean(
        EvalContext(Map("hireDate" -> "2020-01-01", "cost" -> "23"))) == false
      )
      assert(op.evalExpectBoolean(
        EvalContext(Map("hireDate" -> "2020-01-01", "cost" -> "26"))) == false
      )
    }

    val sqlCol1 = SqlAttributeReference("cost", SqlLongType)()
    val sqlLit1 = SqlLiteral(24L, SqlLongType)
    val sqlOp1 = SqlLessThanOrEqual(sqlCol1, sqlLit1)

    val sqlCol2 = SqlAttributeReference("hireDate", SqlStringType)()
    val sqlLit2 = SqlLiteral("2020-01-02")
    val sqlOp2 = SqlGreaterThanOrEqual(
      SqlCast(sqlCol2, SqlDateType),
      SqlCast(sqlLit2, SqlDateType)
    )

    val sqlAnd = SqlAnd(sqlOp1, sqlOp2)
    val op = OpConverter.convert(Seq(sqlAnd)).get
    test_op(op)

    // Test that a sequence leads to an implicit And.
    val op2 = OpConverter.convert(Seq(sqlOp1, sqlOp2)).get
    test_op(op2)
  }

  test("or test") {
    def test_op(op: BaseOp): Unit = {
      op.validate()
      assert(op.evalExpectBoolean(
        EvalContext(Map("hireDate" -> "2020-01-02", "cost" -> "24"))) == true
      )
      assert(op.evalExpectBoolean(
        EvalContext(Map("hireDate" -> "2020-01-03", "cost" -> "23"))) == true
      )
      assert(op.evalExpectBoolean(
        EvalContext(Map("hireDate" -> "2020-01-01", "cost" -> "23"))) == true
      )
      assert(op.evalExpectBoolean(
        EvalContext(Map("hireDate" -> "2020-01-01", "cost" -> "26"))) == false
      )
    }

    val sqlCol1 = SqlAttributeReference("cost", SqlLongType)()
    val sqlLit1 = SqlLiteral(24L, SqlLongType)
    val sqlOp1 = SqlLessThanOrEqual(sqlCol1, sqlLit1)

    val sqlCol2 = SqlAttributeReference("hireDate", SqlStringType)()
    val sqlLit2 = SqlLiteral("2020-01-02")
    val sqlOp2 = SqlGreaterThanOrEqual(
      SqlCast(sqlCol2, SqlDateType),
      SqlCast(sqlLit2, SqlDateType)
    )

    val sqlOr = SqlOr(sqlOp1, sqlOp2)
    val op = OpConverter.convert(Seq(sqlOr)).get
    test_op(op)
  }

  test("not test") {
    val sqlColumn = SqlAttributeReference("userId", SqlIntegerType)()
    val sqlLiteral = SqlLiteral(23, SqlIntegerType)
    val sqlNot = SqlNot(SqlEqualTo(sqlColumn, sqlLiteral))

    val op = OpConverter.convert(Seq(sqlNot)).get
    op.validate()
    assert(op.evalExpectBoolean(EvalContext(Map("userId" -> "23"))) == false)
    assert(op.evalExpectBoolean(EvalContext(Map("userId" -> "24"))) == true)
  }

  test("equal null safe test") {
    val sqlColumn = SqlAttributeReference("userId", SqlIntegerType)()
    val sqlLiteral = SqlLiteral(23, SqlIntegerType)
    val sqlOp = SqlEqualNullSafe(sqlColumn, sqlLiteral)

    val op = OpConverter.convert(Seq(sqlOp)).get
    op.validate()
    assert(op.evalExpectBoolean(EvalContext(Map.empty)) == false)
    assert(op.evalExpectBoolean(EvalContext(Map("userId" -> "23"))) == true)
    assert(op.evalExpectBoolean(EvalContext(Map("userId" -> "24"))) == false)
  }

  test("float test") {
    val sqlColumn = SqlAttributeReference("cost", SqlFloatType)()
    val sqlLiteral = SqlLiteral("100.5")
    val sqlGTE = SqlGreaterThanOrEqual(sqlColumn, sqlLiteral)

    val op = OpConverter.convert(Seq(sqlGTE)).get
    op.validate(true)
  }

  test("Double test") {
    val sqlColumn = SqlAttributeReference("cost", SqlDoubleType)()
    val sqlLiteral = SqlLiteral("10.5")
    val sqlEq = SqlEqualTo(sqlColumn, sqlLiteral)

    val op = OpConverter.convert(Seq(sqlEq)).get
    op.validate(true)
  }

  test("Timestamp test") {
    val formatter = java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME
    val tsStr = "2023-06-07T04:27:03.234Z"
    val tsMicros = java.time.OffsetDateTime.parse(tsStr, formatter).toInstant.toEpochMilli * 1000L
    assert(tsMicros == 1686112023234000L)
    val sqlColumn = SqlAttributeReference("ts", SqlTimestampType)()
    val sqlLiteral = SqlLiteral(tsMicros, SqlLongType)
    val sqlEq = SqlEqualTo(
      SqlCast(sqlColumn, SqlTimestampType),
      SqlCast(sqlLiteral, SqlTimestampType)
    )

    val op = OpConverter.convert(Seq(sqlEq)).get.asInstanceOf[EqualOp]
    op.validate(true)
    assert(op.children(0).asInstanceOf[ColumnOp].valueType == OpDataTypes.TimestampType)
    assert(op.children(1).asInstanceOf[LiteralOp].valueType == OpDataTypes.TimestampType)
    assert(op.children(1).asInstanceOf[LiteralOp].value == tsStr)
  }

  test("In test") {
    val minOutOfBoundVal = "1"
    val maxOutOfBoundVal = "100"

    // Converts the specified values into a json predicate op.
    def convert(inValues: Seq[Int]): BaseOp = {
      val sqlLiterals = inValues.map(v => SqlLiteral(v, SqlIntegerType))
      val sqlColumn = SqlAttributeReference("userId", SqlIntegerType)()
      val sqlIn = SqlIn(sqlColumn, sqlLiterals)
      val op = OpConverter.convert(Seq(sqlIn)).get
      op.validate()
      op
    }

    def test_op(inValues: Seq[Int]): Unit = {
      val op = convert(inValues)
      inValues.map(v => {
        assert(op.evalExpectBoolean(EvalContext(Map("userId" -> v.toString()))) == true)
      })
      assert(op.evalExpectBoolean(EvalContext(Map("userId" -> minOutOfBoundVal))) == false)
      assert(op.evalExpectBoolean(EvalContext(Map("userId" -> maxOutOfBoundVal))) == false)
    }

    test_op(Seq(23))
    test_op(Seq(23, 24, 25, 26))

    var tooManyVals: Seq[Int] = Seq.empty
    for (i <- 0 to OpConverter.kMaxSqlInOpSizeLimit + 1) {
      // Make sure we stay within bounds for out of bound checks.
      tooManyVals = tooManyVals :+ (i + 5)
    }
    // With partialFilterEnabled=false (default), these throw.
    assert(intercept[IllegalArgumentException] {
      convert(tooManyVals)
    }.getMessage.contains("The In predicate exceeds max limit"))

    assert(intercept[IllegalArgumentException] {
      convert(Seq.empty)
    }.getMessage.contains("The In predicate must have at least one entry"))

    // With partialFilterEnabled=true, they produce UnsupportedOp instead.
    assert(OpConverter.convert(
      Seq(SqlIn(SqlAttributeReference("x", SqlIntegerType)(),
        tooManyVals.map(v => SqlLiteral(v, SqlIntegerType)))),
      partialFilterEnabled = true).get.isInstanceOf[UnsupportedOp])

    assert(OpConverter.convert(
      Seq(SqlIn(SqlAttributeReference("x", SqlIntegerType)(), Seq.empty)),
      partialFilterEnabled = true).get.isInstanceOf[UnsupportedOp])
  }

  test("unsupported leaf in comparison produces UnsupportedOp when partialFilterEnabled") {
    // convertAsLeaf throws; convertOneOrUnsupported catches and returns UnsupportedOp.
    val c2Type = StructType(Seq(StructField("f1", SqlIntegerType)))
    val sqlF1 = GetStructField(SqlAttributeReference("c2", c2Type)(), 0, Some("f1"))
    val sqlEq = SqlEqualTo(sqlF1, SqlLiteral(5, SqlIntegerType))

    val op = OpConverter.convert(Seq(sqlEq), partialFilterEnabled = true).get
    assert(op.isInstanceOf[UnsupportedOp])
  }

  test("unsupported expression throws when partialFilterEnabled is false") {
    val c2Type = StructType(Seq(StructField("f1", SqlIntegerType)))
    val sqlC2 = SqlAttributeReference("c2", c2Type)()
    val sqlF1 = GetStructField(sqlC2, 0, Some("f1"))
    val sqlEq = SqlEqualTo(sqlF1, SqlLiteral(5, SqlIntegerType))

    intercept[IllegalArgumentException] {
      OpConverter.convert(Seq(sqlEq), partialFilterEnabled = false)
    }
  }

  test("AND with unsupported child prunes based on supported child when partialFilterEnabled") {
    val sqlC1 = SqlAttributeReference("c1", SqlIntegerType)()
    val supportedEq = SqlEqualTo(sqlC1, SqlLiteral(1, SqlIntegerType))

    val c2Type = StructType(Seq(StructField("f1", SqlIntegerType)))
    val sqlC2 = SqlAttributeReference("c2", c2Type)()
    val sqlF1 = GetStructField(sqlC2, 0, Some("f1"))
    val unsupportedEq = SqlEqualTo(sqlF1, SqlLiteral(5, SqlIntegerType))

    val sqlAnd = SqlAnd(supportedEq, unsupportedEq)
    val op = OpConverter.convert(Seq(sqlAnd), partialFilterEnabled = true).get.asInstanceOf[AndOp]

    // AND tree is preserved: one EqualOp child and one UnsupportedOp child.
    assert(op.children(0).isInstanceOf[EqualOp])
    assert(op.children(1).isInstanceOf[UnsupportedOp])

    // AND prunes when the supported predicate is false (c1 != 1).
    assert(op.evalExpectBoolean(EvalContext(Map("c1" -> "2"))) == false)
    // AND is conservative when the supported predicate is true (c1 == 1).
    assert(op.evalExpectBoolean(EvalContext(Map("c1" -> "1"))) == true)
  }

  test("OR with unsupported child produces OrOp(EqualOp, UnsupportedOp) when partialFilter") {
    val sqlC1 = SqlAttributeReference("c1", SqlIntegerType)()
    val supportedEq = SqlEqualTo(sqlC1, SqlLiteral(1, SqlIntegerType))

    val c2Type = StructType(Seq(StructField("f1", SqlIntegerType)))
    val sqlF1 = GetStructField(SqlAttributeReference("c2", c2Type)(), 0, Some("f1"))
    val unsupportedEq = SqlEqualTo(sqlF1, SqlLiteral(5, SqlIntegerType))

    val op = OpConverter.convert(
      Seq(SqlOr(supportedEq, unsupportedEq)), partialFilterEnabled = true).get.asInstanceOf[OrOp]
    assert(op.children(0).isInstanceOf[EqualOp])
    assert(op.children(1).isInstanceOf[UnsupportedOp])
  }

  test("NOT with unsupported child produces NotOp(UnsupportedOp) when partialFilterEnabled") {
    val c2Type = StructType(Seq(StructField("f1", SqlIntegerType)))
    val sqlF1 = GetStructField(SqlAttributeReference("c2", c2Type)(), 0, Some("f1"))
    val unsupportedEq = SqlEqualTo(sqlF1, SqlLiteral(5, SqlIntegerType))

    val op = OpConverter.convert(
      Seq(SqlNot(unsupportedEq)), partialFilterEnabled = true).get.asInstanceOf[NotOp]
    assert(op.children(0).isInstanceOf[UnsupportedOp])
  }

  test("IsNull with unsupported leaf produces UnsupportedOp when partialFilterEnabled") {
    val c2Type = StructType(Seq(StructField("f1", SqlIntegerType)))
    val sqlF1 = GetStructField(SqlAttributeReference("c2", c2Type)(), 0, Some("f1"))

    val op = OpConverter.convert(Seq(SqlIsNull(sqlF1)), partialFilterEnabled = true).get
    assert(op.isInstanceOf[UnsupportedOp])
  }

  test("IsNotNull with unsupported leaf produces UnsupportedOp when partialFilterEnabled") {
    val c2Type = StructType(Seq(StructField("f1", SqlIntegerType)))
    val sqlF1 = GetStructField(SqlAttributeReference("c2", c2Type)(), 0, Some("f1"))

    val op = OpConverter.convert(Seq(SqlIsNotNull(sqlF1)), partialFilterEnabled = true).get
    assert(op.isInstanceOf[UnsupportedOp])
  }

  test("EqualNullSafe with unsupported leaf produces UnsupportedOp when partialFilterEnabled") {
    val c2Type = StructType(Seq(StructField("f1", SqlIntegerType)))
    val sqlF1 = GetStructField(SqlAttributeReference("c2", c2Type)(), 0, Some("f1"))

    val op = OpConverter.convert(
      Seq(SqlEqualNullSafe(sqlF1, SqlLiteral(5, SqlIntegerType))),
      partialFilterEnabled = true).get
    assert(op.isInstanceOf[UnsupportedOp])
  }

  test("convert then prune end-to-end: AND(supported, unsupported) -> supported only") {
    // Simulates the full client-side flow: convert with partialFilterEnabled, then prune.
    val sqlC1 = SqlAttributeReference("c1", SqlIntegerType)()
    val supportedEq = SqlEqualTo(sqlC1, SqlLiteral(1, SqlIntegerType))

    val c2Type = StructType(Seq(StructField("f1", SqlIntegerType)))
    val sqlF1 = GetStructField(SqlAttributeReference("c2", c2Type)(), 0, Some("f1"))
    val unsupportedEq = SqlEqualTo(sqlF1, SqlLiteral(5, SqlIntegerType))

    val converted = OpConverter.convert(
      Seq(SqlAnd(supportedEq, unsupportedEq)), partialFilterEnabled = true).get
    val (pruned, hadUnsupported) = UnsupportedOpPruner.prune(converted)

    assert(hadUnsupported == true)
    // After pruning, only the supported EqualOp remains -- safe to send to server.
    assert(pruned.get.isInstanceOf[EqualOp])
  }

  test("convert then prune end-to-end: OR(supported, unsupported) -> None") {
    val sqlC1 = SqlAttributeReference("c1", SqlIntegerType)()
    val supportedEq = SqlEqualTo(sqlC1, SqlLiteral(1, SqlIntegerType))

    val c2Type = StructType(Seq(StructField("f1", SqlIntegerType)))
    val sqlF1 = GetStructField(SqlAttributeReference("c2", c2Type)(), 0, Some("f1"))
    val unsupportedEq = SqlEqualTo(sqlF1, SqlLiteral(5, SqlIntegerType))

    val converted = OpConverter.convert(
      Seq(SqlOr(supportedEq, unsupportedEq)), partialFilterEnabled = true).get
    val (pruned, hadUnsupported) = UnsupportedOpPruner.prune(converted)

    assert(hadUnsupported == true)
    // OR with unsupported child must be dropped entirely -- cannot safely prune.
    assert(pruned.isEmpty)
  }

  test("convertOneOrUnsupported catch path: unsupported expr nested in AND via exception") {
    // An expression that is not a known leaf and not a known compound op hits the wildcard
    // case _ => unsupportedOrThrow. With partialFilterEnabled=true this goes through
    // convertOneOrUnsupported's try/catch path when called from boolean operators.
    val c2Type = StructType(Seq(StructField("f1", SqlIntegerType)))
    val sqlF1 = GetStructField(SqlAttributeReference("c2", c2Type)(), 0, Some("f1"))
    // Wrap in AND so convertOneOrUnsupported is called on the unsupported child.
    val sqlC1 = SqlAttributeReference("c1", SqlIntegerType)()
    val supportedEq = SqlEqualTo(sqlC1, SqlLiteral(1, SqlIntegerType))
    val sqlAnd = SqlAnd(supportedEq, sqlF1)  // sqlF1 alone is not a known compound op

    val op = OpConverter.convert(Seq(sqlAnd), partialFilterEnabled = true).get.asInstanceOf[AndOp]
    assert(op.children(1).isInstanceOf[UnsupportedOp])
  }
}
