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

import com.ibm.icu.util.VersionInfo.ICU_VERSION
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.{
  AttributeReference => SqlAttributeReference,
  Cast => SqlCast,
  EqualTo => SqlEqualTo,
  GreaterThan => SqlGreaterThan,
  GreaterThanOrEqual => SqlGreaterThanOrEqual,
  In => SqlIn,
  LessThan => SqlLessThan,
  LessThanOrEqual => SqlLessThanOrEqual,
  Literal => SqlLiteral
}
import org.apache.spark.sql.types.{
  StringType => SqlStringType
}

class OpConverterCollationSuite extends SparkFunSuite {

  val icuVersion: String = s"${ICU_VERSION.getMajor}.${ICU_VERSION.getMinor}"

  test("UTF8_BINARY collation test") {
    val defaultStringType = SqlStringType
    val sqlColumn = SqlAttributeReference("email", defaultStringType)()
    val sqlLiteral = SqlLiteral("test@example.com")
    val sqlEq = SqlEqualTo(sqlColumn, sqlLiteral)

    val op = OpConverter.convert(Seq(sqlEq)).get.asInstanceOf[EqualOp]
    op.validate()

    val columnOp = op.children(0).asInstanceOf[ColumnOp]
    val literalOp = op.children(1).asInstanceOf[LiteralOp]
    assert(columnOp.valueType == OpDataTypes.StringType)
    assert(literalOp.valueType == OpDataTypes.StringType)

    assert(op.exprCtx.isEmpty)
  }

  test("collated string UNICODE_CI equal test") {
    val collatedStringType = SqlStringType("UNICODE_CI")
    val sqlColumn = SqlAttributeReference("name", collatedStringType)()
    val sqlLiteral = SqlLiteral.create("TestValue", collatedStringType)
    val sqlEq = SqlEqualTo(sqlColumn, sqlLiteral)

    val op = OpConverter.convert(Seq(sqlEq)).get.asInstanceOf[EqualOp]
    op.validate()

    val columnOp = op.children(0).asInstanceOf[ColumnOp]
    val literalOp = op.children(1).asInstanceOf[LiteralOp]
    assert(columnOp.valueType == OpDataTypes.StringType)
    assert(literalOp.valueType == OpDataTypes.StringType)

    assert(op.exprCtx.isDefined)
    assert(op.exprCtx.get.collationIdentifier.isDefined)
    val collationId = op.exprCtx.get.collationIdentifier.get
    assert(collationId == s"icu.UNICODE_CI.$icuVersion")
  }

  test("collated string UTF8_LCASE equal test") {
    val collatedStringType = SqlStringType("UTF8_LCASE")
    val sqlColumn = SqlAttributeReference("name", collatedStringType)()
    val sqlLiteral = SqlLiteral.create("TestValue", collatedStringType)
    val sqlEq = SqlEqualTo(sqlColumn, sqlLiteral)

    val op = OpConverter.convert(Seq(sqlEq)).get.asInstanceOf[EqualOp]
    op.validate()

    // Verify that valueType is plain string
    val columnOp = op.children(0).asInstanceOf[ColumnOp]
    val literalOp = op.children(1).asInstanceOf[LiteralOp]
    assert(columnOp.valueType == OpDataTypes.StringType)
    assert(literalOp.valueType == OpDataTypes.StringType)

    // Verify that collationIdentifier is correctly set with spark provider
    assert(op.exprCtx.isDefined)
    assert(op.exprCtx.get.collationIdentifier.isDefined)
    val collationId = op.exprCtx.get.collationIdentifier.get
    assert(collationId == s"spark.UTF8_LCASE.$icuVersion")
  }

  test("collated string with cast test") {
    val collatedStringType = SqlStringType("UNICODE_CI")
    val sqlColumn = SqlAttributeReference("name", collatedStringType)()
    val sqlLiteral = SqlLiteral("TestValue")
    // Cast the literal to the collated type
    val sqlEq = SqlEqualTo(sqlColumn, SqlCast(sqlLiteral, collatedStringType))

    val op = OpConverter.convert(Seq(sqlEq)).get.asInstanceOf[EqualOp]
    op.validate()

    // Verify that valueType is plain string
    val columnOp = op.children(0).asInstanceOf[ColumnOp]
    val literalOp = op.children(1).asInstanceOf[LiteralOp]
    assert(columnOp.valueType == OpDataTypes.StringType)
    assert(literalOp.valueType == OpDataTypes.StringType)

    // Verify that collationIdentifier is correctly set
    assert(op.exprCtx.isDefined)
    assert(op.exprCtx.get.collationIdentifier.isDefined)
    val collationId = op.exprCtx.get.collationIdentifier.get
    assert(collationId == s"icu.UNICODE_CI.$icuVersion")
  }

  test("collated string comparison operations test") {
    val collatedStringType = SqlStringType("UNICODE_CI")
    val sqlColumn = SqlAttributeReference("name", collatedStringType)()
    val sqlLiteral = SqlLiteral.create("TestValue", collatedStringType)

    val expectedCollationId = s"icu.UNICODE_CI.$icuVersion"

    // Test LessThan
    val ltOp = OpConverter.convert(Seq(SqlLessThan(sqlColumn, sqlLiteral)))
      .get.asInstanceOf[LessThanOp]
    assert(ltOp.exprCtx.isDefined)
    assert(ltOp.exprCtx.get.collationIdentifier.contains(expectedCollationId))

    // Test GreaterThan
    val gtOp = OpConverter.convert(Seq(SqlGreaterThan(sqlColumn, sqlLiteral)))
      .get.asInstanceOf[GreaterThanOp]
    assert(gtOp.exprCtx.isDefined)
    assert(gtOp.exprCtx.get.collationIdentifier.contains(expectedCollationId))

    // Test LessThanOrEqual
    val lteOp = OpConverter.convert(Seq(SqlLessThanOrEqual(sqlColumn, sqlLiteral)))
      .get.asInstanceOf[LessThanOrEqualOp]
    assert(lteOp.exprCtx.isDefined)
    assert(lteOp.exprCtx.get.collationIdentifier.contains(expectedCollationId))

    // Test GreaterThanOrEqual
    val gteOp = OpConverter.convert(Seq(SqlGreaterThanOrEqual(sqlColumn, sqlLiteral)))
      .get.asInstanceOf[GreaterThanOrEqualOp]
    assert(gteOp.exprCtx.isDefined)
    assert(gteOp.exprCtx.get.collationIdentifier.contains(expectedCollationId))
  }

  test("collated string In expression test") {
    val collatedStringType = SqlStringType("UNICODE_CI")
    val sqlColumn = SqlAttributeReference("name", collatedStringType)()
    val sqlLiterals = Seq("Value1", "Value2", "Value3").map(v =>
      SqlLiteral.create(v, collatedStringType)
    )
    val sqlIn = SqlIn(sqlColumn, sqlLiterals)

    val op = OpConverter.convert(Seq(sqlIn)).get
    op.validate()

    val orOp = op.asInstanceOf[OrOp]
    assert(orOp.children.size == 3)

    // Verify that each EqualOp has the correct collationIdentifier
    val expectedCollationId = s"icu.UNICODE_CI.$icuVersion"
    orOp.children.foreach { child =>
      val equalOp = child.asInstanceOf[EqualOp]
      assert(equalOp.exprCtx.isDefined)
      assert(equalOp.exprCtx.get.collationIdentifier.contains(expectedCollationId))

      // Verify that valueType is plain string
      val columnOp = equalOp.children(0).asInstanceOf[ColumnOp]
      val literalOp = equalOp.children(1).asInstanceOf[LiteralOp]
      assert(columnOp.valueType == OpDataTypes.StringType)
      assert(literalOp.valueType == OpDataTypes.StringType)
    }
  }

  test("mismatched collations throw error") {
    val unicodeCIType = SqlStringType("UNICODE_CI")
    val utf8LcaseType = SqlStringType("UTF8_LCASE")

    val columnUnicodeCI = SqlAttributeReference("name1", unicodeCIType)()
    val columnUtf8Lcase = SqlAttributeReference("name2", utf8LcaseType)()

    // Test with EqualTo
    val exception1 = intercept[IllegalArgumentException] {
      OpConverter.convert(Seq(SqlEqualTo(columnUnicodeCI, columnUtf8Lcase)))
    }
    assert(exception1.getMessage.contains("Cannot compare strings with different collations"))
    assert(exception1.getMessage.contains("UNICODE_CI"))
    assert(exception1.getMessage.contains("UTF8_LCASE"))

    // Test with LessThan
    val exception2 = intercept[IllegalArgumentException] {
      OpConverter.convert(Seq(SqlLessThan(columnUnicodeCI, columnUtf8Lcase)))
    }
    assert(exception2.getMessage.contains("Cannot compare strings with different collations"))

    // Test with GreaterThan
    val exception3 = intercept[IllegalArgumentException] {
      OpConverter.convert(Seq(SqlGreaterThan(columnUnicodeCI, columnUtf8Lcase)))
    }
    assert(exception3.getMessage.contains("Cannot compare strings with different collations"))
  }

  test("collated vs non-collated string throws error") {
    val collatedType = SqlStringType("UNICODE_CI")
    val defaultType = SqlStringType

    val columnCollated = SqlAttributeReference("name1", collatedType)()
    val columnDefault = SqlAttributeReference("name2", defaultType)()

    val exception = intercept[IllegalArgumentException] {
      OpConverter.convert(Seq(SqlEqualTo(columnCollated, columnDefault)))
    }
    assert(exception.getMessage.contains("Cannot compare strings with different collations"))
    assert(exception.getMessage.contains("string collate UNICODE_CI"))
    assert(exception.getMessage.contains("string"))
  }
}
