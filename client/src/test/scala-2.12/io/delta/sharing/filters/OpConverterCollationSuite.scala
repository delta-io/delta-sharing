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
  AttributeReference => SqlAttributeReference,
  EqualTo => SqlEqualTo,
  Literal => SqlLiteral
}
import org.apache.spark.sql.types.{
  StringType => SqlStringType
}

class OpConverterCollationSuite extends SparkFunSuite {

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

    // UTF8_BINARY (default) should work fine on Scala 2.12
    assert(op.exprCtx.isEmpty)
  }

  // Note: Collated string types (SqlStringType with collation parameter) don't exist in
  // Spark 3.5 (Scala 2.12). They were added in Spark 4.0 (Scala 2.13).
  // Therefore, we cannot test collation behavior in the Scala 2.12 version of this suite.
  // All collation-specific tests are in the Scala 2.13 version of this suite.
}
