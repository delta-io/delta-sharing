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

package io.delta.sharing.spark

import java.lang.ref.WeakReference
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import java.sql.{Date, Timestamp}
import java.time.{Duration, Instant, LocalDate, LocalDateTime, Period, ZoneOffset}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{
  Add,
  And,
  Attribute,
  AttributeReference,
  BoundReference,
  Cast,
  Concat,
  DateAdd,
  Expression,
  GenericInternalRow,
  GreaterThan,
  LessThan,
  Literal,
  Or,
  SubqueryExpression
}
import org.apache.spark.sql.catalyst.trees.CurrentOrigin.withOrigin
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.execution.datasources.{FileFormat, FileIndex, HadoopFsRelation, PartitionDirectory}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

import io.delta.sharing.spark.model.{
  AddCDCFile,
  AddFile,
  AddFileForCDF,
  CDFColumnInfo,
  FileAction,
  RemoveFile,
  Table
}
import io.delta.sharing.spark.util.JsonUtils

// scalastyle:off println

class PredicateSuite extends SparkFunSuite with SharedSparkSession {

  protected def create_row(values: Any*): InternalRow = {
    InternalRow.fromSeq(values.map(CatalystTypeConverters.convertToCatalyst))
  }

  protected def prefix(level: Int): String = {
    var prefix = ""
    for (i <- 1 to level) {
      prefix = prefix + " "
    }
    prefix
  }

  protected def getStr(expr: Expression): String = {
    var n = expr.prettyName
    expr match {
      case a: Attribute =>
        n = n + ":" + a.qualifiedName
      case l: Literal =>
        n = n + n + ":" + l.dataType + ":" + l.value.getClass + ":"
        l.dataType match {
          case d: DateType =>
            n = n + (new Date(l.value.asInstanceOf[Integer].toLong)).toString
          case _ => n = n + l.value
        }
      case _ =>
    }
    n
  }

  protected def traverse(expr: Expression, level: Int = 0): Unit = {
    if (level == 0) {
      println("....TRAVERSAL START.....")
    }
    val pre = prefix(level)
    println(pre + getStr(expr))
    expr.children.forall(c => {
      traverse(c, level + 1)
      true
    })
    if (level == 0) {
      println("....TRAVERSAL END.....")
    }
  }

  test("Equal test") {
    val op = AndOp(Seq(
      EqualOp(
        left = ColumnOp(name = "date", valueType = "date"),
        right = LiteralOp(value = "2021-04-29", valueType = "date")
      ),
      EqualOp(
        left = ColumnOp(name = "id", valueType = "int"),
        right = LiteralOp(value = "25", valueType = "int")
      )
    ))
    println("Abhijit: op=" + op)
    val op_json2 = OpMapper.toJson[BaseOp](op)
    println("Abhijit: op_json2=" + op_json2)
    val op2 = OpMapper.fromJson[BaseOp](op_json2)
    println("Abhijit: op2=" + op2)

    println("expect false: " + op2.eval(EvalContext(Map("date" -> "2021-04-28"))))
    println("expect false: " + op2.eval(EvalContext(Map("date" -> "2021-04-29"))))
    println("expect false: " + op2.eval(EvalContext(Map("id" -> "25"))))
    println("expect false: " + op2.eval(EvalContext(Map("id" -> "20"))))
    println("expect false: " + op2.eval(EvalContext(Map("date" -> "2021-04-30", "id" -> "25"))))
    println("expect false: " + op2.eval(EvalContext(Map("date" -> "2021-04-29", "id" -> "21"))))
    println("expect true: " + op2.eval(EvalContext(Map("date" -> "2021-04-29", "id" -> "25"))))
  }

  test("Null test") {
    val op = EqualOp(
      left = ColumnOp(name = "date", valueType = "date"),
      right = LiteralOp(null, null)
    )
    println("Abhijit: op=" + op)
    val op_json2 = OpMapper.toJson[BaseOp](op)
    println("Abhijit: op_json2=" + op_json2)
    val op2 = OpMapper.fromJson[BaseOp](op_json2)
    println("Abhijit: op2=" + op2)

    println("expect false: " + op2.eval(EvalContext(Map("date" -> "2021-04-28"))))
    println("expect true: " + op2.eval(EvalContext(Map())))

    val nop = NotEqualOp(
      left = ColumnOp(name = "date", valueType = "date"),
      right = LiteralOp(null, null)
    )
    val nop_json = OpMapper.toJson[BaseOp](nop)
    val nop2 = OpMapper.fromJson[BaseOp](nop_json)
    println("expect true: " + nop2.eval(EvalContext(Map("date" -> "2021-04-28"))))
    println("expect false: " + nop2.eval(EvalContext(Map())))
  }

  test("LessThan test") {
    val op = AndOp(Seq(
      LessThanOp(
        left = ColumnOp(name = "date", valueType = "date"),
        right = LiteralOp(value = "2021-04-29", valueType = "date")
      ),
      LessThanOp(
        left = ColumnOp(name = "id", valueType = "int"),
        right = LiteralOp(value = "25", valueType = "int")
      )
    ))
    println("Abhijit: op=" + op)
    val op_json = OpMapper.toJson[BaseOp](op)
    println("Abhijit: op_json=" + op_json)
    val op2 = OpMapper.fromJson[BaseOp](op_json)
    println("Abhijit: op2=" + op2)

    println("expect true: " + op2.eval(EvalContext(Map("date" -> "2021-04-28"))))
    println("expect false: " + op2.eval(EvalContext(Map("date" -> "2021-04-29"))))
    println("expect false: " + op2.eval(EvalContext(Map("id" -> "25"))))
    println("expect true: " + op2.eval(EvalContext(Map("id" -> "20"))))
    println("expect false: " + op2.eval(EvalContext(Map("date" -> "2021-04-30", "id" -> "25"))))
    println("expect false: " + op2.eval(EvalContext(Map("date" -> "2021-04-29", "id" -> "25"))))
    println("expect false: " + op2.eval(EvalContext(Map("date" -> "2021-04-28", "id" -> "25"))))
    println("expect false: " + op2.eval(EvalContext(Map("date" -> "2021-04-29", "id" -> "24"))))
    println("expect true: " + op2.eval(EvalContext(Map("date" -> "2021-04-28", "id" -> "21"))))
  }

  /*
  test("AttributeReference") {
    val date = Date.valueOf("2019-02-22")
    println(
      "date=" + date + ", class=" + date.getClass +
      ", getStr=" + getStr(Literal.create(date))
    )
    val date2 = LocalDate.of(2019, 3, 21)
    println(
      "date2=" + date2 + ", class=" + date2.getClass +
      ", getStr=" + getStr(Literal.create(date2))
    )
    // val xx = new Date(date2.[Integer].toLong)).toString

    val l1 = Literal.create(LocalDate.of(2019, 3, 21))
    println("l1=" + l1)

    val a1 = AttributeReference("a1", DateType, false)()
    println("a1=" + a1)
    val e = LessThan(a1, l1)
    traverse(e)

    val a2 = AttributeReference("a2", DateType, false)(qualifier = Seq("abhijit", "chakankar"))
    println("a2=" + a2)
    val e2 = LessThan(a2, a1)
    traverse(e2)
  }

  test("Int Literals") {
    val a1 = Literal(1)
    println("a1=" + a1)
    val a2 = Literal(2)
    val a = Add(a1, a2)
    println("a=" + a + ", type=" + a.getClass)
    val b1 = Literal(10)
    val b2 = Literal(20)
    val b = Add(b1, b2)
    println("b=" + b)
    val ab = Add(a, b)
    println("ab=" + ab)

    val res = ab.eval(null)
    traverse(ab)
    println("res=" + res)
  }

  test("String Literals") {
    val a1 = Literal("a1")
    println("a1=" + a1)
    val a2 = Literal("a2")
    val a = Concat(Seq(a1, a2))
    println("a=" + a + ", type=" + a.getClass)
    val b1 = Literal("b10")
    val b2 = Literal("b20")
    val b = Concat(Seq(b1, b2))
    println("b=" + b)
    val ab = Concat(Seq(a, b))
    println("ab=" + ab)

    val res = ab.eval(null)
    println("res=" + res)
  }

  test("Date literals") {
    val a1 = Literal.create(LocalDate.of(2019, 3, 21))
    println("a1=" + a1)
    val a2 = Literal(1)
    val a = DateAdd(a1, a2)
    println("a=" + a)

    val b1 = Literal.create(LocalDate.of(2019, 2, 21))
    val ab = LessThan(a, b1)
    println("ab=" + ab)

    val res = ab.eval(null)
    println("res=" + res)

    val date = Date.valueOf("2019-02-22")

    val row = create_row(1, 2, "a", "b", "c", date)
    println("row=" + row)

    val d1 = BoundReference(5, DateType, false)
    println("d1=" + d1)
    println("d1<b1=" + LessThan(d1, b1).eval(row))
    println("d1<a1=" + LessThan(d1, a1).eval(row))
    println("d1<a1 || d1<b1=" + Or(LessThan(d1, a1), LessThan(d1, b1)).eval(row))
    println("d1<a1 && d1<b1=" + And(LessThan(d1, a1), LessThan(d1, b1)).eval(row))
    traverse(And(LessThan(d1, a1), GreaterThan(d1, b1)))
    println("d1<a1 && d1>b1=" + And(LessThan(d1, a1), GreaterThan(d1, b1)).eval(row))

    val r1 = BoundReference(0, IntegerType, true)
    println("r1=" + r1.eval(row) + ", type=" + r1.getClass)
    val rr1 = LessThan(r1, Literal(2))
    println("rr1=" + rr1.eval(row))
    traverse(rr1)

    val r2 = BoundReference(1, IntegerType, true)
    println("r2=" + r2.eval(row))
    val rr2 = LessThan(r2, Literal(2))
    println("rr2=" + rr2.eval(row))

    println("expr1 = " + LessThan(r1, r2))
    println("r1<r2 = " + LessThan(r1, r2).eval(row))
    println("r2<r1 = " + LessThan(r2, r1).eval(row))

    traverse(LessThan(r1, r2))

    val c1 = Concat(Seq(
      BoundReference(2, StringType, true),
      BoundReference(3, StringType, true),
      BoundReference(4, StringType, true)))
    traverse(c1)
    println("c1=" + c1)
    println("concat: " + c1.eval(row))
    println("lt-str: " + LessThan(
      BoundReference(2, StringType, true),
      BoundReference(3, StringType, true)).
      eval(row))
    println("lt-str2: " + LessThan(
      BoundReference(4, StringType, true),
      BoundReference(3, StringType, true)).
      eval(row))

    // val c2 = $"a".int.at(0)
    val c2 = $"a"
    println("c2=" + c2 + ", type=" + c2.getClass)
  }
  */
}
