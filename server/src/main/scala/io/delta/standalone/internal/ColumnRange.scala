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

import scala.math.Ordering.Implicits

// ColumnRange class represents a range of values that a Column can take in a data file.
// ColumnRange is an interval on the Column type, and all types need to support Ordering.
//
// Note: ColumnRange intervals are inclusive, and support point intervals.
//
// ColumnRange can be used to perform point-based or range-based evaluations for data skipping.
//
// Here is an example use case:
//
// Consider the following query where colA is a partition column, and <colB, ColC> are not:
//   SELECT * from <table> where colA = "foo" AND colB = 1 AND colC < 100
//
//   The json predicate op tree for this will be:
//     And(
//       Equal(Column("colA", "string"), Literal("foo", "string")),
//       Equal(Column("colB", "int"), Literal("1", "int")),
//       LessThan(Column("colC", "int"), Literal("100", "int"))
//     )
//
//   Suppose the table has 2 data files:
//     1.parquet:
//       - partition_values("colA" -> "xyz")
//       - stats: min_values("colB" -> "0", "colC" -> 50), max_values("colB" -> "2", "colC" -> 75)
//     2.parquet:
//       - partition_values("colA" -> "foo")
//       - stats: min_values("colB" -> "1", "colC" -> 98), max_values("colB" -> "5", "colC" -> 101)
//
//   During evaluation of the json predicate op, we will resolve columns for each data file:
//     - Replace colA with its corresponding partition value in data file (a point ColumnRange),
//     - Replace colB and colC with their corresponding stats values (normal ColumnRange).
//
//   After columns are resolved during evaluation, we get:
//     - For 1.parquet:
//         And(
//           ColumnRange("xyz", "xyz").contains("foo"),
//           ColumnRange(0,2).contains(1),
//           ColumnRange(50, 75).canBeLess(100)
//         )
//       This evaluates to false, so 1.parquet can be skipped.
//
//     - For 2.parquet:
//         And(
//           ColumnRange("foo", "foo").contains("foo"),
//           ColumnRange(1, 5).contains(1),
//           ColumnRange(98, 101).canBeLess(100)
//         )
//       This evaluates to true, so 2.parquet cannot be skipped.
//
class ColumnRange[T](val minVal: T, val maxVal: T)(implicit ord: Ordering[T]) {
  // Validate the interval.
  require(ord.compare(minVal, maxVal) <= 0)

  // Returns true if "this" interval contains the specified point.
  // An interval i contains a point p iff:
  //   (i.min <= p) && (i.max >= p).
  //
  // This function is used to perform equality check in json predicate ops.
  def contains(point: T): Boolean = {
    val c1 = ord.compare(minVal, point)
    val c2 = ord.compare(maxVal, point)
    (c1 <= 0 && c2 >= 0)
  }

  // Returns true if "this" interval can be less than the specified point.
  // An iterval i can be less than p iff its min is less than p.
  //   i.min < p
  //
  // This function is used to perform lessThan check in json predicate op.
  def canBeLess(point: T): Boolean = {
    ord.compare(this.minVal, point) < 0
  }

  // Returns true if "this" interval can be greater than the specified point.
  // An iterval i can be more than p iff its max is more than p.
  //   i.max > p
  //
  // This function is used to perform lessThan check in json predicate op.
  def canBeGreater(point: T): Boolean = {
    ord.compare(this.maxVal, point) > 0
  }

  // Other operations can be expressed in terms of these.
}

object ColumnRange {
  // Create regular intervals.
  def apply[T](minVal: T, maxVal: T)(implicit ord: Ordering[T]): ColumnRange[T] = {
    new ColumnRange(minVal, maxVal)
  }

  // Create point intervals.
  def apply[T](ptVal: T)(implicit ord: Ordering[T]): ColumnRange[T] = {
    new ColumnRange(ptVal, ptVal)
  }

  // Define Ordering on java.sql.Date class.
  implicit val sqlDateOrdering = new Ordering[java.sql.Date] {
    def compare(x: java.sql.Date, y: java.sql.Date): Int = {
      if (x.before(y)) {
        -1
      } else if (x.equals(y)) {
        0
      } else {
        1
      }
    }
  }

  // Define Ordering on java.sql.Timestamp class.
  implicit val sqlTimestampOrdering = new Ordering[java.sql.Timestamp] {
    def compare(x: java.sql.Timestamp, y: java.sql.Timestamp): Int = {
      if (x.before(y)) {
        -1
      } else if (x.equals(y)) {
        0
      } else {
        1
      }
    }
  }

  // Some utility functions that convert a string range to other types.

  def toInt(minVal: String, maxVal: String): ColumnRange[Integer] = {
    new ColumnRange[Integer](minVal.toInt, maxVal.toInt)
  }

  def toBoolean(minVal: String, maxVal: String): ColumnRange[Boolean] = {
    new ColumnRange[Boolean](minVal.toBoolean, maxVal.toBoolean)
  }

  def toLong(minVal: String, maxVal: String): ColumnRange[Long] = {
    new ColumnRange[Long](minVal.toLong, maxVal.toLong)
  }

  def toDate(minVal: String, maxVal: String): ColumnRange[java.sql.Date] = {
    new ColumnRange[java.sql.Date](
      java.sql.Date.valueOf(minVal),
      java.sql.Date.valueOf(maxVal)
    )
  }

  def toFloat(minVal: String, maxVal: String): ColumnRange[Float] = {
    new ColumnRange[Float](minVal.toFloat, maxVal.toFloat)
  }

  def toDouble(minVal: String, maxVal: String): ColumnRange[Double] = {
    new ColumnRange[Double](minVal.toDouble, maxVal.toDouble)
  }

  def toTimestamp(minVal: String, maxVal: String): ColumnRange[java.sql.Timestamp] = {
    new ColumnRange[java.sql.Timestamp](TimestampUtils.parse(minVal), TimestampUtils.parse(maxVal))
  }
}
