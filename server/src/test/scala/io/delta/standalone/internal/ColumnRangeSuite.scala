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

package io.delta.standalone.internal

import org.scalatest.FunSuite

class ColumnRangeSuite extends FunSuite {

  test("invalid column range test") {
    assert(intercept[IllegalArgumentException] {
      ColumnRange("ab", "aa")
    }.getMessage.contains("requirement failed"))

    assert(intercept[IllegalArgumentException] {
      ColumnRange(100, 99)
    }.getMessage.contains("requirement failed"))

    assert(intercept[IllegalArgumentException] {
      ColumnRange(9000000000L, 8000000000L)
    }.getMessage.contains("requirement failed"))

    assert(intercept[IllegalArgumentException] {
      ColumnRange(true, false)
    }.getMessage.contains("requirement failed"))

    assert(intercept[IllegalArgumentException] {
      ColumnRange.toDate("2023-01-01", "2022-12-31")
    }.getMessage.contains("requirement failed"))

    assert(intercept[IllegalArgumentException] {
      ColumnRange.toFloat("2.1", "1.2")
    }.getMessage.contains("requirement failed"))

    assert(intercept[IllegalArgumentException] {
      ColumnRange.toDouble("100.12345", "100.12344")
    }.getMessage.contains("requirement failed"))

    assert(intercept[IllegalArgumentException] {
      ColumnRange.toTimestamp("2023-06-07T04:27:03.234Z", "2023-06-07T04:27:03.233Z")
    }.getMessage.contains("requirement failed"))
  }

  test("string test") {
    val cRange = ColumnRange("def", "lmn")

    // Test contains.
    assert(cRange.contains("dxy"))
    assert(cRange.contains("def"))
    assert(cRange.contains("lmn"))
    assert(!cRange.contains("dd"))
    assert(!cRange.contains("lx"))

    // Test canBeLess.
    assert(cRange.canBeLess("ln"))
    assert(cRange.canBeLess("gg"))
    assert(cRange.canBeLess("lmn"))
    assert(!cRange.canBeLess("bb"))

    // Test canBeGreater.
    assert(cRange.canBeGreater("lmm"))
    assert(cRange.canBeGreater("gg"))
    assert(cRange.canBeGreater("lmm"))
    assert(!cRange.canBeGreater("mm"))
  }

  test("int test") {
    val cRange = ColumnRange(100, 200)

    // Test contains.
    assert(cRange.contains(110))
    assert(cRange.contains(100))
    assert(cRange.contains(200))
    assert(!cRange.contains(99))
    assert(!cRange.contains(201))

    // Test canBeLess.
    assert(cRange.canBeLess(201))
    assert(cRange.canBeLess(120))
    assert(!cRange.canBeLess(100))
    assert(!cRange.canBeLess(-1))

    // Test canBeGreater.
    assert(cRange.canBeGreater(199))
    assert(cRange.canBeGreater(120))
    assert(!cRange.canBeGreater(200))
    assert(!cRange.canBeGreater(1001))
  }

  test("long test") {
    val cRange = ColumnRange(8000000000L, 9000000000L)

    // Test contains.
    assert(cRange.contains(8500000000L))
    assert(cRange.contains(8000000000L))
    assert(cRange.contains(9000000000L))
    assert(!cRange.contains(7000000000L))
    assert(!cRange.contains(9000000001L))

    // Test canBeLess.
    assert(cRange.canBeLess(9100000000L))
    assert(cRange.canBeLess(9000000000L))
    assert(cRange.canBeLess(8500000000L))
    assert(!cRange.canBeLess(8000000000L))

    // Test canBeGreater.
    assert(cRange.canBeGreater(7100000000L))
    assert(cRange.canBeGreater(8000000000L))
    assert(cRange.canBeGreater(8100000000L))
    assert(!cRange.canBeGreater(9100000000L))
  }

  test("boolean test") {
    // Test contains.
    assert(ColumnRange(true, true).contains(true))
    assert(!ColumnRange(true, true).contains(false))
    assert(ColumnRange(false, true).contains(true))
    assert(ColumnRange(false, true).contains(false))
    assert(ColumnRange(false, false).contains(false))
    assert(!ColumnRange(false, false).contains(true))

    // Test canBeLess.
    assert(ColumnRange(false, false).canBeLess(true))
    assert(!ColumnRange(false, false).canBeLess(false))
    assert(ColumnRange(false, true).canBeLess(true))
    assert(!ColumnRange(false, true).canBeLess(false))
    assert(!ColumnRange(true, true).canBeLess(true))
    assert(!ColumnRange(true, true).canBeLess(false))

    // Test canBeGreater.
    assert(!ColumnRange(false, false).canBeGreater(true))
    assert(!ColumnRange(false, false).canBeGreater(false))
    assert(!ColumnRange(false, true).canBeGreater(true))
    assert(ColumnRange(false, true).canBeGreater(false))
    assert(!ColumnRange(true, true).canBeGreater(true))
    assert(ColumnRange(true, true).canBeGreater(false))
  }

  test("date test") {
    val cRange = ColumnRange.toDate("2022-04-15", "2023-01-15")

    // Test contains.
    assert(cRange.contains(java.sql.Date.valueOf("2022-05-01")))
    assert(cRange.contains(java.sql.Date.valueOf("2022-04-15")))
    assert(cRange.contains(java.sql.Date.valueOf("2023-01-15")))
    assert(!cRange.contains(java.sql.Date.valueOf("2022-04-14")))
    assert(!cRange.contains(java.sql.Date.valueOf("2023-01-16")))

    // Test canBeLess.
    assert(cRange.canBeLess(java.sql.Date.valueOf("2023-05-01")))
    assert(cRange.canBeLess(java.sql.Date.valueOf("2023-01-15")))
    assert(cRange.canBeLess(java.sql.Date.valueOf("2022-08-15")))
    assert(!cRange.canBeLess(java.sql.Date.valueOf("2022-04-15")))

    // Test canBeGreater.
    assert(cRange.canBeGreater(java.sql.Date.valueOf("2023-01-01")))
    assert(cRange.canBeGreater(java.sql.Date.valueOf("2022-04-15")))
    assert(cRange.canBeGreater(java.sql.Date.valueOf("2022-08-15")))
    assert(!cRange.canBeGreater(java.sql.Date.valueOf("2023-04-15")))
  }

  test("float test") {
    val cRange = ColumnRange(100.2.toFloat, 200.5.toFloat)

    // Test contains.
    assert(cRange.contains(110.5.toFloat))
    assert(cRange.contains(100.2.toFloat))
    assert(cRange.contains(200.5.toFloat))
    assert(!cRange.contains(100.199.toFloat))
    assert(!cRange.contains(200.51.toFloat))

    // Test canBeLess.
    assert(cRange.canBeLess(200.51.toFloat))
    assert(cRange.canBeLess(120.toFloat))
    assert(!cRange.canBeLess(100.199.toFloat))
    assert(!cRange.canBeLess(-1.0.toFloat))

    // Test canBeGreater.
    assert(cRange.canBeGreater(200.49.toFloat))
    assert(cRange.canBeGreater(120.1.toFloat))
    assert(!cRange.canBeGreater(200.5.toFloat))
    assert(!cRange.canBeGreater(1001.toFloat))
  }

  test("Double test") {
    val cRange = ColumnRange(100.2.toDouble, 200.5.toDouble)

    // Test contains.
    assert(cRange.contains(110.5))
    assert(cRange.contains(100.2))
    assert(cRange.contains(200.5))
    assert(!cRange.contains(100.199))
    assert(!cRange.contains(200.51))

    // Test canBeLess.
    assert(cRange.canBeLess(200.51))
    assert(cRange.canBeLess(120))
    assert(!cRange.canBeLess(100.199))
    assert(!cRange.canBeLess(-1.0))

    // Test canBeGreater.
    assert(cRange.canBeGreater(200.49))
    assert(cRange.canBeGreater(120.1))
    assert(!cRange.canBeGreater(200.5))
    assert(!cRange.canBeGreater(1001))
  }

  test("timestamp test") {
    val cRange = ColumnRange.toTimestamp("2023-06-07T04:27:03.234Z", "2023-07-07T04:27:03.000Z")

    // Test contains.
    assert(cRange.contains(TimestampUtils.parse("2023-06-10T00:00:00.000Z")))
    assert(cRange.contains(TimestampUtils.parse("2023-06-07T04:27:03.234Z")))
    assert(cRange.contains(TimestampUtils.parse("2023-07-07T04:27:03.000Z")))
    assert(!cRange.contains(TimestampUtils.parse("2023-06-07T04:27:03.233Z")))
    assert(!cRange.contains(TimestampUtils.parse("2023-07-07T05:27:03.000Z")))

    // Test canBeLess.
    assert(cRange.canBeLess(TimestampUtils.parse("2024-07-07T04:27:03.000Z")))
    assert(cRange.canBeLess(TimestampUtils.parse("2023-07-07T04:27:03.000Z")))
    assert(cRange.canBeLess(TimestampUtils.parse("2023-06-15T04:27:03.234Z")))
    assert(!cRange.canBeLess(TimestampUtils.parse("2023-06-07T04:27:03.234Z")))

    // Test canBeGreater.
    assert(cRange.canBeGreater(TimestampUtils.parse("2023-07-06T04:27:03.000Z")))
    assert(cRange.canBeGreater(TimestampUtils.parse("2023-06-07T04:27:03.234Z")))
    assert(cRange.canBeGreater(TimestampUtils.parse("2023-07-07T03:27:03.234Z")))
    assert(!cRange.canBeGreater(TimestampUtils.parse("2023-07-09T04:27:03.000Z")))
  }
}
