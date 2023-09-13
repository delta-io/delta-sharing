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

class TimestampUtilsSuite extends FunSuite {
  test("basic test") {
    // Only ISO 8601 is supported.
    TimestampUtils.parse("2023-06-10T00:00:00.000Z")
    TimestampUtils.parse("2023-06-10T01:02:13Z")

    // Other formats will trigger errors.
    assert(intercept[java.time.format.DateTimeParseException] {
      TimestampUtils.parse("2023-06-10 00:00:00.234")
    }.getMessage.contains("could not be parsed"))

    assert(intercept[java.time.format.DateTimeParseException] {
      TimestampUtils.parse("2023-06-10")
    }.getMessage.contains("could not be parsed"))

    assert(intercept[java.time.format.DateTimeParseException] {
      TimestampUtils.parse("2023-06-10 00:00:00")
    }.getMessage.contains("could not be parsed"))
  }
}
