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

// A utility class that supports timestamp operations.
//
// We only support UTC timestamps for data skipping in the ISO 8601 format.
//
// This is also the format in which delta stores the stats ranges in the delta log.
object TimestampUtils {
  // The formatter we will use.
  private val formatter = java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME

  // We represent the timestamp as java.util.Timestamp in memory.
  //
  // If the timestamp is not in the correct format, this will throw an exception.
  // In the context of predicate evaluation, it will eventually turn off filtering.
  def parse(ts: String): java.sql.Timestamp = {
    new java.sql.Timestamp(java.time.OffsetDateTime.parse(ts, formatter).toInstant.toEpochMilli)
  }
}
