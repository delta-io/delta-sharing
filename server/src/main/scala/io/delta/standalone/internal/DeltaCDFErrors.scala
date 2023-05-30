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

class DeltaCDFIllegalArgumentException(message: String)
  extends IllegalArgumentException(message)

object DeltaCDFErrors {
  def multipleCDFBoundary(position: String): Throwable = {
    new DeltaCDFIllegalArgumentException(s"Multiple $position arguments provided for CDF read. " +
      s"Please provide one of either ${position}Timestamp or ${position}Version."
    )
  }

  def noStartVersionForCDF: Throwable = {
    new DeltaCDFIllegalArgumentException("No startingVersion or startingTimestamp provided for " +
      "CDF read.")
  }

  def startVersionAfterLatestVersion(start: Long, latest: Long): Throwable = {
    new DeltaCDFIllegalArgumentException(s"Provided Start version($start) for reading change " +
      "data is invalid. Start version cannot be greater than the latest version of the " +
      s"table($latest)."
    )
  }

  def endVersionAfterLatestVersion(end: Long, latest: Long): Throwable = {
    new DeltaCDFIllegalArgumentException(s"Provided end version($end) is invalid. End version " +
      s"cannot be greater than the latest version of the table($latest)."
    )
  }

  def endBeforeStartVersionInCDF(start: Long, end: Long): Throwable = {
    new DeltaCDFIllegalArgumentException(
      s"CDF range from start $start to end $end was invalid. End cannot be before start."
    )
  }

  def invalidTimestamp(field: String, message: String): Throwable = {
    new DeltaCDFIllegalArgumentException(s"Invalid $field: $message")
  }

  def changeDataNotRecordedException(version: Long, start: Long, end: Long): Throwable = {
    new DeltaCDFIllegalArgumentException(s"Error getting change data for range [$start, $end] " +
      s"as change data was not recorded for version [$version]"
    )
  }
}
