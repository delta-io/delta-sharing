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

package io.delta.sharing.spark.util

object DeltaSharingErrors {
  def nonExistentDeltaTable(tabldId: String): Throwable = {
    new IllegalStateException(s"Delta table ${tableId} doesn't exist. " +
      s"Please delete your streaming query checkpoint and restart.")
  }

  def invalidSourceVersion(version: String): Throwable = {
    new IllegalStateException(s"sourceVersion($version) is invalid.")
  }

  def cannotFindSourceVersionException(json: String): Throwable = {
    new IllegalStateException(s"Cannot find 'sourceVersion' in $json")
  }

  def unsupportedTableReaderVersion(supportedVersion: Long, tableVersion: Long): Throwable = {
    new IllegalStateException(s"The table reader version ${tableVersion} is larger than " +
      s"supported reader version $supportedVersion. Please upgrade to a new release."
    )
  }

  def illegalDeltaOptionException(name: String, input: String, explain: String): Throwable = {
    new IllegalArgumentException(s"Invalid value '$input' for option '$name', $explain")
  }

  def startingVersionAndTimestampBothSetException(
    versionOptKey: String,
    timestampOptKey: String): Throwable = {
    new IllegalArgumentException(s"Please either provide '$versionOptKey' or '$timestampOptKey'")
  }

  def CDFNotSupportedInStreaming: Throwable = {
    new UnsupportedOperationException("Delta Sharing Streaming CDF is not supported yet.")
  }
}