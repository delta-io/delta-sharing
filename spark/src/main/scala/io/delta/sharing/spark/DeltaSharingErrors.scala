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

package io.delta.sharing.spark

import org.apache.spark.sql.types.StructType

object DeltaSharingErrors {
  def nonExistentDeltaSharingTable(tableId: String): Throwable = {
    new IllegalStateException(s"Delta sharing table ${tableId} doesn't exist. " +
      s"Please delete your streaming query checkpoint and restart.")
  }

  def invalidSourceVersion(version: String): Throwable = {
    new IllegalStateException(s"sourceVersion($version) is invalid.")
  }

  def timestampInvalid(str: String): Throwable = {
    new IllegalArgumentException(s"The provided timestamp ($str) cannot be converted to a valid " +
      s"timestamp.")
  }

  def cannotFindSourceVersionException(json: String): Throwable = {
    new IllegalStateException(s"Cannot find 'sourceVersion' in $json")
  }

  def unsupportedTableReaderVersion(supportedVersion: Long, tableVersion: Long): Throwable = {
    new IllegalStateException(s"The table reader version ${tableVersion} is larger than " +
      s"supported reader version $supportedVersion. Please upgrade to a new release."
    )
  }

  def illegalDeltaSharingOptionException(
      name: String, input: String, explain: String): Throwable = {
    new IllegalArgumentException(s"Invalid value '$input' for option '$name', $explain")
  }

  def versionAndTimestampBothSetException(
    versionOptKey: String,
    timestampOptKey: String): Throwable = {
    new IllegalArgumentException(s"Please either provide '$versionOptKey' or '$timestampOptKey'.")
  }

  def deltaSharingSourceIgnoreDeleteError(version: Long): Throwable = {
    new UnsupportedOperationException("Detected deleted data from streaming source at version " +
      s"$version. This is currently not supported. If you'd like to ignore deletes, set the " +
      s"option 'ignoreDeletes' to 'true'.")
  }

  def deltaSharingSourceIgnoreChangesError(version: Long): Throwable = {
    new UnsupportedOperationException("Detected a data update in the source table at version " +
      s"$version. This is currently not supported. If you'd like to ignore updates, set the " +
      s"option 'skipChangeCommits' to 'true'. If you would like the data update to be reflected, " +
      s"please restart the query from latest snapshot with a fresh checkpoint directory.")
  }

  def unknownReadLimit(limit: String): Throwable = {
    new UnsupportedOperationException(s"Unknown ReadLimit: $limit")
  }

  def specifySchemaAtReadTimeException: Throwable = {
    new UnsupportedOperationException("Delta sharing does not support specifying the schema at " +
      "read time.")
  }

  def pathNotSpecifiedException: Throwable = {
    new IllegalArgumentException("'path' is not specified. If you use SQL to create a Delta " +
      "Sharing table, LOCATION must be specified")
  }

  def timeTravelNotSupportedException: Throwable = {
    new UnsupportedOperationException("Cannot time travel streams.")
  }

  def schemaNotSetException: Throwable = {
    new IllegalStateException("Shared table schema is not set. Please contact your data provider.")
  }

  def schemaChangedException(readSchema: StructType, schemaToCheck: StructType): Throwable = {
    val msg =
      s"""Detected incompatible schema change:
         |schema used to read data: ${readSchema.treeString}
         |
         |schema seen in the table: ${schemaToCheck.treeString}
         |
         |Please try restarting the query. If this issue repeats across query restarts without
         |making progress, you have made an incompatible schema change and need to start your
         |query from scratch using a new checkpoint directory.
      """.stripMargin
    new IllegalStateException(msg)
  }
}
