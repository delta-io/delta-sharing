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

package io.delta.sharing.spark.dsmodel

import com.fasterxml.jackson.annotation.JsonInclude
import org.apache.spark.sql.types.{DataType, LongType, StringType}
import org.codehaus.jackson.annotate.JsonRawValue

import io.delta.sharing.spark.model.Format

// Information about CDF columns.
private[sharing] object CDFColumnInfo {
  // Internal CDF column names.
  val commit_version_col_name = "_commit_version"
  val commit_timestamp_col_name = "_commit_timestamp"
  val change_type_col_name = "_change_type"

  // Returns internal partition schema for internal columns for CDC actions.
  def getInternalPartitonSchemaForCDC(): Map[String, DataType] =
    Map(commit_version_col_name -> LongType, commit_timestamp_col_name -> LongType)

  // Returns internal partition schema for internal columns for CDF add/remove actions.
  def getInternalPartitonSchemaForCDFAddRemoveFile(): Map[String, DataType] =
    getInternalPartitonSchemaForCDC() + (change_type_col_name -> StringType)
}

private[sharing] case class DeltaTableMetadata(
  version: Long,
  protocol: Protocol,
  metadata: Metadata)

private[sharing] case class DeltaTableFiles(
  version: Long,
  protocol: Protocol,
  metadata: Metadata,
  addFiles: Seq[AddFile] = Nil,
  cdfFiles: Seq[AddCDCFile] = Nil,
  removeFiles: Seq[RemoveFile] = Nil,
  additionalMetadatas: Seq[Metadata] = Nil)

private[sharing] case class Share(name: String)

private[sharing] case class Schema(name: String, share: String)

private[sharing] case class Table(name: String, schema: String, share: String) {
  override def toString(): String = { s"$share.$schema.$name" }
}

private[sharing] case class SingleAction(
  add: AddFile = null,
  cdc: AddCDCFile = null,
  remove: RemoveFile = null,
  metaData: Metadata = null,
  protocol: Protocol = null) {

  def unwrap: Action = {
    if (add != null) {
      add
    } else if (cdc != null) {
      cdc
    } else if (remove != null) {
      remove
    } else if (metaData != null) {
      metaData
    } else if (protocol != null) {
      protocol
    } else {
      null
    }
  }
}

private[sharing] case class Metadata(
    id: String = null,
    name: String = null,
    description: String = null,
    format: Format = Format(),
    schemaString: String = null,
    partitionColumns: Seq[String] = Nil,
    configuration: Map[String, String] = Map.empty,
    version: java.lang.Long = null,
    size: java.lang.Long = null,
    numFiles: java.lang.Long = null,
    createdTime: Option[Long] = None) extends Action {
  override def wrap: SingleAction = SingleAction(metaData = this)
}

private[sharing] sealed trait Action {
  /** Turn this object to the [[SingleAction]] wrap object. */
  def wrap: SingleAction
}

private[sharing] case class Protocol(minReaderVersion: Int) extends Action {
  override def wrap: SingleAction = SingleAction(protocol = this)
}

// A common base class for all file actions.
private[sharing] sealed abstract class FileAction(
  val path: String,
  val id: String,
  @JsonInclude(JsonInclude.Include.ALWAYS)
  val partitionValues: Map[String, String],
  val size: Long,
  val version: Long,
  val timestamp: Long,
  val expirationTimestamp: Long) extends Action {

  // Returns the partition values to be used in a data frame.
  // By default, we return the input partition values.
  // Derived class can override this and add internal partitions values as needed.
  // For example, internal CDF columns such as commit version are modeled as partitions.
  def getPartitionValuesInDF(): Map[String, String] = partitionValues
}

private[sharing] case class AddFile(
    override val path: String,
    override val id: String,
    @JsonInclude(JsonInclude.Include.ALWAYS)
    override val partitionValues: Map[String, String],
    override val size: Long,
    override val version: Long,
    override val timestamp: Long,
    override val expirationTimestamp: Long,
    modificationTime: Long,
    dataChange: Boolean,
    @JsonRawValue
    stats: String = null,
    tags: Map[String, String] = null) extends
  FileAction(path, id, partitionValues, size, version, timestamp, expirationTimestamp) {

  override def wrap: SingleAction = SingleAction(add = this)

  override def getPartitionValuesInDF(): Map[String, String] = {
    // The scala map operation "+" will override values of existing keys.
    // So the function is idempotent, and calling it multiple times does not change its output.
    partitionValues +
      (CDFColumnInfo.commit_version_col_name -> version.toString) +
      (CDFColumnInfo.commit_timestamp_col_name -> timestamp.toString) +
      (CDFColumnInfo.change_type_col_name -> "insert")
  }
}

private[sharing] case class AddCDCFile(
    override val path: String,
    override val id: String,
    @JsonInclude(JsonInclude.Include.ALWAYS)
    override val partitionValues: Map[String, String],
    override val size: Long,
    override val version: Long,
    override val timestamp: Long,
    override val expirationTimestamp: Long,
    tags: Map[String, String] = null) extends
  FileAction(path, id, partitionValues, size, version, timestamp, expirationTimestamp) {

  override def wrap: SingleAction = SingleAction(cdc = this)

  override def getPartitionValuesInDF(): Map[String, String] = {
    // The scala map operation "+" will override values of existing keys.
    // So the function is idempotent, and calling it multiple times does not change its output.
    partitionValues +
      (CDFColumnInfo.commit_version_col_name -> version.toString) +
      (CDFColumnInfo.commit_timestamp_col_name -> timestamp.toString)
  }
}

private[sharing] case class RemoveFile(
    override val path: String,
    override val id: String,
    @JsonInclude(JsonInclude.Include.ALWAYS)
    override val partitionValues: Map[String, String],
    override val size: Long,
    override val version: Long,
    override val timestamp: Long,
    override val expirationTimestamp: Long,
    deletionTimestamp: Option[Long],
    dataChange: Boolean,
    extendedFileMetadata: Boolean,
    tags: Map[String, String] = null) extends
  FileAction(path, id, partitionValues, size, version, timestamp, expirationTimestamp) {

  override def wrap: SingleAction = SingleAction(remove = this)

  override def getPartitionValuesInDF(): Map[String, String] = {
    // The scala map operation "+" will override values of existing keys.
    // So the function is idempotent, and calling it multiple times does not change its output.
    partitionValues +
      (CDFColumnInfo.commit_version_col_name -> version.toString) +
      (CDFColumnInfo.commit_timestamp_col_name -> timestamp.toString) +
      (CDFColumnInfo.change_type_col_name -> "delete")
  }
}
