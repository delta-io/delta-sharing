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

package io.delta.sharing.client.model

import com.fasterxml.jackson.annotation.{JsonInclude, JsonRawValue}
import org.apache.spark.sql.types.{DataType, LongType, StringType}

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
    protocol: Protocol = null,
    metadata: Metadata = null,
    lines: Seq[String] = Nil,
    respondedFormat: String)

private[sharing] case class DeltaTableFiles(
    version: Long,
    protocol: Protocol = null,
    metadata: Metadata = null,
    files: Seq[AddFile] = Nil,
    addFiles: Seq[AddFileForCDF] = Nil,
    cdfFiles: Seq[AddCDCFile] = Nil,
    removeFiles: Seq[RemoveFile] = Nil,
    additionalMetadatas: Seq[Metadata] = Nil,
    lines: Seq[String] = Nil,
    refreshToken: Option[String] = None,
    respondedFormat: String)

private[sharing] case class Share(name: String)

private[sharing] case class Schema(name: String, share: String)

private[sharing] case class Table(name: String, schema: String, share: String) {
  override def toString(): String = { s"$share.$schema.$name" }
}

private[sharing] case class SingleAction(
    file: AddFile = null,
    add: AddFileForCDF = null,
    cdf: AddCDCFile = null,
    remove: RemoveFile = null,
    metaData: Metadata = null,
    protocol: Protocol = null,
    endStreamAction: EndStreamAction = null) {

  def unwrap: Action = {
    if (file != null) {
      file
    } else if (add != null) {
      add
    } else if (cdf != null) {
      cdf
    } else if (remove != null) {
      remove
    } else if (metaData != null) {
      metaData
    } else if (protocol != null) {
      protocol
    } else if (endStreamAction != null) {
      endStreamAction
    } else {
      null
    }
  }
}

private[sharing] case class Format(provider: String = "parquet")

private[sharing] case class Metadata(
    id: String = null,
    name: String = null,
    description: String = null,
    format: Format = Format(),
    schemaString: String = null,
    configuration: Map[String, String] = Map.empty,
    partitionColumns: Seq[String] = Nil,
    version: java.lang.Long = null,
    size: java.lang.Long = null,
    numFiles: java.lang.Long = null) extends Action {
  override def wrap: SingleAction = SingleAction(metaData = this)
}

private[sharing] sealed trait Action {
  /** Turn this object to the [[SingleAction]] wrap object. */
  def wrap: SingleAction
}

private[sharing] case class Protocol(minReaderVersion: Int) extends Action {
  override def wrap: SingleAction = SingleAction(protocol = this)
}

private[sharing] case class EndStreamAction(
    refreshToken: String,
    nextPageToken: String,
    minUrlExpirationTimestamp: java.lang.Long)
  extends Action {
  override def wrap: SingleAction = SingleAction(endStreamAction = this)
}

// A common base class for all file actions.
private[sharing] sealed abstract class FileAction(
    val url: String,
    val id: String,
    @JsonInclude(JsonInclude.Include.ALWAYS)
    val partitionValues: Map[String, String],
    val size: Long) extends Action {

  // Returns the partition values to be used in a data frame.
  // By default, we return the input partition values.
  // Derived class can override this and add internal partitions values as needed.
  // For example, internal CDF columns such as commit version are modeled as partitions.
  def getPartitionValuesInDF(): Map[String, String] = partitionValues
}

private[sharing] case class AddFile(
    override val url: String,
    override val id: String,
    @JsonInclude(JsonInclude.Include.ALWAYS)
    override val partitionValues: Map[String, String],
    override val size: Long,
    @JsonRawValue
    stats: String = null,
    version: java.lang.Long = null,
    timestamp: java.lang.Long = null,
    expirationTimestamp: java.lang.Long = null) extends FileAction(url, id, partitionValues, size) {

  override def wrap: SingleAction = SingleAction(file = this)
}

private[sharing] case class AddFileForCDF(
    override val url: String,
    override val id: String,
    @JsonInclude(JsonInclude.Include.ALWAYS)
    override val partitionValues: Map[String, String],
    override val size: Long,
    version: Long,
    timestamp: Long,
    @JsonRawValue
    stats: String = null,
    expirationTimestamp: java.lang.Long = null) extends FileAction(url, id, partitionValues, size) {

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
    override val url: String,
    override val id: String,
    @JsonInclude(JsonInclude.Include.ALWAYS)
    override val partitionValues: Map[String, String],
    override val size: Long,
    version: Long,
    timestamp: Long,
    expirationTimestamp: java.lang.Long = null) extends FileAction(url, id, partitionValues, size) {

  override def wrap: SingleAction = SingleAction(cdf = this)

  override def getPartitionValuesInDF(): Map[String, String] = {
    // The scala map operation "+" will override values of existing keys.
    // So the function is idempotent, and calling it multiple times does not change its output.
    partitionValues +
    (CDFColumnInfo.commit_version_col_name -> version.toString) +
    (CDFColumnInfo.commit_timestamp_col_name -> timestamp.toString)
  }
}

private[sharing] case class RemoveFile(
    override val url: String,
    override val id: String,
    @JsonInclude(JsonInclude.Include.ALWAYS)
    override val partitionValues: Map[String, String],
    override val size: Long,
    version: Long,
    timestamp: Long,
    expirationTimestamp: java.lang.Long = null) extends FileAction(url, id, partitionValues, size) {

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
