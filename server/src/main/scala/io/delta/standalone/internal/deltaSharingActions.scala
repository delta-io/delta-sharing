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

import com.fasterxml.jackson.annotation.JsonInclude
import org.codehaus.jackson.annotate.JsonRawValue

import io.delta.standalone.internal.actions._

private[internal] object DeltaSharingAction {
  /** The maximum version of the protocol that this version of Delta Standalone understands. */
  val readerVersion = 1
  val writerVersion = 2
  val protocolVersion: Protocol = Protocol(readerVersion, writerVersion)

  def fromJson(json: String): Action = {
    JsonUtils.mapper.readValue[DeltaSharingSingleAction](json).unwrap
  }
}

/**
 * Represents a single change to the state of a Delta table. An order sequence
 * of actions can be replayed using [[InMemoryLogReplay]] to derive the state
 * of the table at a given point in time.
 */
private[internal] sealed trait DeltaSharingAction {
  def wrap: DeltaSharingSingleAction

  def json: String = JsonUtils.toJson(wrap)
}

/**
 * Used to block older clients from reading or writing the log when backwards
 * incompatible changes are made to the protocol. Readers and writers are
 * responsible for checking that they meet the minimum versions before performing
 * any other operations.
 *
 * Since this action allows us to explicitly block older clients in the case of a
 * breaking change to the protocol, clients should be tolerant of messages and
 * fields that they do not understand.
 */
private[internal] case class Protocol(
  minReaderVersion: Int = Action.readerVersion,
  minWriterVersion: Int = Action.writerVersion) extends Action {
  override def wrap: DeltaSharingSingleAction = DeltaSharingSingleAction(protocol = this)

  @JsonIgnore
  def simpleString: String = s"($minReaderVersion,$minWriterVersion)"
}

private[internal] object Protocol {
  val MIN_READER_VERSION_PROP = "delta.minReaderVersion"
  val MIN_WRITER_VERSION_PROP = "delta.minWriterVersion"

  def checkMetadataProtocolProperties(metadata: Metadata, protocol: Protocol): Unit = {
    assert(!metadata.configuration.contains(MIN_READER_VERSION_PROP), s"Should not have the " +
      s"protocol version ($MIN_READER_VERSION_PROP) as part of table properties")
    assert(!metadata.configuration.contains(MIN_WRITER_VERSION_PROP), s"Should not have the " +
      s"protocol version ($MIN_WRITER_VERSION_PROP) as part of table properties")
  }
}

/**
 * Adds a new file to the table. When multiple [[AddFile]] file actions
 * are seen with the same `path` only the metadata from the last one is
 * kept.
 */
private[internal] case class DeltaSharingAddFile(
  path: String,
  @JsonInclude(JsonInclude.Include.ALWAYS)
  partitionValues: Map[String, String],
  size: Long,
  modificationTime: Long,
  dataChange: Boolean,
  @JsonRawValue
  stats: String = null,
  version: java.lang.Long = null,
  tags: Map[String, String] = null) extends FileAction {
  require(path.nonEmpty)

  override def wrap: DeltaSingleSingleAction = DeltaSingleSingleAction(add = this)

  def remove: RemoveFile = removeWithTimestamp()

  def removeWithTimestamp(
    timestamp: Long = System.currentTimeMillis(),
    dataChange: Boolean = true): RemoveFile = {
    // scalastyle:off
    RemoveFile(path, Some(timestamp), dataChange)
    // scalastyle:on
  }
}

/**
 * Logical removal of a given file from the reservoir. Acts as a tombstone before a file is
 * deleted permanently.
 *
 * Note that for protocol compatibility reasons, the fields `partitionValues`, `size`, and `tags`
 * are only present when the extendedFileMetadata flag is true. New writers should generally be
 * setting this flag, but old writers (and FSCK) won't, so readers must check this flag before
 * attempting to consume those values.
 */
private[internal] case class DeltaSharingRemoveFile(
  path: String,
  @JsonDeserialize(contentAs = classOf[java.lang.Long])
  deletionTimestamp: Option[Long],
  dataChange: Boolean = true,
  extendedFileMetadata: Boolean = false,
  partitionValues: Map[String, String] = null,
  @JsonDeserialize(contentAs = classOf[java.lang.Long])
  size: Option[Long] = None,
  version: java.lang.Long = null,
  tags: Map[String, String] = null) extends FileAction {
  override def wrap: DeltaSingleSingleAction = DeltaSingleSingleAction(remove = this)

  @JsonIgnore
  val delTimestamp: Long = deletionTimestamp.getOrElse(0L)
}

/**
 * A change file containing CDC data for the Delta version it's within. Non-CDC readers should
 * ignore this, CDC readers should scan all ChangeFiles in a version rather than computing
 * changes from AddFile and RemoveFile actions.
 */
private[internal] case class DeltaSharingAddCDCFile(
  path: String,
  partitionValues: Map[String, String],
  size: Long,
  version: java.lang.Long = null,
  tags: Map[String, String] = null) extends FileAction {
  override val dataChange = false

  override def wrap: DeltaSingleSingleAction = DeltaSingleSingleAction(cdc = this)
}

/**
 * Updates the metadata of the table. Only the last update to the [[Metadata]]
 * of a table is kept. It is the responsibility of the writer to ensure that
 * any data already present in the table is still valid after any change.
 */
private[internal] case class DeltaSharingMetadata(
  id: String,
  name: String = null,
  description: String = null,
  format: Format = Format(),
  schemaString: String = null,
  partitionColumns: Seq[String] = Nil,
  configuration: Map[String, String] = Map.empty,
  version: java.lang.Long = null,
  @JsonDeserialize(contentAs = classOf[java.lang.Long])
  createdTime: Option[Long] = Some(System.currentTimeMillis())) extends Action {

  /** Returns the schema as a [[StructType]] */
  @JsonIgnore
  lazy val schema: StructType =
  Option(schemaString).map { s =>
    DataTypeParser.fromJson(s).asInstanceOf[StructType]
  }.getOrElse(new StructType(Array.empty))

  /** Columns written out to files. */
  @JsonIgnore
  lazy val dataSchema: StructType = {
    val partitions = partitionColumns.toSet
    new StructType(schema.getFields.filterNot(f => partitions.contains(f.getName)))
  }

  /** Returns the partitionSchema as a [[StructType]] */
  @JsonIgnore
  lazy val partitionSchema: StructType =
  new StructType(partitionColumns.map(c => schema.get(c)).toArray)

  override def wrap: DeltaSingleSingleAction = DeltaSingleSingleAction(metaData = this)
}

/** A serialization helper to create a common action envelope. */
private[internal] case class DeltaSharingSingleAction(
  add: DeltaSharingAddFile = null,
  remove: DeltaSharingRemoveFile = null,
  metaData: DeltaSharingMetadata = null,
  protocol: DeltaSharingProtocol = null,
  cdc: DeltaSharingAddCDCFile = null) {


  def unwrap: Action = {
    if (add != null) {
      add
    } else if (remove != null) {
      remove
    } else if (metaData != null) {
      metaData
    } else if (protocol != null) {
      protocol
    } else if (cdc != null) {
      cdc
    } else {
      null
    }
  }
}
