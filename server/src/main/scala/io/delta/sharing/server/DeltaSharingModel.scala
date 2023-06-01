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

package io.delta.sharing.server.dsmodel

import com.fasterxml.jackson.annotation.JsonInclude
import org.codehaus.jackson.annotate.JsonRawValue

import io.delta.sharing.server.model.Format

/**
 * Represents a single change to the state of a Delta table. An order sequence
 * of actions can be replayed using [[InMemoryLogReplay]] to derive the state
 * of the table at a given point in time.
 */
sealed trait DeltaSharingAction {
  def wrap: DeltaSharingSingleAction
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
case class DeltaSharingProtocol(
    minReaderVersion: Int) extends DeltaSharingAction {
  override def wrap: DeltaSharingSingleAction = DeltaSharingSingleAction(protocol = this)
}

/**
 * Adds a new file to the table. When multiple [[AddFile]] file actions
 * are seen with the same `path` only the metadata from the last one is
 * kept.
 */
case class DeltaSharingAddFile(
    path: String,
    id: String,
    @JsonInclude(JsonInclude.Include.ALWAYS)
    partitionValues: Map[String, String],
    size: Long,
    modificationTime: Long,
    dataChange: Boolean,
    @JsonRawValue
    stats: String = null,
    version: java.lang.Long = null,
    timestamp: java.lang.Long = null,
    expirationTimestamp: Long,
    tags: Map[String, String] = null) extends DeltaSharingAction {
  require(path.nonEmpty)

  override def wrap: DeltaSharingSingleAction = DeltaSharingSingleAction(add = this)
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
case class DeltaSharingRemoveFile(
    path: String,
    id: String,
    deletionTimestamp: Option[Long],
    dataChange: Boolean = true,
    extendedFileMetadata: Boolean = false,
    partitionValues: Map[String, String] = null,
    size: Option[Long] = None,
    version: Long,
    timestamp: Long,
    expirationTimestamp: Long,
    tags: Map[String, String] = null) extends DeltaSharingAction {
  override def wrap: DeltaSharingSingleAction = DeltaSharingSingleAction(remove = this)
}

/**
 * A change file containing CDC data for the Delta version it's within. Non-CDC readers should
 * ignore this, CDC readers should scan all ChangeFiles in a version rather than computing
 * changes from AddFile and RemoveFile actions.
 */
case class DeltaSharingAddCDCFile(
    path: String,
    id: String,
    partitionValues: Map[String, String],
    size: Long,
    version: Long,
    timestamp: Long,
    expirationTimestamp: Long,
    tags: Map[String, String] = null) extends DeltaSharingAction {
  override def wrap: DeltaSharingSingleAction = DeltaSharingSingleAction(cdc = this)
}

/**
 * Updates the metadata of the table. Only the last update to the [[Metadata]]
 * of a table is kept. It is the responsibility of the writer to ensure that
 * any data already present in the table is still valid after any change.
 */
case class DeltaSharingMetadata(
    id: String,
    name: String = null,
    description: String = null,
    format: Format = Format(),
    schemaString: String = null,
    partitionColumns: Seq[String] = Nil,
    configuration: Map[String, String] = Map.empty,
    version: java.lang.Long = null,
    createdTime: Option[Long] = Some(System.currentTimeMillis())) extends DeltaSharingAction {
  override def wrap: DeltaSharingSingleAction = DeltaSharingSingleAction(metaData = this)
}

/** A serialization helper to create a common action envelope. */
case class DeltaSharingSingleAction(
    add: DeltaSharingAddFile = null,
    remove: DeltaSharingRemoveFile = null,
    metaData: DeltaSharingMetadata = null,
    protocol: DeltaSharingProtocol = null,
    cdc: DeltaSharingAddCDCFile = null) {


  def unwrap: DeltaSharingAction = {
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
