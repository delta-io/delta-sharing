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

package io.delta.sharing.server.model

import com.fasterxml.jackson.annotation.JsonInclude
import org.codehaus.jackson.annotate.JsonRawValue

case class SingleAction(
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

case class Format(provider: String = "parquet")

case class Metadata(
    id: String = null,
    name: String = null,
    description: String = null,
    format: Format = Format(),
    schemaString: String = null,
    configuration: Map[String, String] = Map.empty,
    partitionColumns: Seq[String] = Nil,
    version: java.lang.Long = null) extends Action {

  override def wrap: SingleAction = SingleAction(metaData = this)
}

sealed trait Action {
  /** Turn this object to the [[SingleAction]] wrap object. */
  def wrap: SingleAction
}

case class Protocol(minReaderVersion: Int) extends Action {
  override def wrap: SingleAction = SingleAction(protocol = this)
}

case class AddFile(
    url: String,
    id: String,
    @JsonInclude(JsonInclude.Include.ALWAYS)
    partitionValues: Map[String, String],
    size: Long,
    @JsonRawValue
    stats: String = null,
    expirationTimestamp: java.lang.Long = null,
    timestamp: java.lang.Long = null,
    version: java.lang.Long = null) extends Action {

  override def wrap: SingleAction = SingleAction(file = this)
}

// This was added because when we develop cdf support in delta sharing, AddFile is used with "file"
// key in the response json, so we need another action to be used with "add".
case class AddFileForCDF(
    url: String,
    id: String,
    @JsonInclude(JsonInclude.Include.ALWAYS)
    partitionValues: Map[String, String],
    size: Long,
    expirationTimestamp: java.lang.Long = null,
    version: Long,
    timestamp: Long,
    @JsonRawValue
    stats: String = null) extends Action {

  override def wrap: SingleAction = SingleAction(add = this)
}

case class AddCDCFile(
    url: String,
    id: String,
    @JsonInclude(JsonInclude.Include.ALWAYS)
    partitionValues: Map[String, String],
    size: Long,
    expirationTimestamp: java.lang.Long = null,
    timestamp: Long,
    version: Long)
    extends Action {

  override def wrap: SingleAction = SingleAction(cdf = this)
}

case class RemoveFile(
    url: String,
    id: String,
    @JsonInclude(JsonInclude.Include.ALWAYS)
    partitionValues: Map[String, String],
    size: Long,
    expirationTimestamp: java.lang.Long = null,
    timestamp: Long,
    version: Long)
    extends Action {

  override def wrap: SingleAction = SingleAction(remove = this)
}

/**
 * An action that is returned as the last line of the streaming response. It allows the server
 * to include additional data that might be dynamically generated while the streaming message
 * is sent, such as:
 *  - refreshToken: a token used to refresh pre-signed urls for a long running query
 *  - nextPageToken: a token used to retrieve the subsequent page of a query
 *  - minUrlExpirationTimestamp: the minimum url expiration timestamp of the urls returned in
 *    current response
 */
case class EndStreamAction(
    refreshToken: String,
    nextPageToken: String,
    minUrlExpirationTimestamp: java.lang.Long
  ) extends Action {
  override def wrap: SingleAction = SingleAction(endStreamAction = this)
}

object Action {
  // The maximum version of the protocol that this version of Delta Standalone understands.
  val maxReaderVersion = 1
  // The maximum writer version that this version of Delta Sharing Standalone supports.
  // Basically delta sharing doesn't support write for now.
  val maxWriterVersion = 0
}

/**
 * Actions defined in delta format, used in response when requested format is delta.
 */

sealed trait DeltaAction {
  /** Turn this object to the [[DeltaSingleAction]] wrap object. */
  def wrap: DeltaSingleAction
}

/**
 * Used to block older clients from reading the shared table when backwards
 * incompatible changes are made to the protocol. Readers and writers are
 * responsible for checking that they meet the minimum versions before performing
 * any other operations.
 *
 * Since this action allows us to explicitly block older clients in the case of a
 * breaking change to the protocol, clients should be tolerant of messages and
 * fields that they do not understand.
 */
case class DeltaProtocol(minReaderVersion: Int) extends DeltaAction {
  override def wrap: DeltaSingleAction = DeltaSingleAction(protocol = this)
}

/**
 * DeltaAddFile used in delta sharing protocol, copied from AddFile in delta.
 *   Adding 4 delta sharing related fields: id/version/timestamp/expirationTimestamp.
 *   If the client uses delta kernel, it should redact these fields as needed.
 *       - id: used to uniquely identify a file, and in idToUrl mapping for executor to get
 *             presigned url.
 *       - version/timestamp: the version and timestamp of the commit, used to generate faked delta
 *                            log file on the client side.
 *       - expirationTimestamp: indicate when the presigned url is going to expire and need a
 *                              refresh.
 *   Ignoring 1 field: tags.
 */
case class DeltaAddFile(
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
    expirationTimestamp: Long) extends DeltaAction {
  require(path.nonEmpty)

  override def wrap: DeltaSingleAction = DeltaSingleAction(add = this)
}

/**
 * DeltaRemoveFile used in delta sharing protocol, copied from RemoveFile in delta.
 *   Adding 4 delta sharing related fields: id/version/timestamp/expirationTimestamp.
 *   If the client uses delta kernel, it should redact these fields as needed.
 *   Ignoring 1 field: tags.
 */
case class DeltaRemoveFile(
    path: String,
    id: String,
    deletionTimestamp: Option[Long],
    dataChange: Boolean = true,
    extendedFileMetadata: Boolean = false,
    partitionValues: Map[String, String] = null,
    size: Option[Long] = None,
    version: Long,
    timestamp: Long,
    expirationTimestamp: Long) extends DeltaAction {
  override def wrap: DeltaSingleAction = DeltaSingleAction(remove = this)
}

/**
 * DeltaAddCDCFile used in delta sharing protocol, copied from AddCDCFile in delta.
 *   Adding 4 delta sharing related fields: id/version/timestamp/expirationTimestamp.
 *   If the client uses delta kernel, it should redact these fields as needed.
 *   Ignoring 1 field: tags.
 */
case class DeltaAddCDCFile(
    path: String,
    id: String,
    partitionValues: Map[String, String],
    size: Long,
    version: Long,
    timestamp: Long,
    expirationTimestamp: Long) extends DeltaAction {
  override def wrap: DeltaSingleAction = DeltaSingleAction(cdc = this)
}

/**
 * DeltaMetadata used in delta sharing protocol, copied from Metadata in delta.
 *   Adding 1 delta sharing related field: version.
 *   If the client uses delta kernel, it should redact these fields as needed.
 */
case class DeltaMetadata(
    id: String,
    name: String = null,
    description: String = null,
    format: Format = Format(),
    schemaString: String = null,
    partitionColumns: Seq[String] = Nil,
    configuration: Map[String, String] = Map.empty,
    version: java.lang.Long = null,
    createdTime: Option[Long] = Some(System.currentTimeMillis())) extends DeltaAction {
  override def wrap: DeltaSingleAction = DeltaSingleAction(metaData = this)
}

/** A serialization helper to create a common action envelope. */
case class DeltaSingleAction(
    add: DeltaAddFile = null,
    remove: DeltaRemoveFile = null,
    metaData: DeltaMetadata = null,
    protocol: DeltaProtocol = null,
    cdc: DeltaAddCDCFile = null) {

  def unwrap: DeltaAction = {
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
