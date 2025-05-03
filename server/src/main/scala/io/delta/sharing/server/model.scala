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
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import org.codehaus.jackson.annotate.JsonRawValue

import io.delta.sharing.server.common.actions.{DeltaFormat, DeltaMetadata, DeltaProtocol, DeltaSingleAction}

case class SingleAction(
    file: AddFile = null,
    add: AddFileForCDF = null,
    cdf: AddCDCFile = null,
    remove: RemoveFile = null,
    metaData: Metadata = null,
    protocol: Protocol = null,
    queryStatus: QueryStatus = null,
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
    } else if (queryStatus != null) {
      queryStatus
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

case class QueryStatus(
      queryId: String = null
      ) extends  Action {
  override def wrap: SingleAction = SingleAction(queryStatus = this)
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
    minUrlExpirationTimestamp: java.lang.Long,
    endStreamAction: String = null
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
 * A copy of delta Metadata class, removed schema/dataSchema/partitionSchema, is serialized as
 * json response for a delta sharing rpc.
 */
case class DeltaMetadataCopy(
    id: String,
    name: String,
    description: String,
    format: DeltaFormat,
    schemaString: String,
    partitionColumns: Seq[String],
    configuration: Map[String, String],
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    createdTime: Option[Long])

object DeltaMetadataCopy {
  def apply(metadata: DeltaMetadata): DeltaMetadataCopy = {
    DeltaMetadataCopy(
      id = metadata.id,
      name = metadata.name,
      description = metadata.description,
      format = metadata.format,
      schemaString = metadata.schemaString,
      partitionColumns = metadata.partitionColumns,
      configuration = metadata.configuration,
      createdTime = metadata.createdTime
    )
  }
}
/**
 * Actions defined to use in the response for delta format sharing.
 */

sealed trait DeltaResponseAction {
  /** Turn this object to the [[DeltaFormatSingleAction]] wrap object. */
  def wrap: DeltaResponseSingleAction
}

/**
 * DeltaFormatResponseProtocol which is part of the delta Protocol.
 */
case class DeltaFormatResponseProtocol(deltaProtocol: DeltaProtocol) extends DeltaResponseAction {
  override def wrap: DeltaResponseSingleAction = DeltaResponseSingleAction(protocol = this)
}

/**
 * DeltaResponseFileAction used in delta sharing protocol. It wraps a delta action,
 *   and adds 4 delta sharing related fields: id/version/timestamp/expirationTimestamp.
 *       - id: used to uniquely identify a file, and in idToUrl mapping for executor to get
 *             presigned url.
 *       - version/timestamp: the version and timestamp of the commit, used to generate faked delta
 *                            log file on the client side.
 *       - expirationTimestamp: indicate when the presigned url is going to expire and need a
 *                              refresh.
 *   Suggest to redact the tags field before returning if there are sensitive info.
 */
case class DeltaResponseFileAction(
    id: String,
    version: java.lang.Long = null,
    timestamp: java.lang.Long = null,
    expirationTimestamp: Long,
    deltaSingleAction: DeltaSingleAction) extends DeltaResponseAction {
  override def wrap: DeltaResponseSingleAction = DeltaResponseSingleAction(file = this)
}

/**
 * DeltaResponseMetadata used in delta sharing protocol, it wraps the Metadata in delta.
 *   Adding 1 delta sharing related field: version.
 */
case class DeltaResponseMetadata(
    version: java.lang.Long = null,
    deltaMetadata: DeltaMetadataCopy) extends DeltaResponseAction {
  override def wrap: DeltaResponseSingleAction = DeltaResponseSingleAction(metaData = this)
}

/** A serialization helper to create a common action envelope. */
case class DeltaResponseSingleAction(
    file: DeltaResponseFileAction = null,
    metaData: DeltaResponseMetadata = null,
    protocol: DeltaFormatResponseProtocol = null) {

  def unwrap: DeltaResponseAction = {
    if (file != null) {
      file
    } else if (metaData != null) {
      metaData
    } else if (protocol != null) {
      protocol
    } else {
      null
    }
  }
}
