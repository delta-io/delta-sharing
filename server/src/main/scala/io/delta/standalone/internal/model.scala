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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import io.delta.standalone.internal.actions.{
  Format => DeltaFormat,
  Metadata => DeltaMetadata,
  Protocol => DeltaProtocol,
  SingleAction => DeltaSingleAction
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
    createdTime: Option[Long]
)

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
 * DeltaResponseProtocol which is part of the delta Protocol.
 */
case class DeltaResponseProtocol(deltaProtocol: DeltaProtocol) extends DeltaResponseAction {
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
    protocol: DeltaResponseProtocol = null) {

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
