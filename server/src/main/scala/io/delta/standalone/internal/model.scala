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

import io.delta.standalone.internal.actions.{
  Metadata => DeltaMetadata,
  SingleAction => DeltaSingleAction
}

/**
 * Actions defined in delta format, used in response when requested format is delta.
 */

sealed trait DeltaResponseAction {
  /** Turn this object to the [[DeltaFormatSingleAction]] wrap object. */
  def wrap: DeltaResponseSingleAction
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
case class DeltaResponseProtocol(minReaderVersion: Int) extends DeltaResponseAction {
  override def wrap: DeltaResponseSingleAction = DeltaResponseSingleAction(protocol = this)
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
case class DeltaResponseFileAction(
    id: String,
    version: java.lang.Long = null,
    timestamp: java.lang.Long = null,
    expirationTimestamp: Long,
    deltaAction: DeltaSingleAction) extends DeltaResponseAction {
  override def wrap: DeltaResponseSingleAction = DeltaResponseSingleAction(file = this)
}

/**
 * DeltaResponseMetadata used in delta sharing protocol, copied from Metadata in delta.
 *   Adding 1 delta sharing related field: version.
 *   If the client uses delta kernel, it should redact these fields as needed.
 */
case class DeltaResponseMetadata(
    version: java.lang.Long = null,
    deltaMetadata: DeltaMetadata) extends DeltaResponseAction {
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
