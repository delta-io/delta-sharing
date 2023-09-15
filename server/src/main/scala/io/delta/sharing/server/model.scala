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
