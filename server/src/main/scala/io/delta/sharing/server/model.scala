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
    protocol: Protocol = null) {

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
    partitionColumns: Seq[String] = Nil) extends Action {

  override def wrap: SingleAction = SingleAction(metaData = this)
}

sealed trait Action {
  /** Turn this object to the [[SingleAction]] wrap object. */
  def wrap: SingleAction
}

case class Protocol(minReaderVersion: Int) extends Action {
  override def wrap: SingleAction = SingleAction(protocol = this)
}

sealed abstract class AddFileBase(
    url: String,
    id: String,
    @JsonInclude(JsonInclude.Include.ALWAYS)
    partitionValues: Map[String, String],
    size: Long,
    @JsonRawValue
    stats: String = null)
    extends Action {}

case class AddFile(
    url: String,
    id: String,
    @JsonInclude(JsonInclude.Include.ALWAYS)
    partitionValues: Map[String, String],
    size: Long,
    @JsonRawValue
    stats: String = null) extends AddFileBase(url, id, partitionValues, size, stats) {

  override def wrap: SingleAction = SingleAction(file = this)
}

case class AddFileForCDF(
    url: String,
    id: String,
    @JsonInclude(JsonInclude.Include.ALWAYS)
    partitionValues: Map[String, String],
    size: Long,
    version: Long,
    timestamp: Long,
    @JsonRawValue
    stats: String = null)
    extends AddFileBase(url, id, partitionValues, size, stats) {

  override def wrap: SingleAction = SingleAction(add = this)
}

case class AddCDCFile(
    url: String,
    id: String,
    @JsonInclude(JsonInclude.Include.ALWAYS)
    partitionValues: Map[String, String],
    size: Long,
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
    timestamp: Long,
    version: Long)
    extends Action {

  override def wrap: SingleAction = SingleAction(remove = this)
}
