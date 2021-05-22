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

package io.delta.sharing.spark.model

import com.fasterxml.jackson.annotation.JsonInclude
import org.codehaus.jackson.annotate.JsonRawValue

private[sharing] case class DeltaTableMetadata(
    version: Long,
    protocol: Protocol,
    metadata: Metadata)

private[sharing] case class DeltaTableFiles(
    version: Long,
    protocol: Protocol,
    metadata: Metadata,
    files: Seq[AddFile])

private[sharing] case class Share(name: String)

private[sharing] case class Schema(name: String, share: String)

private[sharing] case class Table(name: String, schema: String, share: String)

private[sharing] case class SingleAction(
    file: AddFile = null,
    metaData: Metadata = null,
    protocol: Protocol = null) {

  def unwrap: Action = {
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

private[sharing] case class Format(provider: String = "parquet")

private[sharing] case class Metadata(
    id: String = null,
    name: String = null,
    description: String = null,
    format: Format = Format(),
    schemaString: String = null,
    partitionColumns: Seq[String] = Nil) extends Action {
  override def wrap: SingleAction = SingleAction(metaData = this)
}

private[sharing] sealed trait Action {
  /** Turn this object to the [[SingleAction]] wrap object. */
  def wrap: SingleAction
}

private[sharing] case class Protocol(minReaderVersion: Int) extends Action {
  override def wrap: SingleAction = SingleAction(protocol = this)
}

private[sharing] case class AddFile(
    url: String,
    id: String,
    @JsonInclude(JsonInclude.Include.ALWAYS)
    partitionValues: Map[String, String],
    size: Long,
    @JsonRawValue
    stats: String = null) extends Action {

  override def wrap: SingleAction = SingleAction(file = this)
}
