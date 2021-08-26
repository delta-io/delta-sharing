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

package io.delta.sharing.spark

import io.delta.sharing.spark.model.{DeltaTableFiles, DeltaTableMetadata, Metadata, Protocol, SingleAction, Table}
import io.delta.sharing.spark.util.JsonUtils

class TestDeltaSharingClient(
    profileProvider: DeltaSharingProfileProvider = null,
    timeoutInSeconds: Int = 120,
    numRetries: Int = 10,
    sslTrustAll: Boolean = false) extends DeltaSharingClient {

  private val metadataString =
    """{"metaData":{"id":"93351cf1-c931-4326-88f0-d10e29e71b21","format":
      |{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",
      |\"fields\":[{\"name\":\"col1\",\"type\":\"integer\",\"nullable\":true,
      |\"metadata\":{}},{\"name\":\"col2\",\"type\":\"string\",\"nullable\":true,
      |\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1603723967515}}"""
      .stripMargin.replaceAll("\n", "")
  private val metadata = JsonUtils.fromJson[SingleAction](metadataString).metaData

  override def listAllTables(): Seq[Table] = Nil

  override def getMetadata(table: Table): DeltaTableMetadata = {
    DeltaTableMetadata(0, Protocol(0), metadata)
  }

  override def getTableVersion(table: Table): Long = 0

  override def getFiles(
    table: Table,
    predicates: Seq[String],
    limit: Option[Long]): DeltaTableFiles = {
    limit.foreach(lim => TestDeltaSharingClient.limits = TestDeltaSharingClient.limits :+ lim)

    DeltaTableFiles(0, Protocol(0), metadata, Nil)
  }

  def clear(): Unit = {
    TestDeltaSharingClient.limits = Nil
  }
}

object TestDeltaSharingClient {
  var limits = Seq.empty[Long]
}
