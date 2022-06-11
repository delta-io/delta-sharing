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

import org.apache.spark.sql.catalyst.expressions.{AttributeMap, AttributeReference}
import org.apache.spark.sql.sources.BaseRelation

/**
 * Used to link a [[BaseRelation]] in to a logical query plan.
 * Currently mainly support cdf query.
 */
case class RemoteLogicalRelation(
    path: String,
    cdfOptions: Map[String, String]) {
  val relation: BaseRelation = getCDFRelation

  val output: Seq[AttributeReference] = getOutputAttributeReference

  private def getCDFRelation: BaseRelation = {
    val deltaLog = RemoteDeltaLog(path)
    deltaLog.createRelation(None, cdfOptions)
  }

  private def getOutputAttributeReference: Seq[AttributeReference] = {
    relation.asInstanceOf[RemoteDeltaCDFRelation].schema.map(
      f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)())
  }
}
