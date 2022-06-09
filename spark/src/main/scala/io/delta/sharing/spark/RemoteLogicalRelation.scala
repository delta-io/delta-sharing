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

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions.{AttributeMap, AttributeReference}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan, Statistics}
import org.apache.spark.sql.catalyst.util.{truncatedString, CharVarcharUtils}
import org.apache.spark.sql.sources.BaseRelation

/**
 * Used to link a [[BaseRelation]] in to a logical query plan.
 * Currently mainly support cdf query.
 */
case class RemoteLogicalRelation(
    path: String,
    cdfOptions: Map[String, String],
    override val isStreaming: Boolean)
  extends LeafNode with MultiInstanceRelation {

  lazy val relation: BaseRelation = getCDFRelation

  lazy val output: Seq[AttributeReference] = getOutputAttributeReference

  // Only care about relation when canonicalizing.
  override def doCanonicalize(): LogicalPlan = this

  override def computeStats(): Statistics = Statistics(sizeInBytes = relation.sizeInBytes)

  /** Used to lookup original attribute capitalization */
  lazy val attributeMap: AttributeMap[AttributeReference] = AttributeMap(output.map(o => (o, o)))

  /**
   * Returns a new instance of this RemoteLogicalRelation. According to the semantics of
   * MultiInstanceRelation, this method returns a copy of this object with
   * unique expression ids. We respect the `expectedOutputAttributes` and create
   * new instances of attributes in it.
   */
  override def newInstance(): RemoteLogicalRelation = this.copy(
    path = path, cdfOptions = cdfOptions)

  override def simpleString(maxFields: Int): String = {
    val table = relation.asInstanceOf[RemoteDeltaCDFRelation].table
    s"Relation ${table.schema}.${table.name}" +
    s"[${truncatedString(output, ",", maxFields)}] $relation"
  }

  override lazy val metadataOutput: Seq[AttributeReference] = Nil

  private def getCDFRelation: BaseRelation = {
    val deltaLog = RemoteDeltaLog(path)
    deltaLog.createRelation(None, cdfOptions)
  }

  private def getOutputAttributeReference: Seq[AttributeReference] = {
    relation.asInstanceOf[RemoteDeltaCDFRelation].schema.map(
      f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)())
  }
}
