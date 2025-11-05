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

package io.delta.sharing.spark.perf

import scala.reflect.runtime.universe.{termNames, typeOf, typeTag}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.IntegerLiteral
import org.apache.spark.sql.catalyst.plans.logical.{LocalLimit, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.sources.BaseRelation

import io.delta.sharing.client.util.ConfUtils
import io.delta.sharing.spark.RemoteDeltaSnapshotFileIndex

object DeltaSharingLimitPushDown extends Rule[LogicalPlan] {

  def setup(spark: SparkSession): Unit = synchronized {
    if (!spark.experimental.extraOptimizations.contains(DeltaSharingLimitPushDown) ) {
      spark.experimental.extraOptimizations ++= Seq(DeltaSharingLimitPushDown)
    }
  }

  def apply(p: LogicalPlan): LogicalPlan = {
    if (ConfUtils.limitPushdownEnabled(p.conf)) {
      p transform {
        case localLimit @ LocalLimit(
        literalExpr @ IntegerLiteral(limit),
        l @ LogicalRelationWithTable(
        r @ HadoopFsRelation(remoteIndex: RemoteDeltaSnapshotFileIndex, _, _, _, _, _),
        _)
        ) =>
          if (remoteIndex.limitHint.isEmpty) {
            val spark = SparkSession.active
            LocalLimit(literalExpr,
              LogicalRelationShim.copyWithNewRelation(
                l,
                r.copy(
                  location = remoteIndex.copy(limitHint = Some(limit)))(spark))
            )
          } else {
            localLimit
          }
      }
    } else {
      p
    }
  }
}

/**
 * Extract the [[BaseRelation]] and [[CatalogTable]] from [[LogicalRelation]]. You can also
 * retrieve the instance of LogicalRelation like following:
 *
 * case l @ LogicalRelationWithTable(relation, catalogTable) => ...
 *
 * NOTE: This is copied from Spark 4.0 codebase - license: Apache-2.0.
 */
object LogicalRelationWithTable {
  def unapply(plan: LogicalRelation): Option[(BaseRelation, Option[CatalogTable])] = {
    Some(plan.relation, plan.catalogTable)
  }
}

/**
 * This class helps the codebase to address the differences among multiple Spark versions.
 */
object LogicalRelationShim {
  /**
   * This method provides the ability of copying LogicalRelation instance across Spark versions,
   * when the caller only wants to replace the relation in the LogicalRelation.
   */
  def copyWithNewRelation(src: LogicalRelation, newRelation: BaseRelation): LogicalRelation = {
    // We assume Spark would not change the order of the existing parameter, but it's even safe
    // as long as the first parameter is reserved to the `relation`.
    val paramsForPrimaryConstructor = src.productIterator.toArray
    paramsForPrimaryConstructor(0) = newRelation

    val constructor = typeOf[LogicalRelation]
      .decl(termNames.CONSTRUCTOR)
      // Getting all the constructors
      .alternatives
      .map(_.asMethod)
      // Picking the primary constructor
      .find(_.isPrimaryConstructor)
      // A class must always have a primary constructor, so this is safe
      .get
    val constructorMirror = typeTag[LogicalRelation].mirror
      .reflectClass(typeOf[LogicalRelation].typeSymbol.asClass)
      .reflectConstructor(constructor)

    constructorMirror.apply(paramsForPrimaryConstructor: _*).asInstanceOf[LogicalRelation]
  }
}
