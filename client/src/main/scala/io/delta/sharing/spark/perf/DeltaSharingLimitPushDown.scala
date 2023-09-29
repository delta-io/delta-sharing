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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.IntegerLiteral
import org.apache.spark.sql.catalyst.plans.logical.{LocalLimit, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}

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
        l @ LogicalRelation(
        r @ HadoopFsRelation(remoteIndex: RemoteDeltaSnapshotFileIndex, _, _, _, _, _),
        _, _, _)
        ) =>
          if (remoteIndex.limitHint.isEmpty) {
            val spark = SparkSession.active
            LocalLimit(literalExpr,
              l.copy(
                relation = r.copy(
                  location = remoteIndex.copy(limitHint = Some(limit)))(spark)
              )
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
