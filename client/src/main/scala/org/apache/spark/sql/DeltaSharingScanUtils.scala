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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.StructType

object DeltaSharingScanUtils {
  // Create a DataFrame from a LogicalRelation using public APIs
  def ofRows(spark: SparkSession, plan: LogicalRelation): DataFrame = {
    // In Spark 4.0.0, we need to use the relation's data to create the DataFrame
    val rdd = plan.relation.asInstanceOf[org.apache.spark.sql.sources.TableScan].buildScan()
    spark.createDataFrame(rdd, plan.relation.schema)
  }

  // Create a Column from an Expression using public APIs
  def toColumn(expr: Expression): Column = {
    // In Spark 4.0.0, we need to use the internal Column constructor
    new Column(expr.asInstanceOf[org.apache.spark.sql.internal.ColumnNode])
  }
}
