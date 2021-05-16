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

import io.delta.standalone.internal.actions.AddFile
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Cast, Expression, ExtractValue, InterpretedPredicate, Literal}
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, MapType, StringType, StructField, StructType}

object ParserUtils {
  val sqlParser = new SparkSqlParser(new SQLConf)

  def evaluatePredicate(
      partitionSchema: StructType,
      partitionFilter: String,
      addFiles: Seq[AddFile]): Seq[AddFile] = {
    val addSchema = Encoders.product[AddFile].schema
    val attrs = addSchema.map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)())
    val expr = sqlParser.parseExpression(partitionFilter)
    val p = rewritePartitionFilters(partitionSchema, attrs, expr :: Nil).head
    val predicate = InterpretedPredicate.create(p, attrs)
    predicate.initialize(0)
    addFiles.filter { addFile =>
      val converter = CatalystTypeConverters.createToCatalystConverter(addSchema)
      predicate.eval(converter(addFile).asInstanceOf[InternalRow])
    }
  }

  private def rewritePartitionFilters(
    partitionSchema: StructType,
    attrs: Seq[Attribute],
    partitionFilters: Seq[Expression]): Seq[Expression] = {
    val partitionValuesAttr = attrs.find(_.name == "partitionValues").head
    partitionFilters.map(_.transformUp {
      case a: Attribute =>
        // If we have a special column name, e.g. `a.a`, then an UnresolvedAttribute returns
        // the column name as '`a.a`' instead of 'a.a', therefore we need to strip the backticks.
        val unquoted = a.name.stripPrefix("`").stripSuffix("`")
        val partitionCol = partitionSchema.find { field => field.name == unquoted }
        partitionCol match {
          case Some(StructField(name, dataType, _, _)) =>
            Cast(
              ExtractValue(
                partitionValuesAttr,
                Literal(name),
                org.apache.spark.sql.catalyst.analysis.caseInsensitiveResolution),
              dataType)
          case None =>
            // This should not be able to happen, but the case was present in the original code so
            // we kept it to be safe.
            UnresolvedAttribute(Seq("partitionValues", a.name))
        }
    })
  }
}
