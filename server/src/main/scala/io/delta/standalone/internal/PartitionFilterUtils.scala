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

import scala.util.Try
import scala.util.control.NonFatal

import io.delta.standalone.internal.actions.AddFile
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.analysis.{caseInsensitiveResolution, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.slf4j.LoggerFactory

object PartitionFilterUtils {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private lazy val sqlParser = new SparkSqlParser(new SQLConf)

  def evaluatePredicate(
      schemaString: String,
      partitionColumns: Seq[String],
      partitionFilters: Seq[String],
      addFiles: Seq[(AddFile, Int)]): Seq[(AddFile, Int)] = {
    try {
      val tableSchema = DataType.fromJson(schemaString).asInstanceOf[StructType]
      val partitionSchema = new StructType(partitionColumns.map(c => tableSchema(c)).toArray)
      val addSchema = Encoders.product[AddFile].schema
      val attrs =
        addSchema.map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)())
      val exprs =
        rewritePartitionFilters(
          partitionSchema,
          attrs,
          partitionFilters.flatMap { f =>
            Try(sqlParser.parseExpression(f)).toOption
          }.filter(f => isSupportedExpression(f, partitionSchema))
        )
      if (exprs.isEmpty) {
        addFiles
      } else {
        val predicate = InterpretedPredicate.create(exprs.reduce(And), attrs)
        predicate.initialize(0)
        addFiles.filter {
          case (addFile, _) =>
            val converter = CatalystTypeConverters.createToCatalystConverter(addSchema)
            predicate.eval(converter(addFile).asInstanceOf[InternalRow])
        }
      }
    } catch {
      case NonFatal(e) =>
        logger.error(e.getMessage, e)
        // Fail to evaluate the filters. Return all files as a fallback.
        addFiles
    }
  }

  private def isSupportedExpression(e: Expression, partitionSchema: StructType): Boolean = {
    def isPartitionColumOrConstant(e: Expression): Boolean = {
      e match {
        case _: Literal => true
        case u: UnresolvedAttribute if u.nameParts.size == 1 =>
          val unquoted = u.name.stripPrefix("`").stripSuffix("`")
          partitionSchema.exists(part => caseInsensitiveResolution(unquoted, part.name))
        case c: Cast => isPartitionColumOrConstant(c.child)
        case _ => false
      }
    }

   e match {
      case EqualTo(left, right)
        if isPartitionColumOrConstant(left) && isPartitionColumOrConstant(right) =>
        true
      case GreaterThan(left, right)
        if isPartitionColumOrConstant(left) && isPartitionColumOrConstant(right) =>
        true
      case LessThan(left, right)
        if isPartitionColumOrConstant(left) && isPartitionColumOrConstant(right) =>
        true
      case GreaterThanOrEqual(left, right)
        if isPartitionColumOrConstant(left) && isPartitionColumOrConstant(right) =>
        true
      case LessThanOrEqual(left, right)
        if isPartitionColumOrConstant(left) && isPartitionColumOrConstant(right) =>
        true
      case EqualNullSafe(left, right)
        if isPartitionColumOrConstant(left) && isPartitionColumOrConstant(right) =>
        true
      case IsNull(e) if isPartitionColumOrConstant(e) =>
        true
      case IsNotNull(e) if isPartitionColumOrConstant(e) =>
        true
      case Not(e) if isSupportedExpression(e, partitionSchema) =>
        true
      case _ => false
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
