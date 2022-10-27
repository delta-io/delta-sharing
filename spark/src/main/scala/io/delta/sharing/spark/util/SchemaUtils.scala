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

package io.delta.sharing.spark.util

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.types._


object SchemaUtils extends Logging {

  /**
   * TODO: switch to SchemaUtils in delta-io/connectors once isReadCompatible is supported in it.
   *
   * As the Delta snapshots update, the schema may change as well. This method defines whether the
   * new schema of a Delta table can be used with a previously analyzed LogicalPlan. Our
   * rules are to return false if:
   *   - Dropping or renaming any column that was present in the DataFrame schema
   *   - Converting nullable=false to nullable=true for any column
   *   - Any change of datatype
   */
  def isReadCompatible(existingSchema: StructType, newSchema: StructType): Boolean = {

    def toFieldMap(
      fields: Seq[StructField],
      caseSensitive: Boolean = false): Map[String, StructField] = {
      val fieldMap = fields.map(field => field.name -> field).toMap
      if (caseSensitive) {
        fieldMap
      } else {
        CaseInsensitiveMap(fieldMap)
      }
    }

    def isDatatypeReadCompatible(existingType: DataType, newType: DataType): Boolean = {
      // Recursively check that all data types are read compatible.
      (existingType, newType) match {
        case (e: StructType, n: StructType) =>
          isReadCompatible(e, n)
        case (e: ArrayType, n: ArrayType) =>
          // if existing elements are non-nullable, so should be the new element
          (e.containsNull || !n.containsNull) &&
            isDatatypeReadCompatible(e.elementType, n.elementType)
        case (e: MapType, n: MapType) =>
          // if existing value is non-nullable, so should be the new value
          (e.valueContainsNull || !n.valueContainsNull) &&
            isDatatypeReadCompatible(e.keyType, n.keyType) &&
            isDatatypeReadCompatible(e.valueType, n.valueType)
        case (a, b) => a == b
      }
    }

    def isStructReadCompatible: Boolean = {
      val existingFieldMap = toFieldMap(existingSchema)
      // scalastyle:off caselocale
      val existingFieldNames = existingSchema.fieldNames.map(_.toLowerCase).toSet
      assert(
        existingFieldNames.size == existingSchema.length,
        "Delta tables don't allow field names that only differ by case"
      )
      val newFieldMap = newSchema.fieldNames.map(_.toLowerCase).toSet
      assert(
        newFieldMap.size == newSchema.length,
        "Delta tables don't allow field names that only differ by case"
      )
      // scalastyle:on caselocale

      if (!existingFieldNames.subsetOf(newFieldMap)) {
        // Dropped a column that was present in the DataFrame schema
        return false
      }
      newSchema.forall { newField =>
        // new fields are fine, they just won't be returned
        existingFieldMap.get(newField.name).forall { existingField =>
          // we know the name matches modulo case - now verify exact match
          (existingField.name == newField.name
            // if existingFieldMap value is non-nullable, so should be the new value
            && (existingField.nullable || !newField.nullable)
            // and the type of the field must be compatible, too
            && isDatatypeReadCompatible(existingField.dataType, newField.dataType))
        }
      }
    }

    isStructReadCompatible
  }
}
