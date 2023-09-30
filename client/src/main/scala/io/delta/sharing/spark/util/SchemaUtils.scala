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
   * new schema(schemaToCheck) of a Delta table can be used with a previously analyzed LogicalPlan
   * using the readSchema.
   * Our rules are to return false if:
   *   - Dropping or renaming any column that was present in the DataFrame schema
   *   - Converting nullable=false to nullable=true for any column
   *   - Any change of datatype
   */
  def isReadCompatible(schemaToCheck: StructType, readSchema: StructType): Boolean = {

    def toFieldMap(fields: Seq[StructField]): Map[String, StructField] = {
      CaseInsensitiveMap(fields.map(field => field.name -> field).toMap)
    }

    def isDatatypeReadCompatible(toCheckType: DataType, readType: DataType): Boolean = {
      // Recursively check that all data types are read compatible.
      (toCheckType, readType) match {
        case (t: StructType, r: StructType) =>
          isReadCompatible(t, r)
        case (t: ArrayType, r: ArrayType) =>
          // if the read elements are non-nullable, so should be the new element
          (!t.containsNull || r.containsNull) &&
            isDatatypeReadCompatible(t.elementType, r.elementType)
        case (t: MapType, r: MapType) =>
          // if the read value is non-nullable, so should be the new value
          (!t.valueContainsNull || r.valueContainsNull) &&
            isDatatypeReadCompatible(t.keyType, r.keyType) &&
            isDatatypeReadCompatible(t.valueType, r.valueType)
        case (a, b) => a == b
      }
    }

    def isStructReadCompatible: Boolean = {
      val toCheckFieldMap = toFieldMap(schemaToCheck)
      // scalastyle:off caselocale
      val toCheckFieldNames = schemaToCheck.fieldNames.map(_.toLowerCase).toSet
      assert(
        toCheckFieldNames.size == schemaToCheck.length,
        "Delta tables don't allow field names that only differ by case"
      )
      val readFieldMap = readSchema.fieldNames.map(_.toLowerCase).toSet
      assert(
        readFieldMap.size == readSchema.length,
        "Delta tables don't allow field names that only differ by case"
      )
      // scalastyle:on caselocale

      if (!toCheckFieldNames.subsetOf(readFieldMap)) {
        // Dropped a column that was present in the DataFrame schema
        return false
      }
      readSchema.forall { readField =>
        // new fields are fine, they just won't be returned
        toCheckFieldMap.get(readField.name).forall { toCheckField =>
          // we know the name matches modulo case - now verify exact match
          (toCheckField.name == readField.name
            // if toCheckFieldMap value is non-nullable, so should be the new value
            && (!toCheckField.nullable || readField.nullable)
            // and the type of the field must be compatible, too
            && isDatatypeReadCompatible(toCheckField.dataType, readField.dataType))
        }
      }
    }

    isStructReadCompatible
  }
}
