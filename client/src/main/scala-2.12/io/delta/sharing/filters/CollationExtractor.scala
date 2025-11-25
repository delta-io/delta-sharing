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

package io.delta.sharing.filters

import org.apache.spark.sql.catalyst.expressions.{Expression => SqlExpression}
import org.apache.spark.sql.types.{StringType => SqlStringType}

object CollationExtractor {
  // Extracts collation identifier from two expressions if both are strings
  // with the same collation.
  def extractCollationIdentifier(
      left: SqlExpression,
      right: SqlExpression): Option[String] = {
    (left.dataType, right.dataType) match {
      case (leftStr: SqlStringType, rightStr: SqlStringType) =>
        // Spark needs to make sure to only compare strings of the same collation.
        if (leftStr != rightStr) {
          throw new IllegalArgumentException(
            s"Cannot compare strings with different collations: " +
            s"'${leftStr.typeName}' vs '${rightStr.typeName}'"
          )
        }

        // The 2.12 client depends on Spark 3.5, which does not support collations.
        // This means we cannot extract the collation identifier. In this case, we
        // should throw an error so the filter is not converted. This avoids applying
        // an incorrect filter and ensures we do not return wrong results.
        validateNoCollations(leftStr.typeName)
        None

      case _ =>
        None
    }
  }

  private def validateNoCollations(typeName: String): Unit = {
    if (typeName.startsWith("string collate")) {
      throw new IllegalArgumentException(
        s"Cannot convert operand of unsupported type $typeName"
      )
    }
  }
}
