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

import com.ibm.icu.util.VersionInfo.ICU_VERSION
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

        val typeName = leftStr.typeName
        if (typeName.startsWith("string collate")) {
          val collationName = typeName.stripPrefix("string collate").trim
          val provider = if (collationName.equalsIgnoreCase("UTF8_LCASE")) "spark" else "icu"
          val version = s"${ICU_VERSION.getMajor}.${ICU_VERSION.getMinor}"
          Some(s"$provider.$collationName.$version")
        } else {
          None
        }

      case _ =>
        None
    }
  }
}
