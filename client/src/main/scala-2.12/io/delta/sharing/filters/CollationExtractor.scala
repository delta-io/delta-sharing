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
    // The 2.12 client depends on Spark 3.5, which does not support collations.
    // This means we cannot extract the collation identifier. In this case, we
    // return None to default to UTF8 binary comparisons, as collations are just a
    // writer feature in Delta and Spark 3.5 does not support them but should still
    // be able to read the table.
    None
  }
}
