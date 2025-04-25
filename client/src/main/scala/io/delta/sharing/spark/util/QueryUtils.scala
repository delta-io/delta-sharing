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

import java.nio.charset.StandardCharsets.UTF_8

import com.google.common.hash.Hashing

object QueryUtils {

  // Get a query hash id based on the query parameters for snapshot queries
  def getQueryParamsHashId(
      predicates: Seq[String],
      limitHint: Option[Long],
      jsonPredicateHints: Option[String],
      version: Long): String = {
    val predicateStr = predicates.mkString(",")
    val limitStr = limitHint.map(_.toString).getOrElse("none")
    val jsonHintsStr = jsonPredicateHints.getOrElse("none")
    val fullQueryString = s"${predicateStr}_${jsonHintsStr}_${limitStr}_${version}"
    Hashing.sha256().hashString(fullQueryString, UTF_8).toString
  }

  // Get a query hash id based on the query parameters for CDF queries
  def getQueryParamsHashId(cdfOptions: Map[String, String]): String = {
    Hashing.sha256().hashString(cdfOptions.toString, UTF_8).toString
  }

  // Get a query hash id based on the query parameters for streaming queries
  def getQueryParamsHashId(
     startVersion: Long,
     endVersion: Long): String = {
    val fullQueryString = s"${startVersion}_${endVersion}"
    Hashing.sha256().hashString(fullQueryString, UTF_8).toString
  }

  // Add id as a suffix to table path, to uniquely identify a query
  def getTablePathWithIdSuffix(tablePath: String, id: String): String = {
    s"${tablePath}_${id}"
  }
}
