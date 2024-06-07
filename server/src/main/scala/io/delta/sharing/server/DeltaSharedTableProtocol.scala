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

package io.delta.sharing.server

/**
 *  QueryResult of query and queryCDF function, including a version, a resopnseFormat, and a list
 *  of actions.
 */
case class QueryResult(
                        version: Long,
                        actions: Seq[Object],
                        responseFormat: String)
trait DeltaSharedTableProtocol {
  def getTableVersion(startingTimestamp: Option[String]): Long = -1

  // scalastyle:off argcount
  def query(
             includeFiles: Boolean,
             predicateHints: Seq[String],
             jsonPredicateHints: Option[String],
             limitHint: Option[Long],
             version: Option[Long],
             timestamp: Option[String],
             startingVersion: Option[Long],
             endingVersion: Option[Long],
             maxFiles: Option[Int],
             pageToken: Option[String],
             includeRefreshToken: Boolean,
             refreshToken: Option[String],
             responseFormatSet: Set[String]): QueryResult

  def queryCDF(
                cdfOptions: Map[String, String],
                includeHistoricalMetadata: Boolean = false,
                maxFiles: Option[Int],
                pageToken: Option[String],
                responseFormatSet: Set[String] = Set("parquet")): QueryResult

  def validateTable(inputFullHistoryShared: Boolean): Unit = {}

  def getPartitionSpecLogicalToPhysicalMap(inputFullHistoryShared: Boolean): Map[String, String] =
    Map.empty
}
