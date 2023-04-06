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
import org.slf4j.LoggerFactory

import io.delta.sharing.server.util.JsonUtils

object JsonPredicateFilterUtils {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private val jsonPredicateHintsSizeLimit = 1L * 1024L * 1024L
  private val jsonPredicateMaxTreeDepth = 100

  private var numJsonPredicateErrors = 0
  private var numJsonPredicateErrorsLogged = 0

  private val kMaxNumJsonPredicateErrorsToLog = 20
  private val kMaxNumJsonPredicateErrors = 500

  // Evaluate the specified jsonPredicateHints on the set of add files.
  // Returns the add files that match json predicates.
  def evaluatePredicate(
      jsonPredicateHints: Option[String],
      addFiles: Seq[AddFile]): Seq[AddFile] = {
    if (!jsonPredicateHints.isDefined) {
      return addFiles
    }
    val op = maybeCreateJsonPredicateOp(jsonPredicateHints)
    addFiles.filter(addFile => {
      matchJsonPredicate(op, addFile.partitionValues)
    })
  }

  // Creates a json predicate op from the specified jsonPredicateHints.
  // The op represents the predicate tree to be used to filter files.
  //
  // If there are any errors during op creation or validation, returns a None which
  // implies that we will skip filtering.
  private def maybeCreateJsonPredicateOp(jsonPredicateHints: Option[String]): Option[BaseOp] = {
    try {
      val opJson = jsonPredicateHints.get
      if (opJson.size > jsonPredicateHintsSizeLimit) {
        throw new IllegalArgumentException(
          "The jsonPredicateHints size is " + opJson.size +
          " which exceeds the limit of " + jsonPredicateHintsSizeLimit
        )
      }
      val op = JsonUtils.fromJson[BaseOp](opJson)
      op.validate()
      if (op.treeDepthExceeds(jsonPredicateMaxTreeDepth)) {
        throw new IllegalArgumentException(
          "The jsonPredicate tree depth exceeds the limit, which is " + jsonPredicateMaxTreeDepth
        )
      }
      Some(op)
    } catch {
      // If we encounter any error during op creation or validation, we will record the error
      // and skip filtering.
      case e: Exception =>
        val errStr = "failed to unpack jsonPredicateHints=" + jsonPredicateHints + ", error=" + e
        logger.warn(errStr)
        None
    }
  }

  // Performs json predicate based filtering.
  // If we encounter any errors, the filtering will be skipped.
  private def matchJsonPredicate(
      op: Option[BaseOp],
      partitionValues: Map[String, String]): Boolean = {
    if (op.isEmpty || numJsonPredicateErrors >= kMaxNumJsonPredicateErrors) {
      return true
    }
    try {
      op.get.evalExpectBoolean(EvalContext(partitionValues))
    } catch {
      case e: Exception =>
        numJsonPredicateErrors += 1
        // In order to avoid error explosion in logs, we will only log the first few errors.
        if (numJsonPredicateErrorsLogged < kMaxNumJsonPredicateErrorsToLog) {
          val errStr =
            "failed to evaluate op " + op + " on partition values " + partitionValues + ": " + e
          logger.warn(errStr)
          numJsonPredicateErrorsLogged += 1
        }
        true
    }
  }

}
