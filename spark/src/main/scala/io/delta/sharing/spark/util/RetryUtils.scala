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

import java.io.{InterruptedIOException, IOException}

import scala.util.control.NonFatal

import org.apache.spark.internal.Logging

private[sharing] object RetryUtils extends Logging {

  // Expose it for testing
  @volatile var sleeper: Long => Unit = (sleepMs: Long) => Thread.sleep(sleepMs)

  def runWithExponentialBackoff[T](numRetries: Int)(func: => T): T = {
    var times = 0
    var sleepMs = 100
    while (true) {
      times += 1
      try {
        return func
      } catch {
        case NonFatal(e) if shouldRetry(e) && times <= numRetries =>
          logWarning(s"Sleeping $sleepMs ms to retry because of error: ${e.getMessage}", e)
          sleeper(sleepMs)
          sleepMs *= 2
      }
    }
    throw new IllegalStateException("Should not happen")
  }

  def shouldRetry(t: Throwable): Boolean = {
    t match {
      case e: UnexpectedHttpStatus =>
        if (e.statusCode == 429) { // Too Many Requests
          true
        } else if (e.statusCode >= 500 && e.statusCode < 600) { // Internal Error
          true
        } else {
          false
        }
      case _: InterruptedException => false
      case _: InterruptedIOException => false
      case _: IOException => true
      case _ => false
    }
  }
}

private[sharing] class UnexpectedHttpStatus(message: String, val statusCode: Int)
  extends IllegalStateException(message)
