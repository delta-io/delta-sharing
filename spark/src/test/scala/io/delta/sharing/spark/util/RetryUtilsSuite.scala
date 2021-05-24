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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkFunSuite

class RetryUtilsSuite extends SparkFunSuite {
  import RetryUtils._

  test("shouldRetry") {
    assert(shouldRetry(new UnexpectedHttpStatus("error", 429)))
    assert(shouldRetry(new UnexpectedHttpStatus("error", 500)))
    assert(!shouldRetry(new UnexpectedHttpStatus("error", 404)))
    assert(!shouldRetry(new InterruptedException))
    assert(!shouldRetry(new InterruptedIOException))
    assert(shouldRetry(new IOException))
    assert(!shouldRetry(new RuntimeException))
  }

  test("runWithExponentialBackoff") {
    val sleeps = new ArrayBuffer[Long]()
    RetryUtils.sleeper = (sleepMs: Long) => sleeps += sleepMs
    // Retry case
    intercept[UnexpectedHttpStatus] {
      runWithExponentialBackoff(10) {
        throw new UnexpectedHttpStatus("error", 429)
      }
    }
    // Run 11 times should sleep 10 times
    assert(sleeps.length == 10)
    assert(sleeps == Seq(100, 200, 400, 800, 1600, 3200, 6400, 12800, 25600, 51200))
    // No retry case
    sleeps.clear()
    intercept[RuntimeException] {
      runWithExponentialBackoff(10) {
        throw new RuntimeException
      }
    }
    assert(sleeps == Seq())
    RetryUtils.sleeper = (sleepMs: Long) => Thread.sleep(sleepMs)
  }
}
