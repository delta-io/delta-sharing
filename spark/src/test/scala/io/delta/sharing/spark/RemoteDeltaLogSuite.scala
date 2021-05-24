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

package io.delta.sharing.spark

import org.apache.spark.SparkFunSuite

class RemoteDeltaLogSuite extends SparkFunSuite {

  test("parsePath") {
    assert(RemoteDeltaLog.parsePath("file:///foo/bar#a.b.c") == ("file:///foo/bar", "a", "b", "c"))
    assert(RemoteDeltaLog.parsePath("file:///foo/bar#bar#a.b.c") ==
      ("file:///foo/bar#bar", "a", "b", "c"))
    assert(RemoteDeltaLog.parsePath("file:///foo/bar#bar#a.b.c ") ==
      ("file:///foo/bar#bar", "a", "b", "c "))
    intercept[IllegalArgumentException] {
      RemoteDeltaLog.parsePath("file:///foo/bar")
    }
    intercept[IllegalArgumentException] {
      RemoteDeltaLog.parsePath("file:///foo/bar#a.b")
    }
    intercept[IllegalArgumentException] {
      RemoteDeltaLog.parsePath("file:///foo/bar#a.b.c.d")
    }
    intercept[IllegalArgumentException] {
      RemoteDeltaLog.parsePath("#a.b.c")
    }
    intercept[IllegalArgumentException] {
      RemoteDeltaLog.parsePath("foo#a.b.")
    }
    intercept[IllegalArgumentException] {
      RemoteDeltaLog.parsePath("foo#a.b.c.")
    }
  }
}
