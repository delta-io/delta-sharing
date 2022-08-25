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

import org.apache.hadoop.fs.Path
import org.scalatest.FunSuite

class CloudFileSignerSuite extends FunSuite {

  test("GCSFileSigner.getBucketAndObjectNames") {
    assert(GCSFileSigner.getBucketAndObjectNames(new Path("gs://delta-sharing-test/foo"))
      == ("delta-sharing-test", "foo"))
    assert(GCSFileSigner.getBucketAndObjectNames(new Path("gs://delta_sharing_test/foo"))
      == ("delta_sharing_test", "foo"))
  }
}
