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

package io.delta.sharing.server.credential.aws

import java.net.URI

import org.scalatest.FunSuite

import io.delta.sharing.server.credential.Privilege

class AwsPolicyGeneratorSuite extends FunSuite {

  test("sessionPolicy contains bucket and resourcePrefix") {
    val policy = AwsPolicyGenerator.sessionPolicy("mybucket", "table/*")
    assert(policy.contains("mybucket"))
    assert(policy.contains("table/*"))
    assert(policy.contains("s3:GetObject"))
    assert(policy.contains("s3:ListBucket"))
    assert(policy.contains("2012-10-17"))
  }

  test("backupSessionPolicy is GetObject only") {
    val policy = AwsPolicyGenerator.backupSessionPolicy("b", "p/*")
    assert(policy.contains("s3:GetObject"))
    assert(!policy.contains("s3:ListBucket"))
    assert(policy.contains("b"))
    assert(policy.contains("p/*"))
  }

  test("generatePolicy for SELECT and single location") {
    val uri = new URI("s3a://bucket/table/prefix/")
    val policy = AwsPolicyGenerator.generatePolicy(Set(Privilege.SELECT), Seq(uri))
    assert(policy.contains("bucket"))
    assert(policy.contains("table/prefix/"))
    assert(policy.contains("s3:GetObject"))
  }

  test("generatePolicy throws when locations empty") {
    val e = intercept[IllegalArgumentException] {
      AwsPolicyGenerator.generatePolicy(Set(Privilege.SELECT), Seq.empty)
    }
    // scalastyle:off caselocale
    assert(e.getMessage.toLowerCase.contains("location"))
    // scalastyle:on caselocale
  }
}
