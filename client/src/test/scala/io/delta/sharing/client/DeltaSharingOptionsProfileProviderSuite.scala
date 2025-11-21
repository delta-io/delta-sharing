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

package io.delta.sharing.client

import org.apache.spark.SparkFunSuite



class DeltaSharingOptionsProfileProviderSuite extends SparkFunSuite {

  private def testProfile(
      shareCredentialsOptions: Map[String, Any], expected: DeltaSharingProfile): Unit = {
    assert(new DeltaSharingOptionsProfileProvider(shareCredentialsOptions)
      .getProfile == expected)
  }

  test("parse") {
    testProfile(
      Map(
        "shareCredentialsVersion" -> 1,
        "endpoint" -> "foo",
        "bearerToken" -> "bar",
        "expirationTime" -> "2021-11-12T00:12:29Z"
      ),
      DeltaSharingProfile(
        shareCredentialsVersion = Some(1),
        endpoint = "foo",
        bearerToken = "bar",
        expirationTime = "2021-11-12T00:12:29Z"
      )
    )
  }

  test("expirationTime is optional") {
    testProfile(
      Map(
        "shareCredentialsVersion" -> 1,
        "endpoint" -> "foo",
        "bearerToken" -> "bar"
      ),
      DeltaSharingProfile(
        shareCredentialsVersion = Some(1),
        endpoint = "foo",
        bearerToken = "bar"
      )
    )
  }

  test("shareCredentialsVersion is missing") {
    val e = intercept[IllegalArgumentException] {
      testProfile(
        Map(
          "endpoint" -> "foo",
          "bearerToken" -> "bar"
        ),
        null
      )
    }
    assert(e.getMessage.contains(
      "Cannot find the 'shareCredentialsVersion' field in the profile"))
  }

  test("shareCredentialsVersion is incorrect") {
    val e = intercept[IllegalArgumentException] {
      testProfile(
        Map(
          "shareCredentialsVersion" -> 2,
          "endpoint" -> "foo",
          "bearerToken" -> "bar"
        ),
        null
      )
    }
    assert(e.getMessage.contains(
      "'shareCredentialsVersion' in the profile is 2 which is too new. " +
        "The current release supports version 1 and below. Please upgrade to a newer release."))
  }

  test("shareCredentialsVersion is not supported") {
    val e = intercept[IllegalArgumentException] {
      testProfile(
        Map(
          "shareCredentialsVersion" -> 100
        ),
        null
      )
    }
    assert(e.getMessage.contains(
      "'shareCredentialsVersion' in the profile is 100 which is too new."))
  }

  test("endpoint is missing") {
    val e = intercept[IllegalArgumentException] {
      testProfile(
        Map(
          "shareCredentialsVersion" -> 1,
          "bearerToken" -> "bar"
        ),
        null
      )
    }
    assert(e.getMessage.contains("Cannot find the 'endpoint' field in the profile"))
  }

  test("bearerToken is missing") {
    val e = intercept[IllegalArgumentException] {
      testProfile(
        Map(
          "shareCredentialsVersion" -> 1,
          "endpoint" -> "foo"
        ),
        null
      )
    }
    assert(e.getMessage.contains("Cannot find the 'bearerToken' field in the profile"))
  }

  test("unknown field should be ignored") {
    testProfile(
      Map(
      "shareCredentialsVersion" -> 1,
      "endpoint" -> "foo",
      "bearerToken" -> "bar",
      "expirationTime" -> "2021-11-12T00:12:29Z",
      "futureField" -> "xyz"
      ),
      DeltaSharingProfile(
        shareCredentialsVersion = Some(1),
        endpoint = "foo",
        bearerToken = "bar",
        expirationTime = "2021-11-12T00:12:29Z"
      )
    )
  }

  test("oauth_client_credentials only supports version 2") {
    val e = intercept[IllegalArgumentException] {
      testProfile(
        Map(
          "shareCredentialsVersion" -> 1,
          "endpoint" -> "foo",
          "tokenEndpoint" -> "bar",
          "clientId" -> "abc",
          "clientSecret" -> "xyz",
          "type" -> "oauth_client_credentials",
          "scope" -> "testScope"
        ),
        null
      )
    }
    assert(e.getMessage.contains(s"oauth_client_credentials only supports version 2"))
  }

  test("oauth mandatory config is missing") {
    val mandatoryFields = Seq("endpoint", "tokenEndpoint", "clientId", "clientSecret")

    for (missingField <- mandatoryFields) {

      val profile = {
        mandatoryFields
        .filter(_ != missingField)
        .map(f => f -> "value")
        .toMap + ("shareCredentialsVersion" -> 1, "type" -> "oauth_client_credentials")
      }

      val e = intercept[IllegalArgumentException] {
        testProfile(profile, null)
      }
      assert(e.getMessage.contains(s"Cannot find the '$missingField' field in the profile"))
    }
  }
}
