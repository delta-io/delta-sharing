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

package io.delta.sharing.server.credential.gcp

import java.io.IOException
import java.net.{URI, URLDecoder}
import java.util.Date

import com.google.api.client.testing.http.{MockHttpTransport, MockLowLevelHttpResponse}
import com.google.auth.oauth2.{AccessToken, GoogleCredentials}
import org.scalatest.FunSuite

import io.delta.sharing.server.common.JsonUtils
import io.delta.sharing.server.credential.{CredentialContext, Privilege}

class GcpCredentialVendorSuite extends FunSuite {

  test("directory credentials restrict reads and listing to a directory boundary") {
    val boundary = GcpCredentialVendor.accessBoundary("bucket", "shared/table")
    val rules = boundary.getAccessBoundaryRules
    assert(rules.size() == 1)
    val rule = rules.get(0)
    assert(rule.getAvailableResource == "//storage.googleapis.com/projects/_/buckets/bucket")
    assert(rule.getAvailablePermissions.size() == 1)
    assert(rule.getAvailablePermissions.get(0) == "inRole:roles/storage.objectViewer")
    assert(rule.getAvailabilityCondition.getExpression ==
      "resource.name.startsWith(\"projects/_/buckets/bucket/objects/shared/table/\") || " +
        "api.getAttribute('storage.googleapis.com/objectListPrefix', '')" +
        ".startsWith(\"shared/table/\")")
  }

  test("trailing separators do not widen the directory boundary") {
    val expected = GcpCredentialVendor.accessBoundary("bucket", "shared/table")
      .getAccessBoundaryRules.get(0).getAvailabilityCondition.getExpression
    for (prefix <- Seq("shared/table/", "shared/table//")) {
      assert(GcpCredentialVendor.accessBoundary("bucket", prefix)
        .getAccessBoundaryRules.get(0).getAvailabilityCondition.getExpression == expected)
    }
  }

  test("bucket-root credentials retain a bucket and read-only role boundary") {
    for (prefix <- Seq("", "/")) {
      val rules = GcpCredentialVendor.accessBoundary("bucket", prefix).getAccessBoundaryRules
      assert(rules.size() == 1)
      assert(rules.get(0).getAvailableResource ==
        "//storage.googleapis.com/projects/_/buckets/bucket")
      assert(rules.get(0).getAvailablePermissions.size() == 1)
      assert(rules.get(0).getAvailablePermissions.get(0) == "inRole:roles/storage.objectViewer")
      assert(rules.get(0).getAvailabilityCondition == null)
    }
  }

  test("quotes and backslashes in object names remain inside CEL string literals") {
    val expression = GcpCredentialVendor.accessBoundary("bucket", "shared/a\"b\\c")
      .getAccessBoundaryRules.get(0).getAvailabilityCondition.getExpression
    assert(expression ==
      "resource.name.startsWith(\"projects/_/buckets/bucket/objects/shared/a\\\"b\\\\c/\") || " +
        "api.getAttribute('storage.googleapis.com/objectListPrefix', '')" +
        ".startsWith(\"shared/a\\\"b\\\\c/\")")
  }

  test("vending exchanges source credentials even for bucket-root tables") {
    for (location <- Seq("gs://bucket", "gs://bucket/", "gs://bucket/shared/table")) {
      // Only the remote STS response is substituted; credential construction and exchange are real.
      val response = new MockLowLevelHttpResponse()
        .setContentType("application/json")
        .setContent("""{"access_token":"scoped-token","expires_in":600,"token_type":"Bearer",
          |"issued_token_type":"urn:ietf:params:oauth:token-type:access_token"}""".stripMargin)
      val transport = new MockHttpTransport.Builder().setLowLevelHttpResponse(response).build()
      val source = GoogleCredentials.create(
        new AccessToken("source-token", new Date(System.currentTimeMillis() + 3600000)))
      val context = CredentialContext.create(new URI(location), Set(Privilege.SELECT))
      val before = System.currentTimeMillis()
      val credentials = new GcpCredentialVendor().vendGcpCredential(
        context, 3600, source, () => transport)
      assert(credentials.location == location)
      assert(credentials.gcpOauthToken.oauthToken == "scoped-token")
      assert(credentials.expirationTime >= before + 590000)
      assert(credentials.expirationTime <= System.currentTimeMillis() + 600000)
      val form = transport.getLowLevelHttpRequest.getContentAsString.split("&").map { pair =>
        val fields = pair.split("=", 2)
        URLDecoder.decode(fields(0), "UTF-8") -> URLDecoder.decode(fields(1), "UTF-8")
      }.toMap
      val rule = JsonUtils.mapper.readTree(form("options"))
        .get("accessBoundary").get("accessBoundaryRules").get(0)
      assert(rule.get("availableResource").asText() ==
        "//storage.googleapis.com/projects/_/buckets/bucket")
      assert(rule.get("availablePermissions").get(0).asText() ==
        "inRole:roles/storage.objectViewer")
      if (location.contains("shared/table")) {
        assert(rule.get("availabilityCondition").get("expression").asText()
          .contains("shared/table/"))
      }
    }
  }


  test("failed downscoping never falls back to the source token") {
    val response = new MockLowLevelHttpResponse()
      .setStatusCode(403)
      .setContentType("application/json")
      .setContent("""{"error":"access_denied"}""")
    val transport = new MockHttpTransport.Builder().setLowLevelHttpResponse(response).build()
    val source = GoogleCredentials.create(
      new AccessToken("source-token", new Date(System.currentTimeMillis() + 3600000)))
    val context = CredentialContext.create(new URI("gs://bucket"), Set(Privilege.SELECT))
    intercept[IOException] {
      new GcpCredentialVendor().vendGcpCredential(context, 3600, source, () => transport)
    }
  }

}
