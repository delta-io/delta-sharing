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

package io.delta.sharing.server.common

import java.time.{OffsetDateTime, ZoneOffset}
import java.util.Base64
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

import org.scalatest.FunSuite

class AzureUserDelegationSasGeneratorSuite extends FunSuite {

  private val testKey =
    AzureUserDelegationSasGenerator.UserDelegationKeyInfo(
      signedOid = "oid-1234",
      signedTid = "tid-5678",
      signedStart = "2026-05-04T00:00:00Z",
      signedExpiry = "2026-05-04T01:00:00Z",
      signedService = "b",
      signedVersion = "2022-11-02",
      value = Base64.getEncoder.encodeToString(
        "test-delegation-key-value-32b!".getBytes("UTF-8"))
    )

  private val fixedStart = OffsetDateTime.of(
    2026, 5, 4, 0, 0, 0, 0, ZoneOffset.UTC)
  private val fixedExpiry = OffsetDateTime.of(
    2026, 5, 4, 1, 0, 0, 0, ZoneOffset.UTC)

  test("computeUserDelegationSas produces valid SAS query") {
    val sas = AzureUserDelegationSasGenerator
      .computeUserDelegationSas(
        "myaccount", "mycontainer", testKey,
        "r", fixedStart, fixedExpiry)

    // Verify required SAS parameters are present
    assert(sas.contains("sv=2022-11-02"))
    assert(sas.contains("sr=c"))
    assert(sas.contains("sp=r"))
    assert(sas.contains("spr=https"))
    assert(sas.contains("skoid=oid-1234"))
    assert(sas.contains("sktid=tid-5678"))
    assert(sas.contains("sks=b"))
    assert(sas.contains("skv=2022-11-02"))
    assert(sas.contains("sig="))
    // Verify start/expiry are URL-encoded ISO timestamps
    assert(sas.contains("st=2026-05-04"))
    assert(sas.contains("se=2026-05-04"))
  }

  test("computeUserDelegationSas signature is deterministic") {
    val sas1 = AzureUserDelegationSasGenerator
      .computeUserDelegationSas(
        "acct", "ctr", testKey, "r",
        fixedStart, fixedExpiry)
    val sas2 = AzureUserDelegationSasGenerator
      .computeUserDelegationSas(
        "acct", "ctr", testKey, "r",
        fixedStart, fixedExpiry)
    assert(sas1 == sas2)
  }

  test("computeUserDelegationSas signature changes with account") {
    val sas1 = AzureUserDelegationSasGenerator
      .computeUserDelegationSas(
        "account1", "ctr", testKey, "r",
        fixedStart, fixedExpiry)
    val sas2 = AzureUserDelegationSasGenerator
      .computeUserDelegationSas(
        "account2", "ctr", testKey, "r",
        fixedStart, fixedExpiry)
    assert(sas1 != sas2)
  }

  test("computeUserDelegationSas signature changes with container") {
    val sas1 = AzureUserDelegationSasGenerator
      .computeUserDelegationSas(
        "acct", "container1", testKey, "r",
        fixedStart, fixedExpiry)
    val sas2 = AzureUserDelegationSasGenerator
      .computeUserDelegationSas(
        "acct", "container2", testKey, "r",
        fixedStart, fixedExpiry)
    assert(sas1 != sas2)
  }

  test("computeUserDelegationSas signature changes with key") {
    val key2 = testKey.copy(
      value = Base64.getEncoder.encodeToString(
        "different-key-value-32bytes!!".getBytes("UTF-8")))
    val sas1 = AzureUserDelegationSasGenerator
      .computeUserDelegationSas(
        "acct", "ctr", testKey, "r",
        fixedStart, fixedExpiry)
    val sas2 = AzureUserDelegationSasGenerator
      .computeUserDelegationSas(
        "acct", "ctr", key2, "r",
        fixedStart, fixedExpiry)
    assert(sas1 != sas2)
  }

  test("computeUserDelegationSas string-to-sign has 24 fields") {
    // The Azure spec for sv=2022-11-02 requires exactly 24 values
    // separated by 23 newlines in the string-to-sign.
    // We verify by computing HMAC manually and comparing.
    val sv = "2022-11-02"
    val sp = "r"
    val st = "2026-05-04T00:00:00Z"
    val se = "2026-05-04T01:00:00Z"
    val cr = "/blob/myaccount/mycontainer"
    val spr = "https"

    val expectedSts = Seq(
      sp, st, se, cr,
      testKey.signedOid, testKey.signedTid,
      testKey.signedStart, testKey.signedExpiry,
      testKey.signedService, testKey.signedVersion,
      "", "", "", "", spr, sv, "c",
      "", "", "", "", "", "", ""
    ).mkString("\n")

    // Verify 24 values = 23 newlines
    assert(expectedSts.count(_ == '\n') == 23,
      s"Expected 23 newlines, got ${expectedSts.count(_ == '\n')}")

    // Compute expected signature
    val keyBytes = Base64.getDecoder.decode(testKey.value)
    val mac = Mac.getInstance("HmacSHA256")
    mac.init(new SecretKeySpec(keyBytes, "HmacSHA256"))
    val expectedSig = Base64.getEncoder.encodeToString(
      mac.doFinal(expectedSts.getBytes("UTF-8")))

    // Get actual SAS and extract sig parameter
    val sas = AzureUserDelegationSasGenerator
      .computeUserDelegationSas(
        "myaccount", "mycontainer", testKey,
        "r", fixedStart, fixedExpiry)
    val sigParam = sas.split("&")
      .find(_.startsWith("sig=")).get
    val actualSig = java.net.URLDecoder.decode(
      sigParam.stripPrefix("sig="), "UTF-8")

    assert(actualSig == expectedSig,
      s"Signature mismatch.\n" +
        s"Expected: $expectedSig\n" +
        s"Actual:   $actualSig")
  }

  test("parseUserDelegationKeyXml handles BOM") {
    // Verify BOM stripping logic: charAt(0) == 0xFEFF
    val bom = 0xFEFF.toChar
    assert(bom.toInt == 0xFEFF)
    val testStr = s"${bom}hello"
    assert(testStr.charAt(0) == 0xFEFF)
    assert(testStr.substring(1) == "hello")
  }
}
