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

import org.apache.commons.io.FileUtils
import org.scalatest.FunSuite

import io.delta.sharing.server.model.DeltaFormatResponseProtocol

/**
 * Unit tests for [[DeltaSharedTable.queryCDF]] focused on the historical
 * Protocol emission behavior when `includeHistoricalProtocol = true`.
 *
 * Uses [[DeltaSharedTableTestBase]] to hand-craft a tiny local Delta log with
 * a Protocol upgrade at v=2, then asserts on the wrapped
 * [[DeltaFormatResponseProtocol]] objects that the server-side response
 * stream would carry over the wire.
 */
class DeltaSharedTableCDFSuite extends FunSuite with DeltaSharedTableTestBase {

  private val responseFormatDelta = Set("delta")

  test("queryCDF: head Protocol carries no version and no historical Protocols are emitted " +
      "when includeHistoricalProtocol=false") {
    val tableDir = buildProtocolEvolutionTable()
    try {
      val table = buildSharedTable(tableDir)
      val result = table.queryCDF(
        cdfOptions = Map("startingVersion" -> "0"),
        includeHistoricalMetadata = false,
        maxFiles = None,
        pageToken = None,
        responseFormatSet = responseFormatDelta,
        includeEndStreamAction = false,
        includeHistoricalProtocol = false)
      val protocols = protocolsOf(roundTripActions(result.actions))

      // Exactly one Protocol: the head. When the client doesn't opt in via
      // `includeHistoricalProtocol`, the head Protocol carries no `version` field so the
      // pre-existing delta-format wire shape is preserved for legacy clients.
      assert(protocols.size == 1, s"expected single head Protocol, got $protocols")
      val head = protocols.head
      assert(head.version == null,
        s"head Protocol.version must be omitted when flag is off, got ${head.version}")
      assert(head.deltaProtocol.minReaderVersion == 1)
      assert(head.deltaProtocol.minWriterVersion == 4)
    } finally {
      FileUtils.deleteDirectory(tableDir)
    }
  }

  test("queryCDF: includeHistoricalProtocol=true emits head (stamped with snapshot version) " +
      "+ intermediate Protocol@v=2 each stamped with its delta log version") {
    val tableDir = buildProtocolEvolutionTable()
    try {
      val table = buildSharedTable(tableDir)
      val result = table.queryCDF(
        cdfOptions = Map("startingVersion" -> "0"),
        includeHistoricalMetadata = false,
        maxFiles = None,
        pageToken = None,
        responseFormatSet = responseFormatDelta,
        includeEndStreamAction = false,
        includeHistoricalProtocol = true)
      val protocols = protocolsOf(roundTripActions(result.actions))

      // We expect two DeltaFormatResponseProtocol entries: the head (snapshot version)
      // and the v=2 upgrade. The v=0 Protocol must NOT appear as a historical entry
      // because the `v > start` gate suppresses it (it's already represented by the head).
      val versions = protocols.map(_.version.longValue()).sorted
      assert(versions == Seq(2L, 4L),
        s"expected DeltaFormatResponseProtocol@v=2 and head@v=4, got versions=$versions")

      val byVersion = protocols.map(p => p.version.longValue() -> p).toMap

      // Head Protocol at v=4 reflects the upgraded reader/writer versions.
      assert(byVersion(4L).deltaProtocol.minReaderVersion == 1)
      assert(byVersion(4L).deltaProtocol.minWriterVersion == 4)

      // Historical Protocol at v=2 is the upgrade itself.
      assert(byVersion(2L).deltaProtocol.minReaderVersion == 1)
      assert(byVersion(2L).deltaProtocol.minWriterVersion == 4)
    } finally {
      FileUtils.deleteDirectory(tableDir)
    }
  }

  test("queryCDF: starting from the Protocol-upgrade version itself emits only the head " +
      "(the `v > start` gate suppresses the historical Protocol@v=start)") {
    val tableDir = buildProtocolEvolutionTable()
    try {
      val table = buildSharedTable(tableDir)
      val result = table.queryCDF(
        cdfOptions = Map("startingVersion" -> "2"),
        includeHistoricalMetadata = false,
        maxFiles = None,
        pageToken = None,
        responseFormatSet = responseFormatDelta,
        includeEndStreamAction = false,
        includeHistoricalProtocol = true)
      val protocols = protocolsOf(roundTripActions(result.actions))

      // Only the head Protocol; v=2 fails the `v > start` gate.
      assert(protocols.size == 1,
        s"expected single head Protocol when start=upgrade, got $protocols")
      assert(protocols.head.version == 4L)
      assert(protocols.head.deltaProtocol.minWriterVersion == 4)
    } finally {
      FileUtils.deleteDirectory(tableDir)
    }
  }

}
