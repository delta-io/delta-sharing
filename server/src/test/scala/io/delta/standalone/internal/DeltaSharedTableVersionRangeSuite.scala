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
 * Unit tests for [[DeltaSharedTable.query]] in its streaming version-range mode
 * (the path taken when the request supplies `startingVersion`). Verifies that
 * the historical Protocol emission behavior mirrors the CDF path: when
 * `includeHistoricalProtocol = true` and the response is in delta format, the
 * wrapped [[DeltaFormatResponseProtocol]] objects carry the correct
 * per-version stamps and the `v > startingVersion` gate is honored.
 *
 * Uses [[DeltaSharedTableTestBase]] to hand-craft a tiny local Delta log with
 * a Protocol upgrade at v=2.
 */
class DeltaSharedTableVersionRangeSuite extends FunSuite with DeltaSharedTableTestBase {

  private val responseFormatDelta = Set("delta")

  /** Invoke `DeltaSharedTable.query` in version-range mode with sensible defaults. */
  private def runQuery(
      table: DeltaSharedTable,
      startingVersion: Long,
      endingVersion: Option[Long],
      includeHistoricalProtocol: Boolean): Seq[Object] = {
    table.query(
      includeFiles = true,
      predicateHints = Nil,
      jsonPredicateHints = None,
      limitHint = None,
      version = None,
      timestamp = None,
      startingVersion = Some(startingVersion),
      endingVersion = endingVersion,
      maxFiles = None,
      pageToken = None,
      includeRefreshToken = false,
      refreshToken = None,
      responseFormatSet = responseFormatDelta,
      clientReaderFeaturesSet = Set.empty,
      includeEndStreamAction = false,
      includeHistoricalProtocol = includeHistoricalProtocol).actions
  }

  test("query[startingVersion]: head Protocol carries no version and no historical " +
      "Protocols are emitted when includeHistoricalProtocol=false") {
    val tableDir = buildProtocolEvolutionTable()
    try {
      val table = buildSharedTable(tableDir)
      val rawActions = runQuery(
        table,
        startingVersion = 0L,
        endingVersion = Some(4L),
        includeHistoricalProtocol = false)
      val protocols = protocolsOf(roundTripActions(rawActions))

      // Exactly one Protocol: the head. In the streaming `query` path the snapshot is
      // pinned at `startingVersion`, so the head Protocol reflects whatever Protocol
      // the table had at v=startingVersion (= v=0 here, i.e. Protocol(1,2)). When the
      // client doesn't opt in via `includeHistoricalProtocol`, the head carries no
      // `version` field so the pre-existing delta-format wire shape is preserved.
      assert(protocols.size == 1, s"expected single head Protocol, got $protocols")
      val head = protocols.head
      assert(head.version == null,
        s"head Protocol.version must be omitted when flag is off, got ${head.version}")
      assert(head.deltaProtocol.minReaderVersion == 1)
      assert(head.deltaProtocol.minWriterVersion == 2)
    } finally {
      FileUtils.deleteDirectory(tableDir)
    }
  }

  test("query[startingVersion]: includeHistoricalProtocol=true emits head + intermediate " +
      "Protocol@v=2 each stamped with its delta log version") {
    val tableDir = buildProtocolEvolutionTable()
    try {
      val table = buildSharedTable(tableDir)
      val rawActions = runQuery(
        table,
        startingVersion = 0L,
        endingVersion = Some(4L),
        includeHistoricalProtocol = true)
      val protocols = protocolsOf(roundTripActions(rawActions))

      // We expect two DeltaFormatResponseProtocol entries: the head (stamped with
      // startingVersion=0, Protocol(1,2) which is what the table had at v=0) and the
      // v=2 upgrade Protocol(1,4). The v=0 Protocol from the log must NOT appear as
      // a separate historical entry because the `v > startingVersion` gate suppresses
      // it (the head already conveys it).
      val versions = protocols.map(_.version.longValue()).sorted
      assert(versions == Seq(0L, 2L),
        s"expected DeltaFormatResponseProtocol@v=0 (head) and @v=2 (upgrade), got $versions")

      val byVersion = protocols.map(p => p.version.longValue() -> p).toMap

      // Head Protocol at v=0 reflects the table's Protocol at startingVersion.
      assert(byVersion(0L).deltaProtocol.minReaderVersion == 1)
      assert(byVersion(0L).deltaProtocol.minWriterVersion == 2)

      // Historical Protocol at v=2 is the upgrade itself.
      assert(byVersion(2L).deltaProtocol.minReaderVersion == 1)
      assert(byVersion(2L).deltaProtocol.minWriterVersion == 4)
    } finally {
      FileUtils.deleteDirectory(tableDir)
    }
  }

  test("query[startingVersion]: starting from the Protocol-upgrade version itself emits " +
      "only the head (the `v > startingVersion` gate suppresses Protocol@v=start)") {
    val tableDir = buildProtocolEvolutionTable()
    try {
      val table = buildSharedTable(tableDir)
      val rawActions = runQuery(
        table,
        startingVersion = 2L,
        endingVersion = Some(4L),
        includeHistoricalProtocol = true)
      val protocols = protocolsOf(roundTripActions(rawActions))

      // Only the head Protocol; v=2 fails the `v > startingVersion` gate. Snapshot is
      // pinned at v=2, so head carries Protocol(1,4).
      assert(protocols.size == 1,
        s"expected single head Protocol when start=upgrade, got $protocols")
      val head = protocols.head
      assert(head.version == 2L,
        s"head Protocol.version should equal startingVersion (2), got ${head.version}")
      assert(head.deltaProtocol.minReaderVersion == 1)
      assert(head.deltaProtocol.minWriterVersion == 4)
    } finally {
      FileUtils.deleteDirectory(tableDir)
    }
  }
}
