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

import java.io.File
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.scalatest.FunSuite

import io.delta.standalone.DeltaLog
import io.delta.standalone.internal.actions.{Action, AddFile, Metadata, Protocol}

/**
 * Unit tests for [[DeltaSharingCDCReader.queryCDF]] focused on the historical
 * Metadata / Protocol selection behavior.
 *
 * These tests hand-craft a tiny Delta log on the local file system so we can
 * deterministically place a Protocol upgrade and a Metadata change mid-history.
 * No S3 fixture, file signer, or HTTP server is required.
 *
 * Note: the streaming version-range path
 * (`DeltaSharedTable.queryDataChangeSinceStartVersion`) applies the same
 * `case (p: Protocol, _) if includeHistoricalProtocol && v > startingVersion`
 * gate, so the coverage below transfers structurally to that path as well.
 * It is not directly testable here because instantiating `DeltaSharedTable`
 * requires an S3/Azure/GCS file signer.
 */
class DeltaSharingCDCReaderSuite extends FunSuite {

  private val schemaJson =
    """{"type":"struct","fields":[
      |{"name":"id","type":"integer","nullable":true,"metadata":{}}]}""".stripMargin

  /**
   * Write the supplied actions as a single Delta commit at the given version.
   * Each action becomes one NDJSON line in `_delta_log/<version>.json`.
   */
  private def writeCommit(logDir: File, version: Long, actions: Seq[Action]): Unit = {
    val name = f"$version%020d.json"
    val file = new File(logDir, name)
    val body = actions.map(_.json).mkString("", "\n", "\n")
    FileUtils.writeStringToFile(file, body, UTF_8)
    // Stagger modification timestamps so DeltaSharingHistoryManager observes
    // a monotonically increasing commit timeline.
    file.setLastModified(1700000000000L + version * 1000L)
  }

  /**
   * Build a small CDF-enabled Delta table with this layout:
   *   v0: Protocol(1,2) + Metadata(CDF on)        - initial commit
   *   v1: AddFile                                 - data commit
   *   v2: Protocol(1,4) + Metadata(CDF on)        - Protocol + Metadata change
   *   v3: AddFile                                 - data commit
   *   v4: AddFile                                 - data commit
   */
  private def buildTable(): File = {
    val tableDir = Files.createTempDirectory("delta-sharing-cdc-reader-suite").toFile
    tableDir.deleteOnExit()
    val logDir = new File(tableDir, "_delta_log")
    logDir.mkdirs()

    val cdfConfig = Map("delta.enableChangeDataFeed" -> "true")
    val metadataV0 = Metadata(
      id = "11111111-1111-1111-1111-111111111111",
      schemaString = schemaJson,
      configuration = cdfConfig,
      createdTime = Some(1700000000000L))
    val metadataV2 = metadataV0.copy(name = "renamed-at-v2")

    writeCommit(logDir, 0, Seq(Protocol(1, 2), metadataV0))
    writeCommit(logDir, 1, Seq(AddFile(
      path = "v1.parquet",
      partitionValues = Map.empty,
      size = 100L,
      modificationTime = 1700000001000L,
      dataChange = true)))
    writeCommit(logDir, 2, Seq(Protocol(1, 4), metadataV2))
    writeCommit(logDir, 3, Seq(AddFile(
      path = "v3.parquet",
      partitionValues = Map.empty,
      size = 100L,
      modificationTime = 1700000003000L,
      dataChange = true)))
    writeCommit(logDir, 4, Seq(AddFile(
      path = "v4.parquet",
      partitionValues = Map.empty,
      size = 100L,
      modificationTime = 1700000004000L,
      dataChange = true)))

    tableDir
  }

  /**
   * Open the hand-crafted table as a DeltaLog and build a CDC reader.
   * Returns `(reader, latestVersion)`.
   */
  private def newReader(tableDir: File): (DeltaSharingCDCReader, Long) = {
    val conf = new Configuration()
    val deltaLog = DeltaLog.forTable(conf, tableDir.getAbsolutePath).asInstanceOf[DeltaLogImpl]
    val reader = new DeltaSharingCDCReader(deltaLog, conf)
    (reader, deltaLog.snapshot.getVersion)
  }

  /** Project the CDCDataSpec list down to `(version, actionsByType)` for easier asserts. */
  private def summarize(specs: Seq[_]): Map[Long, Seq[Action]] = {
    specs.map { s =>
      val spec = s.asInstanceOf[DeltaSharingCDCReader#CDCDataSpec]
      spec.version -> spec.actions
    }.toMap
  }

  test("queryCDF: no historical Metadata or Protocol when both flags are false") {
    val tableDir = buildTable()
    try {
      val (reader, latest) = newReader(tableDir)
      val specs = reader.queryCDF(
        start = 0L,
        end = latest,
        latestVersion = latest,
        includeHistoricalMetadata = false,
        includeHistoricalProtocol = false)
      val byVersion = summarize(specs)

      // Only AddFile-bearing versions should expose any actions.
      assert(byVersion(0L).isEmpty, "v0 should not surface its initial Metadata/Protocol")
      assert(byVersion(1L).exists(_.isInstanceOf[AddFile]))
      assert(byVersion(2L).isEmpty, "v2 Protocol/Metadata must be hidden when both flags are off")
      assert(byVersion(3L).exists(_.isInstanceOf[AddFile]))
      assert(byVersion(4L).exists(_.isInstanceOf[AddFile]))
      assert(byVersion.values.flatten.collectFirst { case _: Protocol => () }.isEmpty)
      assert(byVersion.values.flatten.collectFirst { case _: Metadata => () }.isEmpty)
    } finally {
      FileUtils.deleteDirectory(tableDir)
    }
  }

  test("queryCDF: includeHistoricalProtocol=true emits intermediate Protocol actions") {
    val tableDir = buildTable()
    try {
      val (reader, latest) = newReader(tableDir)
      val specs = reader.queryCDF(
        start = 0L,
        end = latest,
        latestVersion = latest,
        includeHistoricalMetadata = false,
        includeHistoricalProtocol = true)
      val byVersion = summarize(specs)

      // v0 Protocol must NOT be emitted: the `v > start` gate keeps the head Protocol
      // out of the historical stream so it doesn't duplicate the head action.
      assert(byVersion(0L).collectFirst { case _: Protocol => () }.isEmpty,
        "head-version Protocol should never appear as a historical action")

      val v2Protocol = byVersion(2L).collectFirst { case p: Protocol => p }
      assert(v2Protocol.isDefined, "Protocol@v2 must be emitted when the flag is set")
      assert(v2Protocol.get.minReaderVersion == 1)
      assert(v2Protocol.get.minWriterVersion == 4)

      // Metadata is still suppressed because includeHistoricalMetadata=false.
      assert(byVersion.values.flatten.collectFirst { case _: Metadata => () }.isEmpty)
    } finally {
      FileUtils.deleteDirectory(tableDir)
    }
  }

  test("queryCDF: includeHistoricalMetadata + includeHistoricalProtocol emit both at v2") {
    val tableDir = buildTable()
    try {
      val (reader, latest) = newReader(tableDir)
      val specs = reader.queryCDF(
        start = 0L,
        end = latest,
        latestVersion = latest,
        includeHistoricalMetadata = true,
        includeHistoricalProtocol = true)
      val byVersion = summarize(specs)

      val v2Actions = byVersion(2L)
      val v2Protocol = v2Actions.collectFirst { case p: Protocol => p }
      val v2Metadata = v2Actions.collectFirst { case m: Metadata => m }
      assert(v2Protocol.isDefined, "Protocol@v2 must be emitted")
      assert(v2Metadata.isDefined, "Metadata@v2 must be emitted")
      assert(v2Protocol.get.minWriterVersion == 4)
      assert(v2Metadata.get.name == "renamed-at-v2")

      // Confirm no Protocol/Metadata leak from version 0 (which equals `start`).
      assert(byVersion(0L).isEmpty,
        "v0 Protocol/Metadata must stay out of the historical stream when start=0")
    } finally {
      FileUtils.deleteDirectory(tableDir)
    }
  }

  test("queryCDF: when start equals the Protocol-upgrade version, that Protocol is not emitted") {
    val tableDir = buildTable()
    try {
      val (reader, latest) = newReader(tableDir)
      val specs = reader.queryCDF(
        start = 2L,
        end = latest,
        latestVersion = latest,
        includeHistoricalMetadata = true,
        includeHistoricalProtocol = true)
      val byVersion = summarize(specs)

      // start = 2, so v=2 fails the `v > start` gate. The Protocol/Metadata at v2
      // are conveyed to the client via the head action instead of as historical events.
      assert(byVersion.get(2L).forall(_.isEmpty),
        "v2 should expose no historical Metadata/Protocol when start=2")
      assert(byVersion(3L).exists(_.isInstanceOf[AddFile]))
      assert(byVersion(4L).exists(_.isInstanceOf[AddFile]))
    } finally {
      FileUtils.deleteDirectory(tableDir)
    }
  }

  test("queryCDF: CDCDataSpec.version reflects the actual delta log version") {
    val tableDir = buildTable()
    try {
      val (reader, latest) = newReader(tableDir)
      val specs = reader.queryCDF(
        start = 0L,
        end = latest,
        latestVersion = latest,
        includeHistoricalMetadata = true,
        includeHistoricalProtocol = true)
      // Versions 0..4 must each appear exactly once and in order.
      val versions = specs.map(_.asInstanceOf[DeltaSharingCDCReader#CDCDataSpec].version)
      assert(versions == Seq(0L, 1L, 2L, 3L, 4L))
    } finally {
      FileUtils.deleteDirectory(tableDir)
    }
  }
}
