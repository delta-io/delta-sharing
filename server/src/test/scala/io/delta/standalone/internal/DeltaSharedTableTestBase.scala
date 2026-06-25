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
import org.apache.hadoop.fs.Path

import io.delta.sharing.server.common.{CloudFileSigner, JsonUtils, PreSignedUrl}
import io.delta.sharing.server.config.TableConfig
// Aliases to avoid the standalone-side `DeltaResponseSingleAction` in
// `io.delta.standalone.internal.model` shadowing the server-side wire types.
import io.delta.sharing.server.{model => serverModel}
import io.delta.standalone.internal.actions.{Action, AddFile, Metadata, Protocol}

/**
 * Shared scaffolding for unit tests that exercise [[DeltaSharedTable]] against a
 * hand-crafted local Delta log. Used by [[DeltaSharedTableCDFSuite]] and
 * [[DeltaSharedTableVersionRangeSuite]] so they can assert on the wrapped
 * [[DeltaFormatResponseProtocol]] objects without standing up an S3 fixture,
 * file signer, or HTTP server.
 */
trait DeltaSharedTableTestBase {

  private val schemaJson =
    """{"type":"struct","fields":[
      |{"name":"id","type":"integer","nullable":true,"metadata":{}}]}""".stripMargin

  /**
   * Build a small CDF-enabled Delta table with this layout:
   *   v0: Protocol(1,2) + Metadata(CDF on)        - initial commit
   *   v1: AddFile                                 - data commit
   *   v2: Protocol(1,4) + Metadata(CDF on, renamed) - Protocol + Metadata change
   *   v3: AddFile                                 - data commit
   *   v4: AddFile                                 - data commit
   *
   * Caller is responsible for deleting the directory when done.
   */
  protected def buildProtocolEvolutionTable(): File = {
    val tableDir = Files.createTempDirectory("delta-sharing-shared-table-suite").toFile
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

  /** Write a single Delta commit at `version` as NDJSON action lines. */
  private def writeCommit(logDir: File, version: Long, actions: Seq[Action]): Unit = {
    val name = f"$version%020d.json"
    val file = new File(logDir, name)
    val body = actions.map(_.json).mkString("", "\n", "\n")
    FileUtils.writeStringToFile(file, body, UTF_8)
    // Stagger modification timestamps so DeltaSharingHistoryManager observes a
    // monotonically increasing commit timeline.
    file.setLastModified(1700000000000L + version * 1000L)
  }

  /**
   * Build a [[DeltaSharedTable]] pointed at the given local-fs table. The
   * file signer is overridden with a no-op that returns the path as-is so the
   * table can be constructed without an S3/Azure/GCS credentials chain.
   *
   * `historyShared = true` so the CDF flag survives Metadata cleanup.
   */
  protected def buildSharedTable(tableDir: File): DeltaSharedTable = {
    val tableConfig = new TableConfig(
      "test-table",
      tableDir.getAbsolutePath,
      "00000000-0000-0000-0000-000000000099",
      /* historyShared */ true,
      /* startVersion */ 0L)
    new DeltaSharedTable(
      tableConfig = tableConfig,
      preSignedUrlTimeoutSeconds = 3600,
      evaluatePredicateHints = false,
      evaluateJsonPredicateHints = false,
      evaluateJsonPredicateHintsV2 = false,
      queryTablePageSizeLimit = 100000,
      queryTablePageTokenTtlMs = 60000,
      refreshTokenTtlMs = 60000) {
      override protected def newFileSigner(): CloudFileSigner =
        DeltaSharedTableTestBase.NoOpFileSigner
    }
  }

  /**
   * Re-parse the raw response objects produced by the standalone-side
   * `DeltaSharedTable` through the kernel-side wire types, mirroring what
   * the HTTP server does for delta-format streaming/CDF responses. Returns
   * the deserialized server-side `DeltaResponseSingleAction`s so callers can
   * pattern match on `DeltaFormatResponseProtocol` directly.
   */
  protected def roundTripActions(
      actions: Seq[Object]): Seq[serverModel.DeltaResponseSingleAction] = {
    actions.map { a =>
      val json = JsonUtils.toJson(a)
      JsonUtils.fromJson[serverModel.DeltaResponseSingleAction](json)
    }
  }

  /** Collect every `DeltaFormatResponseProtocol` in the result. */
  protected def protocolsOf(
      actions: Seq[serverModel.DeltaResponseSingleAction]
    ): Seq[serverModel.DeltaFormatResponseProtocol] = {
    actions.flatMap(a => Option(a.protocol))
  }
}

object DeltaSharedTableTestBase {
  // A no-op signer suitable for local-fs tests. Returns the path verbatim with
  // a far-future expiration so the streaming/CDF code paths can run end-to-end
  // without ever talking to a real cloud signer.
  val NoOpFileSigner: CloudFileSigner = new CloudFileSigner {
    override def sign(path: Path): PreSignedUrl =
      PreSignedUrl(url = path.toString, expirationTimestamp = Long.MaxValue)
  }
}
