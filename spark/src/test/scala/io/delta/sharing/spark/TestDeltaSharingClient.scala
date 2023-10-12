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

import io.delta.sharing.client.{
  DeltaSharingClient,
  DeltaSharingProfile,
  DeltaSharingProfileProvider
}
import io.delta.sharing.client.model.{
  AddCDCFile,
  AddFile,
  AddFileForCDF,
  DeltaTableFiles,
  DeltaTableMetadata,
  Metadata,
  Protocol,
  RemoveFile,
  SingleAction,
  Table
}
import io.delta.sharing.client.util.JsonUtils
import io.delta.sharing.spark.TestDeltaSharingClient.TESTING_TIMESTAMP

class TestDeltaSharingClient(
    profileProvider: DeltaSharingProfileProvider = new TestDeltaSharingProfileProvider,
    timeoutInSeconds: Int = 120,
    numRetries: Int = 10,
    maxRetryDuration: Long = Long.MaxValue,
    sslTrustAll: Boolean = false,
    forStreaming: Boolean = false,
    responseFormat: String = DeltaSharingOptions.RESPONSE_FORMAT_PARQUET,
    readerFeatures: String = "",
    queryTablePaginationEnabled: Boolean = false,
    maxFilesPerReq: Int = 10000
  ) extends DeltaSharingClient {

  import DeltaSharingOptions.RESPONSE_FORMAT_PARQUET

  private val metadataString =
    """{"metaData":{"id":"93351cf1-c931-4326-88f0-d10e29e71b21","format":
      |{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",
      |\"fields\":[{\"name\":\"col1\",\"type\":\"integer\",\"nullable\":true,
      |\"metadata\":{}},{\"name\":\"col2\",\"type\":\"string\",\"nullable\":true,
      |\"metadata\":{}}]}","partitionColumns":[],"configuration":{},
      |"size": 100,"numFiles": 2,"createdTime":1603723967515}}"""
      .stripMargin.replaceAll("\n", "")
  // Note the difference on the nullable field.
  private val schemaStringV1 = """{"type":"struct",
      |"fields":[{"name":"col1","type":"integer","nullable":false,
      |"metadata":{}},{"name":"col2","type":"string","nullable":false,
      |"metadata":{}}]}""".stripMargin.replaceAll("\n", "")
  private val schemaStringV2 =
    """{"type":"struct",
      |"fields":[{"name":"col1","type":"integer","nullable":true,
      |"metadata":{}},{"name":"col2","type":"string","nullable":false,
      |"metadata":{}}]}""".stripMargin.replaceAll("\n", "")
  private val metadata = JsonUtils.fromJson[SingleAction](metadataString).metaData
  private val metadataV1 = metadata.copy(schemaString = schemaStringV1)
  private val metadataV2 = metadata.copy(schemaString = schemaStringV2)

  override def listAllTables(): Seq[Table] = Nil

  // This function returns the mocked response from delta sharing server.
  override def getMetadata(
      table: Table,
      versionAsOf: Option[Long] = None,
      timestampAsOf: Option[String] = None): DeltaTableMetadata = {
    TestDeltaSharingClient.numMetadataCalled += 1
    // Different metadata are returned for rpcs with different parameters to test the parameters
    // are set properly.
    if (versionAsOf.exists(_ == 1)) {
      DeltaTableMetadata(1, Protocol(0), metadataV1, respondedFormat = RESPONSE_FORMAT_PARQUET)
    } else if (timestampAsOf.exists(_ == TESTING_TIMESTAMP)) {
      DeltaTableMetadata(1, Protocol(0), metadataV2, respondedFormat = RESPONSE_FORMAT_PARQUET)
    } else {
      DeltaTableMetadata(0, Protocol(0), metadata, respondedFormat = RESPONSE_FORMAT_PARQUET)
    }
  }

  override def getTableVersion(table: Table, startingTimestamp: Option[String] = None): Long = 0

  override def getFiles(
    table: Table,
    predicates: Seq[String],
    limit: Option[Long],
    versionAsOf: Option[Long],
    timestampAsOf: Option[String],
    jsonPredicateHints: Option[String],
    refreshToken: Option[String]): DeltaTableFiles = {
    limit.foreach(lim => TestDeltaSharingClient.limits = TestDeltaSharingClient.limits :+ lim)
    jsonPredicateHints.foreach(p => {
      TestDeltaSharingClient.jsonPredicateHints = TestDeltaSharingClient.jsonPredicateHints :+ p
    })

    val addFiles: Seq[AddFile] = if (versionAsOf.isDefined || timestampAsOf.isDefined) {
       Seq(
        AddFile("f1.parquet", "f1", Map.empty, 0, version = 1, timestamp = 1600000000L),
        AddFile("f2.parquet", "f2", Map.empty, 0, version = 1, timestamp = 1600000000L),
        AddFile("f3.parquet", "f3", Map.empty, 0, version = 1, timestamp = 1600000000L),
        AddFile("f4.parquet", "f4", Map.empty, 0, version = 1, timestamp = 1600000000L)
      ).take(limit.getOrElse(4L).toInt)
    } else {
      Seq(
        AddFile("f1.parquet", "f1", Map.empty, 0),
        AddFile("f2.parquet", "f2", Map.empty, 0),
        AddFile("f3.parquet", "f3", Map.empty, 0),
        AddFile("f4.parquet", "f4", Map.empty, 0)
      ).take(limit.getOrElse(4L).toInt)
    }

    if (versionAsOf.exists(_ == 1)) {
      DeltaTableFiles(
        1, Protocol(0), metadataV1, addFiles, respondedFormat = RESPONSE_FORMAT_PARQUET
      )
    } else if (timestampAsOf.exists(_ == TESTING_TIMESTAMP)) {
      DeltaTableFiles(
        1, Protocol(0), metadataV2, addFiles, respondedFormat = RESPONSE_FORMAT_PARQUET
      )
    } else {
      DeltaTableFiles(0, Protocol(0), metadata, addFiles, respondedFormat = RESPONSE_FORMAT_PARQUET)
    }
  }

  override def getFiles(
      table: Table,
      startingVersion: Long,
      endingVersion: Option[Long]
  ): DeltaTableFiles = {
    // This is not used anywhere.
    DeltaTableFiles(
      0, Protocol(0), metadata, Nil, Nil, Nil, Nil, respondedFormat = RESPONSE_FORMAT_PARQUET
    )
  }

  override def getCDFFiles(
      table: Table,
      cdfOptions: Map[String, String],
      includeHistoricalMetadata: Boolean): DeltaTableFiles = {
    val addFiles: Seq[AddFileForCDF] = Seq(
      AddFileForCDF("cdf_add1.parquet", "cdf_add1", Map.empty, 100, 1, 1000)
    )
    val cdcFiles: Seq[AddCDCFile] = Seq(
      // Return one cdc file from version 2, and two files with version 3.
      // This should result in two partition directories.
      AddCDCFile("cdf_cdc1.parquet", "cdf_cdc1", Map.empty, 200, 2, 2000),
      AddCDCFile("cdf_cdc2.parquet", "cdf_cdc2", Map.empty, 300, 3, 3000),
      AddCDCFile("cdf_cdc2.parquet", "cdf_cdc3", Map.empty, 310, 3, 3000)
    )
    val removeFiles: Seq[RemoveFile] = Seq(
      // Return files with same version but different timestamps.
      // This should result in two partition directories.
      RemoveFile("cdf_rem1.parquet", "cdf_rem1", Map.empty, 400, 4, 4000),
      RemoveFile("cdf_rem2.parquet", "cdf_rem2", Map.empty, 420, 4, 4200)
    )
    DeltaTableFiles(
      version = 0,
      protocol = Protocol(0),
      metadata = metadata,
      files = Nil,
      addFiles = addFiles,
      cdfFiles = cdcFiles,
      removeFiles = removeFiles,
      respondedFormat = RESPONSE_FORMAT_PARQUET
    )
  }

  override def getProfileProvider: DeltaSharingProfileProvider = profileProvider

  def clear(): Unit = {
    TestDeltaSharingClient.limits = Nil
    TestDeltaSharingClient.jsonPredicateHints = Nil
    TestDeltaSharingClient.numMetadataCalled = 0
  }
}

class TestDeltaSharingProfileProvider extends DeltaSharingProfileProvider {
  override def getProfile: DeltaSharingProfile = null

  override def getCustomTablePath(tablePath: String): String = "prefix." + tablePath
}

object TestDeltaSharingClient {
  var limits = Seq.empty[Long]
  var jsonPredicateHints = Seq.empty[String]

  var numMetadataCalled = 0

  val TESTING_TIMESTAMP = "2022-01-01 00:00:00.0"
}
