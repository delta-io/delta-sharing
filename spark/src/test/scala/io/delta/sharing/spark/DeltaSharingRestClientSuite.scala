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

import java.sql.Timestamp

import io.delta.sharing.spark.model.{
  AddCDCFile,
  AddFile,
  AddFileForCDF,
  Format,
  Metadata,
  Protocol,
  Table
}
import io.delta.sharing.spark.util.UnexpectedHttpStatus

// scalastyle:off maxLineLength
class DeltaSharingRestClientSuite extends DeltaSharingIntegrationTest {

  integrationTest("listAllTables") {
    val client = new DeltaSharingRestClient(testProfileProvider, sslTrustAll = true)
    try {
      val expected = Set(
        Table(name = "table1", schema = "default", share = "share1"),
        Table(name = "table2", schema = "default", share = "share2"),
        Table(name = "table3", schema = "default", share = "share1"),
        Table(name = "table4", schema = "default", share = "share3"),
        Table(name = "table5", schema = "default", share = "share3"),
        Table(name = "table7", schema = "default", share = "share1"),
        Table(name = "table8", schema = "schema1", share = "share7"),
        Table(name = "table9", schema = "schema2", share = "share7"),
        Table(name = "cdf_table_cdf_enabled", schema = "default", share = "share1"),
        Table(name = "cdf_table_with_partition", schema = "default", share = "share1"),
        Table(name = "cdf_table_with_vacuum", schema = "default", share = "share1"),
        Table(name = "cdf_table_missing_log", schema = "default", share = "share1"),
        Table(name = "test_gzip", schema = "default", share = "share4"),
        Table(name = "table_wasb", schema = "default", share = "share_azure"),
        Table(name = "table_abfs", schema = "default", share = "share_azure"),
        Table(name = "table_gcs", schema = "default", share = "share_gcp")
      )
      assert(expected == client.listAllTables().toSet)
    } finally {
      client.close()
    }
  }

  integrationTest("getTableVersion") {
    val client = new DeltaSharingRestClient(testProfileProvider, sslTrustAll = true)
    try {
      assert(client.getTableVersion(Table(name = "table2", schema = "default", share = "share2")) == 2)
      assert(client.getTableVersion(Table(name = "table1", schema = "default", share = "share1")) == 2)
      assert(client.getTableVersion(Table(name = "table3", schema = "default", share = "share1")) == 4)
    } finally {
      client.close()
    }
  }

  integrationTest("getMetadata") {
    val client = new DeltaSharingRestClient(testProfileProvider, sslTrustAll = true)
    try {
      val tableMatadata =
        client.getMetadata(Table(name = "table2", schema = "default", share = "share2"))
      assert(Protocol(minReaderVersion = 1) == tableMatadata.protocol)
      val expectedMetadata = Metadata(
        id = "f8d5c169-3d01-4ca3-ad9e-7dc3355aedb2",
        format = Format(),
        schemaString = """{"type":"struct","fields":[{"name":"eventTime","type":"timestamp","nullable":true,"metadata":{}},{"name":"date","type":"date","nullable":true,"metadata":{}}]}""",
        partitionColumns = Seq("date"))
      assert(expectedMetadata == tableMatadata.metadata)
    } finally {
      client.close()
    }
  }

  integrationTest("getMetadata with configuration") {
    val client = new DeltaSharingRestClient(testProfileProvider, sslTrustAll = true)
    try {
      val tableMatadata =
        client.getMetadata(Table(name = "cdf_table_cdf_enabled", schema = "default", share = "share1"))
      assert(Protocol(minReaderVersion = 1) == tableMatadata.protocol)
      val expectedMetadata = Metadata(
        id = "16736144-3306-4577-807a-d3f899b77670",
        format = Format(),
        schemaString = """{"type":"struct","fields":[{"name":"name","type":"string","nullable":true,"metadata":{}},{"name":"age","type":"integer","nullable":true,"metadata":{}},{"name":"birthday","type":"date","nullable":true,"metadata":{}}]}""",
        configuration = Map("enableChangeDataFeed" -> "true"),
        partitionColumns = Nil)
      assert(expectedMetadata == tableMatadata.metadata)
    } finally {
      client.close()
    }
  }

  integrationTest("getFiles") {
    val client = new DeltaSharingRestClient(testProfileProvider, sslTrustAll = true)
    try {
      val tableFiles =
        client.getFiles(Table(name = "table2", schema = "default", share = "share2"), Nil, None, None)
      assert(Protocol(minReaderVersion = 1) == tableFiles.protocol)
      val expectedMetadata = Metadata(
        id = "f8d5c169-3d01-4ca3-ad9e-7dc3355aedb2",
        format = Format(),
        schemaString = """{"type":"struct","fields":[{"name":"eventTime","type":"timestamp","nullable":true,"metadata":{}},{"name":"date","type":"date","nullable":true,"metadata":{}}]}""",
        partitionColumns = Seq("date"))
      assert(expectedMetadata == tableFiles.metadata)
      assert(tableFiles.files.size == 2)
      val expectedFiles = Seq(
        AddFile(
          url = tableFiles.files(0).url,
          id = "9f1a49539c5cffe1ea7f9e055d5c003c",
          partitionValues = Map("date" -> "2021-04-28"),
          size = 573,
          stats = """{"numRecords":1,"minValues":{"eventTime":"2021-04-28T23:33:57.955Z"},"maxValues":{"eventTime":"2021-04-28T23:33:57.955Z"},"nullCount":{"eventTime":0}}"""
        ),
        AddFile(
          url = tableFiles.files(1).url,
          id = "cd2209b32f5ed5305922dd50f5908a75",
          partitionValues = Map("date" -> "2021-04-28"),
          size = 573,
          stats = """{"numRecords":1,"minValues":{"eventTime":"2021-04-28T23:33:48.719Z"},"maxValues":{"eventTime":"2021-04-28T23:33:48.719Z"},"nullCount":{"eventTime":0}}"""
        )
      )
      assert(expectedFiles == tableFiles.files.toList)
    } finally {
      client.close()
    }
  }

  integrationTest("getFiles with version") {
    val client = new DeltaSharingRestClient(testProfileProvider, sslTrustAll = true)
    try {
      val tableFiles = client.getFiles(
        Table(name = "cdf_table_cdf_enabled", schema = "default", share = "share1"),
        Nil,
        None,
        Some(1L))
      assert(tableFiles.files.size == 3)
      val expectedFiles = Seq(
        AddFile(
          url = tableFiles.files(0).url,
          id = "60d0cf57f3e4367db154aa2c36152a1f",
          partitionValues = Map.empty,
          size = 1030,
          stats = """{"numRecords":1,"minValues":{"name":"1","age":1,"birthday":"2020-01-01"},"maxValues":{"name":"1","age":1,"birthday":"2020-01-01"},"nullCount":{"name":0,"age":0,"birthday":0}}"""
        ),
        AddFile(
          url = tableFiles.files(1).url,
          id = "d7ed708546dd70fdff9191b3e3d6448b",
          partitionValues = Map.empty,
          size = 1030,
          stats = """{"numRecords":1,"minValues":{"name":"3","age":3,"birthday":"2020-01-01"},"maxValues":{"name":"3","age":3,"birthday":"2020-01-01"},"nullCount":{"name":0,"age":0,"birthday":0}}"""
        ),
        AddFile(
          url = tableFiles.files(2).url,
          id = "a6dc5694a4ebcc9a067b19c348526ad6",
          partitionValues = Map.empty,
          size = 1030,
          stats = """{"numRecords":1,"minValues":{"name":"2","age":2,"birthday":"2020-01-01"},"maxValues":{"name":"2","age":2,"birthday":"2020-01-01"},"nullCount":{"name":0,"age":0,"birthday":0}}"""
        )
      )
      assert(expectedFiles == tableFiles.files.toList)
    } finally {
      client.close()
    }
  }

  integrationTest("getFiles with version exception") {
    val client = new DeltaSharingRestClient(testProfileProvider, sslTrustAll = true)
    try {
      val errorMessage = intercept[UnexpectedHttpStatus] {
        client.getFiles(
          Table(name = "table1", schema = "default", share = "share1"),
          Nil,
          None,
          Some(1L)
        )
      }.getMessage
      assert(errorMessage.contains("Reading table by version is not supported because change data feed is not enabled on table: share1.default.table1"))
    } finally {
      client.close()
    }
  }

  integrationTest("getCDFFiles") {
    val client = new DeltaSharingRestClient(testProfileProvider, sslTrustAll = true)
    try {
      val cdfOptions = Map("startingVersion" -> "0", "endingVersion" -> "3")
      val tableFiles = client.getCDFFiles(
        Table(name = "cdf_table_cdf_enabled", schema = "default", share = "share1"),
        cdfOptions
      )
      assert(Protocol(minReaderVersion = 1) == tableFiles.protocol)
      val expectedMetadata = Metadata(
        id = "16736144-3306-4577-807a-d3f899b77670",
        format = Format(),
        schemaString = """{"type":"struct","fields":[{"name":"name","type":"string","nullable":true,"metadata":{}},{"name":"age","type":"integer","nullable":true,"metadata":{}},{"name":"birthday","type":"date","nullable":true,"metadata":{}}]}""",
        configuration = Map("enableChangeDataFeed" -> "true"),
        partitionColumns = Nil)
      assert(expectedMetadata == tableFiles.metadata)
      assert(tableFiles.cdfFiles.size == 2)
      val expectedCdfFiles = Seq(
        AddCDCFile(
          url = tableFiles.cdfFiles(0).url,
          id = tableFiles.cdfFiles(0).id,
          partitionValues = Map.empty,
          size = 1301,
          version = 2,
          timestamp = 1651272655000L
        ),
        AddCDCFile(
          url = tableFiles.cdfFiles(1).url,
          id = tableFiles.cdfFiles(1).id,
          partitionValues = Map.empty,
          size = 1416,
          version = 3,
          timestamp = 1651272660000L
        )
      )
      assert(expectedCdfFiles == tableFiles.cdfFiles.toList)
      assert(tableFiles.addFilesForCdf.size == 3)
      val expectedAddFiles = Seq(
        AddFileForCDF(
          url = tableFiles.addFilesForCdf(0).url,
          id = tableFiles.addFilesForCdf(0).id,
          partitionValues = Map.empty,
          size = 1030,
          stats = """{"numRecords":1,"minValues":{"name":"1","age":1,"birthday":"2020-01-01"},"maxValues":{"name":"1","age":1,"birthday":"2020-01-01"},"nullCount":{"name":0,"age":0,"birthday":0}}""",
          version = 1,
          timestamp = 1651272635000L
        ),
        AddFileForCDF(
          url = tableFiles.addFilesForCdf(1).url,
          id = tableFiles.addFilesForCdf(1).id,
          partitionValues = Map.empty,
          size = 1030,
          stats = """{"numRecords":1,"minValues":{"name":"2","age":2,"birthday":"2020-01-01"},"maxValues":{"name":"2","age":2,"birthday":"2020-01-01"},"nullCount":{"name":0,"age":0,"birthday":0}}""",
          version = 1,
          timestamp = 1651272635000L
        ),
        AddFileForCDF(
          url = tableFiles.addFilesForCdf(2).url,
          id = tableFiles.addFilesForCdf(2).id,
          partitionValues = Map.empty,
          size = 1030,
          stats = """{"numRecords":1,"minValues":{"name":"3","age":3,"birthday":"2020-01-01"},"maxValues":{"name":"3","age":3,"birthday":"2020-01-01"},"nullCount":{"name":0,"age":0,"birthday":0}}""",
          version = 1,
          timestamp = 1651272635000L
        )
      )
      assert(expectedAddFiles == tableFiles.addFilesForCdf.toList)
    } finally {
      client.close()
    }
  }

  integrationTest("getCDFFiles: cdf_table_with_vacuum") {
    val client = new DeltaSharingRestClient(testProfileProvider, sslTrustAll = true)
    try {
      val cdfOptions = Map("startingVersion" -> "0")
      val tableFiles = client.getCDFFiles(
        Table(name = "cdf_table_with_vacuum", schema = "default", share = "share1"),
        cdfOptions
      )
      assert(Protocol(minReaderVersion = 1) == tableFiles.protocol)
      assert(tableFiles.addFilesForCdf.size == 4)
      assert(tableFiles.cdfFiles.size == 2)
      assert(tableFiles.removeFiles.size == 0)
    } finally {
      client.close()
    }
  }

  integrationTest("getCDFFiles: cdf_table_missing_log") {
    val client = new DeltaSharingRestClient(testProfileProvider, sslTrustAll = true)
    try {
      val errorMessage = intercept[UnexpectedHttpStatus] {
        val cdfOptions = Map("startingVersion" -> "1")
        client.getCDFFiles(
          Table(name = "cdf_table_missing_log", schema = "default", share = "share1"),
          cdfOptions
        )
      }.getMessage
      assert(errorMessage.contains("""400 Bad Request {"errorCode":"RESOURCE_DOES_NOT_EXIST""""))
      assert(errorMessage.contains("table files missing"))
    } finally {
      client.close()
    }
  }

  integrationTest("getCDFFiles with startingTimestamp") {
    val client = new DeltaSharingRestClient(testProfileProvider, sslTrustAll = true)
    try {
      // This is to test that timestamp is correctly passed to the server and parsed.
      // The error message is expected as we are using a timestamp much larger than the latest
      // version of the table.
      val cdfOptions = Map("startingTimestamp" -> "2000-01-01 00:00:00")
      val errorMessage = intercept[UnexpectedHttpStatus] {
        val tableFiles = client.getCDFFiles(
          Table(name = "cdf_table_cdf_enabled", schema = "default", share = "share1"),
          cdfOptions
        )
      }.getMessage
      assert(errorMessage.contains("Please use a timestamp greater"))
    } finally {
      client.close()
    }
  }

  integrationTest("getCDFFiles with endingTimestamp") {
    val client = new DeltaSharingRestClient(testProfileProvider, sslTrustAll = true)
    try {
      // This is to test that timestamp is correctly passed to the server and parsed.
      // The error message is expected as we are using a timestamp much larger than the latest
      // version of the table.
      val cdfOptions = Map("startingVersion" -> "0", "endingTimestamp" -> "2100-01-01 00:00:00")
      val errorMessage = intercept[UnexpectedHttpStatus] {
        val tableFiles = client.getCDFFiles(
          Table(name = "cdf_table_cdf_enabled", schema = "default", share = "share1"),
          cdfOptions
        )
      }.getMessage
      assert(errorMessage.contains("Please use a timestamp less than or equal to"))
    } finally {
      client.close()
    }
  }

  integrationTest("getCDFFiles_exceptions") {
    val client = new DeltaSharingRestClient(testProfileProvider, sslTrustAll = true)
    try {
      val cdfOptions = Map("startingVersion" -> "0")
      val errorMessage = intercept[UnexpectedHttpStatus] {
        client.getCDFFiles(
          Table(name = "table1", schema = "default", share = "share1"),
          cdfOptions
        )
      }.getMessage
      assert(errorMessage.contains("cdf is not enabled on table share1.default.table1"))
    } finally {
      client.close()
    }
  }
}
