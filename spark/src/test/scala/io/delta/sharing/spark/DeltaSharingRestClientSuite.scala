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

import io.delta.sharing.spark.model.{AddFile, Format, Metadata, Protocol, Table}

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

  integrationTest("getFiles") {
    val client = new DeltaSharingRestClient(testProfileProvider, sslTrustAll = true)
    try {
      val tableFiles =
        client.getFiles(Table(name = "table2", schema = "default", share = "share2"), Nil, None)
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
}
