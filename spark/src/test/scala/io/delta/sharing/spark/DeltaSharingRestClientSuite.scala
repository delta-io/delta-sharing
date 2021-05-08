package io.delta.sharing.spark

import io.delta.sharing.spark.model.{AddFile, Format, Metadata, Protocol, Table}

class DeltaSharingRestClientSuite extends DeltaSharingIntegrationTest {

  integrationTest("listAllTables") {
    val client = new DeltaSharingRestClient(testProfileProvider, sslTrustAll = true)
    try {
      val expected = Set(
        Table(name = "table1", schema = "default", share = "share1"),
        Table(name = "table2", schema = "default", share = "share2"),
        Table(name = "table3", schema = "default", share = "share1")
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
          stats = """{"numRecords":1,"minValues":{"eventTime":"2021-04-28T23:33:57.955Z"},"maxValues":{"eventTime":"2021-04-28T23:33:57.955Z"},"nullCount":{"eventTime":0}}""",
        ),
        AddFile(
          url = tableFiles.files(1).url,
          id = "cd2209b32f5ed5305922dd50f5908a75",
          partitionValues = Map("date" -> "2021-04-28"),
          size = 573,
          stats = """{"numRecords":1,"minValues":{"eventTime":"2021-04-28T23:33:48.719Z"},"maxValues":{"eventTime":"2021-04-28T23:33:48.719Z"},"nullCount":{"eventTime":0}}""",
        )
      )
      assert(expectedFiles == tableFiles.files.toList)
    } finally {
      client.close()
    }
  }
}
