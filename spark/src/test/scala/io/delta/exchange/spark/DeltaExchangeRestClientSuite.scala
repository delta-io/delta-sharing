package io.delta.exchange.client

import io.delta.exchange.client.model.{AddFile, Format, Metadata, Protocol, Table}
import org.scalatest.{FunSuite, Ignore}

@Ignore
class DeltaExchangeRestClientSuite extends FunSuite {

  val profileProvider = new DeltaExchangeProfileProvider {
    override def getProfile: DeltaExchangeProfile =
      DeltaExchangeProfile("https://localhost:443/delta-exchange", "dapi5e3574ec767ca1548ae5bbed1a2dc04d")
  }

  test("listAllTables") {
    val client = new DeltaExchangeRestClient(profileProvider, sslTrustAll = true)
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

  test("getMetadata") {
    val client = new DeltaExchangeRestClient(profileProvider, sslTrustAll = true)
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

  test("getFiles") {
    val client = new DeltaExchangeRestClient(profileProvider, sslTrustAll = true)
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
      // TODO verify stats
      val expectedFiles = Seq(
        AddFile(
          url = tableFiles.files(0).url,
          id = "date=2021-04-28/part-00000-8b0086f2-7b27-4935-ac5a-8ed6215a6640.c000.snappy.parquet",
          partitionValues = Map("date" -> "2021-04-28"),
          size = 573,
          stats = """{"numRecords":1,"minValues":{"eventTime":"2021-04-28T23:33:57.955Z"},"maxValues":{"eventTime":"2021-04-28T23:33:57.955Z"},"nullCount":{"eventTime":0}}""",
        ),
        AddFile(
          url = tableFiles.files(1).url,
          id = "date=2021-04-28/part-00000-591723a8-6a27-4240-a90e-57426f4736d2.c000.snappy.parquet",
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
