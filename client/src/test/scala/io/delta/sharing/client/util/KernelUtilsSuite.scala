package io.delta.sharing.client

import org.apache.spark.SparkFunSuite
import org.apache.hadoop.conf.Configuration

import io.delta.kernel.client.DefaultTableClient
import io.delta.kernel.client.TableClient
import io.delta.kernel.Table

class KernelUtilsSuite extends SparkFunSuite {
  test("read a delta table") {
    val myTablePath = "s3://delta-exchange-test/delta-exchange-test/table1/" // fully qualified table path. Ex: file:/user/tables/myTable
    val hadoopConf: Configuration = new Configuration()
    val myTableClient: TableClient = DefaultTableClient.create(hadoopConf)
    val myTable: Table = Table.forPath(myTablePath);

    val mySnapshot = myTable.getLatestSnapshot(myTableClient)
    val version = mySnapshot.getVersion(myTableClient)

    println(version)
  }
}
