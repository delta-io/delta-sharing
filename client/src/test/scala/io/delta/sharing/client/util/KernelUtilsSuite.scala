package io.delta.sharing.client.util

import org.apache.spark.SparkFunSuite
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path


import io.delta.kernel.client.DefaultTableClient
import io.delta.kernel.client.TableClient
import io.delta.kernel.Table
import io.delta.sharing.client.util.KernelUtils
import io.delta.sharing.client.DeltaSharingFileSystem


class KernelUtilsSuite extends SparkFunSuite {
  test("deserialize scan state and scan file") {
    val hadoopConf: Configuration = new Configuration()
    val myTableClient: TableClient = DefaultTableClient.create(hadoopConf)
  }
}
