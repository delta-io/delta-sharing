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

import java.io.ByteArrayInputStream
import java.net.{URI, URLDecoder, URLEncoder}
import java.util.concurrent.TimeUnit

import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable
import org.apache.http.client.config.RequestConfig
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.spark.SparkEnv
import org.apache.spark.delta.sharing.{PreSignedUrlCache, PreSignedUrlFetcher}
import org.apache.spark.network.util.JavaUtils

import io.delta.sharing.spark.model.FileAction

/** Read-only file system for delta sharing log. */
private[sharing] class DeltaSharingLogFileSystem extends FileSystem {
  import DeltaSharingLogFileSystem._

  lazy private val numRetries = {
    val numRetries = getConf.getInt("spark.delta.sharing.network.numRetries", 10)
    if (numRetries < 0) {
      throw new IllegalArgumentException(
        "spark.delta.sharing.network.numRetries must not be negative")
    }
    numRetries
  }

  lazy private val timeoutInSeconds = {
    val timeoutStr = getConf.get("spark.delta.sharing.network.timeout", "120s")
    val timeoutInSeconds = JavaUtils.timeStringAs(timeoutStr, TimeUnit.SECONDS)
    if (timeoutInSeconds < 0) {
      throw new IllegalArgumentException(
        "spark.delta.sharing.network.timeout must not be negative")
    }
    if (timeoutInSeconds > Int.MaxValue) {
      throw new IllegalArgumentException(
        s"spark.delta.sharing.network.timeout is too big: $timeoutStr")
    }
    timeoutInSeconds.toInt
  }

  lazy private val httpClient = {
    val maxConnections = getConf.getInt("spark.delta.sharing.network.maxConnections", 64)
    if (maxConnections < 0) {
      throw new IllegalArgumentException(
        "spark.delta.sharing.network.maxConnections must not be negative")
    }
    val config = RequestConfig.custom()
      .setConnectTimeout(timeoutInSeconds * 1000)
      .setConnectionRequestTimeout(timeoutInSeconds * 1000)
      .setSocketTimeout(timeoutInSeconds * 1000).build()
    HttpClientBuilder.create()
      .setMaxConnTotal(maxConnections)
      .setMaxConnPerRoute(maxConnections)
      .setDefaultRequestConfig(config)
      // Disable the default retry behavior because we have our own retry logic.
      // See `RetryUtils.runWithExponentialBackoff`.
      .disableAutomaticRetries()
      .build()
  }

  private lazy val refreshThresholdMs = getConf.getLong(
    "spark.delta.sharing.executor.refreshThresholdMs",
    TimeUnit.MINUTES.toMillis(10))

  private lazy val preSignedUrlCacheRef = PreSignedUrlCache.getEndpointRefInExecutor(SparkEnv.get)

  override def getScheme: String = SCHEME

  override def getUri(): URI = URI.create(s"$SCHEME:///")

  // scalastyle:off
  val tttJson1 = """
{"commitInfo":{"timestamp":1677282365870,"userId":"7953272455820895","userName":"lin.zhou@databricks.com","operation":"WRITE","operationParameters":{"mode":"Append","partitionBy":"[]"},"notebook":{"notebookId":"493771540318175"},"clusterId":"1118-013127-82wynr8t","readVersion":0,"isolationLevel":"WriteSerializable","isBlindAppend":true,"operationMetrics":{"numFiles":"1","numOutputRows":"1","numOutputBytes":"1030"},"engineInfo":"Databricks-Runtime/12.x-snapshot-scala2.12","txnId":"a91328a4-cbbe-4347-94ab-1b009d0022c4"}}
{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
{"metaData":{"id":"8bf14108-032f-4292-a93c-6fe30e73a42b","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"age\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"birthday\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1677282362103}}
{"add":{"path":"delta-sharing:///share1.default.linzhou_test_table_two/f3c23ec1ae8aa5c9cd5b7641e801adfa/1030","partitionValues":{},"size":1030,"modificationTime":1677282366000,"dataChange":true,"stats":"{\"numRecords\":1,\"minValues\":{\"name\":\"1\",\"age\":1,\"birthday\":\"2020-01-01\"},\"maxValues\":{\"name\":\"1\",\"age\":1,\"birthday\":\"2020-01-01\"},\"nullCount\":{\"name\":0,\"age\":0,\"birthday\":0}}","tags":{"INSERTION_TIME":"1677282366000000","MIN_INSERTION_TIME":"1677282366000000","MAX_INSERTION_TIME":"1677282366000000","OPTIMIZE_TARGET_SIZE":"268435456"}}}
""".stripMargin
  val cdfJson1 = """{"commitInfo":{"timestamp":1651272634441,"userId":"7953272455820895","userName":"lin.zhou@databricks.com","operation":"WRITE","operationParameters":{"mode":"Append","partitionBy":"[]"},"notebook":{"notebookId":"3173513222201325"},"clusterId":"0819-204509-hill72","readVersion":0,"isolationLevel":"WriteSerializable","isBlindAppend":true,"operationMetrics":{"numFiles":"3","numOutputRows":"3","numOutputBytes":"3900"},"engineInfo":"Databricks-Runtime/11.x-snapshot-scala2.12","txnId":"d66d1362-4920-4c0c-ae90-7392801dca42"}}
{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
{"metaData":{"id":"16736144-3306-4577-807a-d3f899b77670","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"age\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"birthday\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{"delta.enableChangeDataFeed":"true"},"createdTime":1651272615011}}
{"add":{"path":"delta-sharing:///share8.default.cdf_table_cdf_enabled/d7ed708546dd70fdff9191b3e3d6448b/1030","partitionValues":{},"size":1030,"modificationTime":1651272634000,"dataChange":true,"stats":"{\"numRecords\":1,\"minValues\":{\"name\":\"1\",\"age\":1,\"birthday\":\"2020-01-01\"},\"maxValues\":{\"name\":\"1\",\"age\":1,\"birthday\":\"2020-01-01\"},\"nullCount\":{\"name\":0,\"age\":0,\"birthday\":0}}","tags":{"INSERTION_TIME":"1651272634000000","OPTIMIZE_TARGET_SIZE":"268435456"}}}
{"add":{"path":"delta-sharing:///share8.default.cdf_table_cdf_enabled/60d0cf57f3e4367db154aa2c36152a1f/1030","partitionValues":{},"size":1030,"modificationTime":1651272635000,"dataChange":true,"stats":"{\"numRecords\":1,\"minValues\":{\"name\":\"2\",\"age\":2,\"birthday\":\"2020-01-01\"},\"maxValues\":{\"name\":\"2\",\"age\":2,\"birthday\":\"2020-01-01\"},\"nullCount\":{\"name\":0,\"age\":0,\"birthday\":0}}","tags":{"INSERTION_TIME":"1651272634000001","OPTIMIZE_TARGET_SIZE":"268435456"}}}
{"add":{"path":"delta-sharing:///share8.default.cdf_table_cdf_enabled/a6dc5694a4ebcc9a067b19c348526ad6/1030","partitionValues":{},"size":1030,"modificationTime":1651272634000,"dataChange":true,"stats":"{\"numRecords\":1,\"minValues\":{\"name\":\"3\",\"age\":3,\"birthday\":\"2020-01-01\"},\"maxValues\":{\"name\":\"3\",\"age\":3,\"birthday\":\"2020-01-01\"},\"nullCount\":{\"name\":0,\"age\":0,\"birthday\":0}}","tags":{"INSERTION_TIME":"1651272634000002","OPTIMIZE_TARGET_SIZE":"268435456"}}}""".stripMargin
  val cdfJson2 = """{"cdc":{"path":"delta-sharing:///share8.default.cdf_table_cdf_enabled/6521ba910108d4b54d27beaa9fc2373f/1301","partitionValues":{},"size":1301,"dataChange":false}}
{"commitInfo":{"timestamp":1651272654866,"userId":"7953272455820895","userName":"lin.zhou@databricks.com","operation":"DELETE","operationParameters":{"predicate":""},"notebook":{"notebookId":"3173513222201325"},"clusterId":"0819-204509-hill72","readVersion":1,"isolationLevel":"WriteSerializable","isBlindAppend":false,"operationMetrics":{"numRemovedFiles":"1","numCopiedRows":"1","numAddedChangeFiles":"1","executionTimeMs":"1536","numDeletedRows":"1","scanTimeMs":"916","numAddedFiles":"0","rewriteTimeMs":"618"},"engineInfo":"Databricks-Runtime/11.x-snapshot-aarch64-scala2.12","txnId":"336afaea-72f4-46a4-9c69-809a90b3bdbd"}}""".stripMargin
  val cdfJson3 = """{"cdc":{"path":"delta-sharing:///share8.default.cdf_table_cdf_enabled/2508998dce55bd726369e53761c4bc3f/1416","partitionValues":{},"size":1416,"dataChange":false}}
{"commitInfo":{"timestamp":1651272659127,"userId":"7953272455820895","userName":"lin.zhou@databricks.com","operation":"UPDATE","operationParameters":{"predicate":"(age#15119 = 2)"},"notebook":{"notebookId":"3173513222201325"},"clusterId":"0819-204509-hill72","readVersion":2,"isolationLevel":"WriteSerializable","isBlindAppend":false,"operationMetrics":{"numRemovedFiles":"1","numCopiedRows":"0","numAddedChangeFiles":"1","executionTimeMs":"1222","scanTimeMs":"99","numAddedFiles":"1","numUpdatedRows":"1","rewriteTimeMs":"1119"},"engineInfo":"Databricks-Runtime/11.x-snapshot-aarch64-scala2.12","txnId":"4c39d77a-4105-4df9-aef8-2353411dd622"}}""".stripMargin
  // cdf_table_with_partition
  val pJson0 = """{"protocol":{"minReaderVersion":1,"minWriterVersion":4}}
{"metaData":{"id":"e21eb083-6976-4159-90f2-ad88d06b7c7f","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"age\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"birthday\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":["birthday"],"configuration":{"delta.enableChangeDataFeed":"true"},"createdTime":1651614964678}}
{"commitInfo":{"timestamp":1651614964787,"userId":"7953272455820895","userName":"lin.zhou@databricks.com","operation":"CREATE TABLE","operationParameters":{"isManaged":"false","description":null,"partitionBy":"[\"birthday\"]","properties":"{\"delta.enableChangeDataFeed\":\"true\"}"},"notebook":{"notebookId":"3173513222201325"},"clusterId":"0819-204509-hill72","isolationLevel":"WriteSerializable","isBlindAppend":true,"operationMetrics":{},"engineInfo":"Databricks-Runtime/11.x-snapshot-aarch64-scala2.12","txnId":"6da4e212-f9a8-44a0-8173-6d0beb82180a"}}""".stripMargin
  val pJson1 = """{"add":{"path":"delta-sharing:///share8.default.cdf_table_with_partition/a04d61f17541fac1f9b5df5b8d26fff8/791","partitionValues":{"birthday":"2020-01-01"},"size":791,"modificationTime":1651614979000,"dataChange":true,"stats":"{\"numRecords\":1,\"minValues\":{\"name\":\"1\",\"age\":1},\"maxValues\":{\"name\":\"1\",\"age\":1},\"nullCount\":{\"name\":0,\"age\":0}}","tags":{"INSERTION_TIME":"1651614979000000","OPTIMIZE_TARGET_SIZE":"268435456"}}}
{"add":{"path":"delta-sharing:///share8.default.cdf_table_with_partition/f206a7168597c4db5956b2b11ed5cbb2/791","partitionValues":{"birthday":"2020-01-01"},"size":791,"modificationTime":1651614979000,"dataChange":true,"stats":"{\"numRecords\":1,\"minValues\":{\"name\":\"2\",\"age\":2},\"maxValues\":{\"name\":\"2\",\"age\":2},\"nullCount\":{\"name\":0,\"age\":0}}","tags":{"INSERTION_TIME":"1651614979000001","OPTIMIZE_TARGET_SIZE":"268435456"}}}
{"add":{"path":"delta-sharing:///share8.default.cdf_table_with_partition/9410d65d7571842eb7fb6b1ac01af372/791","partitionValues":{"birthday":"2020-03-03"},"size":791,"modificationTime":1651614979000,"dataChange":true,"stats":"{\"numRecords\":1,\"minValues\":{\"name\":\"3\",\"age\":3},\"maxValues\":{\"name\":\"3\",\"age\":3},\"nullCount\":{\"name\":0,\"age\":0}}","tags":{"INSERTION_TIME":"1651614979000002","OPTIMIZE_TARGET_SIZE":"268435456"}}}
{"commitInfo":{"timestamp":1651614979084,"userId":"7953272455820895","userName":"lin.zhou@databricks.com","operation":"WRITE","operationParameters":{"mode":"Append","partitionBy":"[]"},"notebook":{"notebookId":"3173513222201325"},"clusterId":"0819-204509-hill72","readVersion":0,"isolationLevel":"WriteSerializable","isBlindAppend":true,"operationMetrics":{"numFiles":"3","numOutputRows":"3","numOutputBytes":"2373"},"engineInfo":"Databricks-Runtime/11.x-snapshot-aarch64-scala2.12","txnId":"0b6ba445-be36-42dc-9f41-d1a9c31bf977"}}""".stripMargin
  val pJson2 ="""{"remove":{"path":"birthday=2020-01-01/part-00001-b23096f0-d3db-4286-a447-0a436c0a9c9d.c000.snappy.parquet","deletionTimestamp":1651614985785,"dataChange":true,"extendedFileMetadata":true,"partitionValues":{"birthday":"2020-01-01"},"size":791,"tags":{"INSERTION_TIME":"1651614979000001","OPTIMIZE_TARGET_SIZE":"268435456"}}}
{"add":{"path":"delta-sharing:///share8.default.cdf_table_with_partition/b2e140e30d29e503bfae89ad06ce5361/1008","partitionValues":{"birthday":"2020-02-02"},"size":1008,"modificationTime":1651614986000,"dataChange":true,"stats":"{\"numRecords\":1,\"minValues\":{\"name\":\"2\",\"age\":2},\"maxValues\":{\"name\":\"2\",\"age\":2},\"nullCount\":{\"name\":0,\"age\":0,\"_change_type\":1}}","tags":{"INSERTION_TIME":"1651614979000001","OPTIMIZE_TARGET_SIZE":"268435456"}}}
{"cdc":{"path":"_change_data/birthday=2020-01-01/cdc-00000-be11af93-5494-4809-856e-8dbb20bbb172.c000.snappy.parquet","partitionValues":{"birthday":"2020-01-01"},"size":1125,"dataChange":false}}
{"cdc":{"path":"_change_data/birthday=2020-02-02/cdc-00000-1c63deb8-9c6e-44f8-816e-146b834b07e5.c000.snappy.parquet","partitionValues":{"birthday":"2020-02-02"},"size":1132,"dataChange":false}}
{"commitInfo":{"timestamp":1651614985786,"userId":"7953272455820895","userName":"lin.zhou@databricks.com","operation":"UPDATE","operationParameters":{"predicate":"(age#148851 = 2)"},"notebook":{"notebookId":"3173513222201325"},"clusterId":"0819-204509-hill72","readVersion":1,"isolationLevel":"WriteSerializable","isBlindAppend":false,"operationMetrics":{"numRemovedFiles":"1","numCopiedRows":"0","numAddedChangeFiles":"2","executionTimeMs":"1388","scanTimeMs":"136","numAddedFiles":"1","numUpdatedRows":"1","rewriteTimeMs":"1252"},"engineInfo":"Databricks-Runtime/11.x-snapshot-aarch64-scala2.12","txnId":"eab2ba8f-9816-4467-b1a3-fdc34f07ae15"}}""".stripMargin
  val pJson3 = """{"protocol":{"minReaderVersion":1,"minWriterVersion":4}}
{"metaData":{"id":"e21eb083-6976-4159-90f2-ad88d06b7c7f","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"age\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"birthday\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":["birthday"],"configuration":{"delta.enableChangeDataFeed":"true"},"createdTime":1651614964678}}
{"remove":{"path":"birthday=2020-03-03/part-00002-dcf46026-980e-4599-8232-8ad11900266c.c000.snappy.parquet","deletionTimestamp":1651614993581,"dataChange":true,"extendedFileMetadata":true,"partitionValues":{"birthday":"2020-03-03"},"size":791,"tags":{"INSERTION_TIME":"1651614979000002","OPTIMIZE_TARGET_SIZE":"268435456"}}}
{"commitInfo":{"timestamp":1651614993693,"userId":"","userName":"lin.zhou@databricks.com","operation":"DELETE","operationParameters":{"predicate":""},"notebook":{"notebookId":"3173513222201325"},"clusterId":"0819-204509-hill72","readVersion":2,"isolationLevel":"WriteSerializable","isBlindAppend":false,"operationMetrics":{"numRemovedFiles":"1","numAddedChangeFiles":"0","executionTimeMs":"110","scanTimeMs":"110","rewriteTimeMs":"0"},"engineInfo":"Databricks-Runtime/11.x-snapshot-aarch64-scala2.12","txnId":"afd8597c-bfe5-465e-b175-851f83adcd39"}}""".stripMargin
  // scalastyle:on
  override def open(f: Path, bufferSize: Int): FSDataInputStream = {
    // scalastyle:off println
    consolePrintln(s"----[linzhou]----open:${f}")
    if (f.toString == "delta-sharing-log:/linzhou_test_table_two/_delta_log/_last_checkpoint") {
      consolePrintln(s"----[linzhou]----throwing exception for last_checkpoint")
      throw new UnsupportedOperationException("checkpoint.crc")
    } else if (f.toString ==
      "delta-sharing-log:/linzhou_test_table_two/_delta_log/00000000000000000001.crc") {
      consolePrintln(s"----[linzhou]----throwing exception for 1.crc")
      throw new UnsupportedOperationException("00001.crc")
    } else if (f.toString ==
      "delta-sharing-log:/linzhou_test_table_two/_delta_log/00000000000000000006.json") {
      consolePrintln(s"----[linzhou]----returning 6.json:${tttJson1}")
      return new FSDataInputStream(new SeekableByteArrayInputStream(tttJson1.getBytes(), "6.json"))
    } else if (f.toString ==
      "delta-sharing-log:/cdf_table_cdf_enabled/_delta_log/00000000000000000001.json") {
      consolePrintln(s"----[linzhou]----returning cdf 1.json:${cdfJson1.length}")
      return new FSDataInputStream(new SeekableByteArrayInputStream(
        cdfJson1.getBytes(), "cdf_1.json"))
    } else if (f.toString ==
      "delta-sharing-log:/cdf_table_cdf_enabled/_delta_log/00000000000000000002.json") {
      consolePrintln(s"----[linzhou]----returning cdf 2.json:${cdfJson2.length}")
      return new FSDataInputStream(new SeekableByteArrayInputStream(
        cdfJson2.getBytes(), "cdf_2.json"))
    } else if (f.toString ==
      "delta-sharing-log:/cdf_table_cdf_enabled/_delta_log/00000000000000000003.json") {
      consolePrintln(s"----[linzhou]----returning cdf 3.json:${cdfJson3.length}")
      return new FSDataInputStream(new SeekableByteArrayInputStream(
        cdfJson3.getBytes(), "cdf_3.json"))
    } else if (f.toString ==
      "delta-sharing-log:/cdf_table_cdf_enabled/_delta_log/00000000000000000001.crc") {
      consolePrintln(s"----[linzhou]----throwing exception for 1.crc")
      throw new UnsupportedOperationException("00001.crc")
    } else if (f.toString ==
      "delta-sharing-log:/cdf_table_with_partition/_delta_log/00000000000000000000.json") {
      consolePrintln(s"----[linzhou]----returning partition 0.json:${pJson0.length}")
      return new FSDataInputStream(new SeekableByteArrayInputStream(
        pJson0.getBytes(), "partition_0.json"))
    } else if (f.toString ==
      "delta-sharing-log:/cdf_table_with_partition/_delta_log/00000000000000000001.json") {
      consolePrintln(s"----[linzhou]----returning partition 1.json:${pJson1.length}")
      return new FSDataInputStream(new SeekableByteArrayInputStream(
        pJson1.getBytes(), "partition_1.json"))
    } else if (f.toString ==
      "delta-sharing-log:/cdf_table_with_partition/_delta_log/00000000000000000002.json") {
      consolePrintln(s"----[linzhou]----returning partition 2.json:${pJson2.length}")
      return new FSDataInputStream(new SeekableByteArrayInputStream(
        pJson2.getBytes(), "partition_2.json"))
    } else if (f.toString ==
      "delta-sharing-log:/cdf_table_with_partition/_delta_log/00000000000000000003.json") {
      consolePrintln(s"----[linzhou]----returning partition 3.json:${pJson3.length}")
      return new FSDataInputStream(new SeekableByteArrayInputStream(
        pJson3.getBytes(), "partition_3.json"))
    }
    consolePrintln(s"----[linzhou]----returning emptry for :${f.toString}")
    new FSDataInputStream(new SeekableByteArrayInputStream("".getBytes(), f.toString))
  }

  def consolePrintln(str: String): Unit = if (true) { Console.println(str) }

  override def create(
    f: Path,
    permission: FsPermission,
    overwrite: Boolean,
    bufferSize: Int,
    replication: Short,
    blockSize: Long,
    progress: Progressable): FSDataOutputStream = {
    consolePrintln(s"----[linzhou]----create:${f}")
    throw new UnsupportedOperationException("create")
  }

  override def append(f: Path, bufferSize: Int, progress: Progressable): FSDataOutputStream = {
    consolePrintln(s"----[linzhou]----append:${f}")
    throw new UnsupportedOperationException("append")
  }

  override def rename(src: Path, dst: Path): Boolean = {
    consolePrintln(s"----[linzhou]----rename:${src}")
    throw new UnsupportedOperationException("rename")
  }

  override def delete(f: Path, recursive: Boolean): Boolean = {
    consolePrintln(s"----[linzhou]----delete:${f}")
    throw new UnsupportedOperationException("delete")
  }

  override def exists(f: Path): Boolean = {
    consolePrintln(s"----[linzhou]----exists:${f}")
    return f.toString == "delta-sharing-log:/linzhou_test_table_two/_delta_log" ||
      f.toString == "delta-sharing-log:/cdf_table_cdf_enabled/_delta_log" ||
      f.toString == "delta-sharing-log:/cdf_table_with_partition/_delta_log"
  }

  override def listStatus(f: Path): Array[FileStatus] = {
    consolePrintln(s"----[linzhou]----listStatus:${f}")
    if (f.toString == "delta-sharing-log:/linzhou_test_table_two/_delta_log") {
      val a = Array(
        new FileStatus(0, false, 0, 1, 0, new Path(
          "delta-sharing-log:/linzhou_test_table_two/_delta_log/00000000000000000000.json")),
        new FileStatus(0, false, 0, 1, 0, new Path(
          "delta-sharing-log:/linzhou_test_table_two/_delta_log/00000000000000000001.json")),
        new FileStatus(0, false, 0, 1, 0, new Path(
          "delta-sharing-log:/linzhou_test_table_two/_delta_log/00000000000000000002.json")),
        new FileStatus(0, false, 0, 1, 0, new Path(
          "delta-sharing-log:/linzhou_test_table_two/_delta_log/00000000000000000003.json")),
        new FileStatus(0, false, 0, 1, 0, new Path(
          "delta-sharing-log:/linzhou_test_table_two/_delta_log/00000000000000000004.json")),
        new FileStatus(0, false, 0, 1, 0, new Path(
          "delta-sharing-log:/linzhou_test_table_two/_delta_log/00000000000000000005.json")),
      new FileStatus(tttJson1.length, false, 0, 1, 0, new Path(
        "delta-sharing-log:/linzhou_test_table_two/_delta_log/00000000000000000006.json"))
      )
      consolePrintln(s"----[linzhou]----listing:${a}")
      return a
    } else if (f.toString == "delta-sharing-log:/cdf_table_cdf_enabled/_delta_log") {
      val a = Array(
        new FileStatus(0, false, 0, 1, 0, new Path(
          "delta-sharing-log:/cdf_table_cdf_enabled/_delta_log/00000000000000000000.json")),
        new FileStatus(cdfJson1.length, false, 0, 1, 1651272635000L, new Path(
          "delta-sharing-log:/cdf_table_cdf_enabled/_delta_log/00000000000000000001.json")),
        new FileStatus(cdfJson2.length, false, 0, 1, 1651272655000L, new Path(
          "delta-sharing-log:/cdf_table_cdf_enabled/_delta_log/00000000000000000002.json")),
        new FileStatus(cdfJson3.length, false, 0, 1, 1651272660000L, new Path(
          "delta-sharing-log:/cdf_table_cdf_enabled/_delta_log/00000000000000000003.json"))
      )
      consolePrintln(s"----[linzhou]----listing:${a}")
      return a
    } else if (f.toString == "delta-sharing-log:/cdf_table_with_partition/_delta_log") {
      val a = Array(
        new FileStatus(pJson0.length, false, 0, 1, 0, new Path(
          "delta-sharing-log:/cdf_table_with_partition/_delta_log/00000000000000000000.json")),
        new FileStatus(pJson1.length, false, 0, 1, 1651614980000L, new Path(
          "delta-sharing-log:/cdf_table_with_partition/_delta_log/00000000000000000001.json")),
        new FileStatus(pJson2.length, false, 0, 1, 1651614986000L, new Path(
          "delta-sharing-log:/cdf_table_with_partition/_delta_log/00000000000000000002.json")),
        new FileStatus(pJson3.length, false, 0, 1, 1651614994000L, new Path(
          "delta-sharing-log:/cdf_table_with_partition/_delta_log/00000000000000000003.json"))
      )
      consolePrintln(s"----[linzhou]----listing:${a}")
      return a
    }
    throw new UnsupportedOperationException("listStatus")
  }

  override def listStatusIterator(f: Path): RemoteIterator[FileStatus] = {
    consolePrintln(s"----[linzhou]----listStatusIterator:${f}")
    throw new UnsupportedOperationException("listStatusIterator")
  }

  override def setWorkingDirectory(new_dir: Path): Unit =
    throw new UnsupportedOperationException("setWorkingDirectory")

  override def getWorkingDirectory: Path = new Path(getUri)

  override def mkdirs(f: Path, permission: FsPermission): Boolean = {
    consolePrintln(s"----[linzhou]----mkdirs:${f},${permission}")
    throw new UnsupportedOperationException("mkdirs")
  }

  override def getFileStatus(f: Path): FileStatus = {
    consolePrintln(s"----[linzhou]----getFileStatus:${f}")
    val fileSize = 0
    new FileStatus(fileSize, false, 0, 1, 0, f)
  }

  override def finalize(): Unit = {
    consolePrintln(s"----[linzhou]----finalize")
    try super.finalize() finally close()
  }

  override def close(): Unit = {
    try super.close() finally httpClient.close()
  }
}

private[sharing] object DeltaSharingLogFileSystem {

  val SCHEME = "delta-sharing-log"

  case class DeltaSharingPath(tablePath: String, fileId: String, fileSize: Long) {

    /**
     * Convert `DeltaSharingPath` to a `Path` in the following format:
     *
     * ```
     * delta-sharing-log:///<url encoded table path>/<url encoded file id>/<size>
     * ```
     *
     * This format can be decoded by `DeltaSharingLogFileSystem.decode`.
     */
    def toPath: Path = {
      val encodedTablePath = URLEncoder.encode(tablePath, "UTF-8")
      val encodedFileId = URLEncoder.encode(fileId, "UTF-8")
      new Path(s"$SCHEME:///$encodedTablePath/$encodedFileId/$fileSize")
    }
  }

  def encode(tablePath: String, action: FileAction): Path = {
    DeltaSharingPath(tablePath, action.id, action.size).toPath
  }

  def decode(path: Path): DeltaSharingPath = {
    val encodedPath = path.toString
      .stripPrefix(s"$SCHEME:///")
      .stripPrefix(s"$SCHEME:/")
    val Array(encodedTablePath, encodedFileId, sizeString) = encodedPath.split("/")
    DeltaSharingPath(
      URLDecoder.decode(encodedTablePath, "UTF-8"),
      URLDecoder.decode(encodedFileId, "UTF-8"),
      sizeString.toLong)
  }

  /**
   * A ByteArrayInputStream that implements interfaces required by FSDataInputStream.
   */
  private class SeekableByteArrayInputStream(bytes: Array[Byte], fileName: String)
    extends ByteArrayInputStream(bytes) with Seekable with PositionedReadable {
    assert(available == bytes.length)

    def consolePrintln(str: String): Unit = if (false) { Console.println(str) }

    override def seek(pos: Long): Unit = {
      consolePrintln(s"----[linzhou]------seek pos: $pos, avail:$available, for $fileName")
      if (mark != 0) {
        consolePrintln(s"----[linzhou]------seek exception, mark: $mark")
        throw new IllegalStateException("Cannot seek if mark is set")
      }
      consolePrintln(s"----[linzhou]------seek reset")
      reset()
      skip(pos)
    }

    override def seekToNewSource(pos: Long): Boolean = {
      consolePrintln(s"----[linzhou]------seekToNewSource, for $fileName")
      false  // there aren't multiple sources available
    }

    override def getPos(): Long = {
      consolePrintln(s"----[linzhou]------getPos, for $fileName")
      bytes.length - available
    }

    override def read(buffer: Array[Byte], offset: Int, length: Int): Int = {
      consolePrintln(s"----[linzhou]------read pos:$pos, off: $offset, len: $length, for $fileName")
//      if (pos >= bytes.length) {
//        return -1
//      }
//      val readSize = math.min(length, bytes.length - pos).toInt
//      System.arraycopy(bytes, pos.toInt, buffer, offset, readSize)
      val readSize = super.read(buffer, offset, length)
      consolePrintln(s"----[linzhou]------after read pos:$pos, readSize: $readSize")
      readSize
    }

    override def read(pos: Long, buffer: Array[Byte], offset: Int, length: Int): Int = {
      consolePrintln(s"----[linzhou]------read-pos input:$pos, offset: $offset, for $fileName")
      if (pos >= bytes.length) {
        return -1
      }
      val readSize = math.min(length, bytes.length - pos).toInt
      System.arraycopy(bytes, pos.toInt, buffer, offset, readSize)
      readSize
    }

    override def readFully(pos: Long, buffer: Array[Byte], offset: Int, length: Int): Unit = {
      consolePrintln(s"----[linzhou]--------readFully, offset:${offset}, for $fileName")
      System.arraycopy(bytes, pos.toInt, buffer, offset, length)
    }

    override def readFully(pos: Long, buffer: Array[Byte]): Unit = {
      consolePrintln(s"----[linzhou]--------readFully, for $fileName")
      System.arraycopy(bytes, pos.toInt, buffer, 0, buffer.length)
    }
  }
}
