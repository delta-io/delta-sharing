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

import java.io.File
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import java.sql.Timestamp
import java.util.concurrent.TimeUnit

import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.test.SharedSparkSession

class ViewCDFSuite extends QueryTest with SharedSparkSession {
  override def beforeAll(): Unit = {
    super.beforeAll()
    DeltaSharingDataSource.setupFileSystem(spark.sqlContext)
    // A cached filesystem can retain the RPC endpoint of a previous suite's stopped SparkContext.
    closeSharingFileSystem()
  }

  override def afterAll(): Unit = {
    try {
      closeSharingFileSystem()
    } finally {
      super.afterAll()
    }
  }

  private def closeSharingFileSystem(): Unit = {
    new Path("delta-sharing:///").getFileSystem(spark.sessionState.newHadoopConf()).close()
  }

  test("view CDF with parquet response format") {
    val serverDir = Files.createTempDirectory("view-sharing-server")
    val readyFile = serverDir.resolve("ready")
    val stopFile = serverDir.resolve("stop")
    val logFile = serverDir.resolve("server.log").toFile
    val process = new ProcessBuilder(
      "./build/sbt", "-batch",
      s"server / Test / runMain io.delta.sharing.server.TestViewSharingServer " +
        s"$readyFile $stopFile")
      .directory(new File("..").getCanonicalFile)
      .redirectErrorStream(true)
      .redirectOutput(logFile)
      .start()
    try {
      val deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(3)
      while (!Files.exists(readyFile) && process.isAlive && System.nanoTime() < deadline) {
        Thread.sleep(100)
      }
      assert(Files.exists(readyFile),
        s"View sharing server failed to start:\n${FileUtils.readFileToString(logFile, UTF_8)}")
      val sharingOptions = Map(
        "endpoint" -> new String(Files.readAllBytes(readyFile), UTF_8),
        "bearerToken" -> "token",
        "shareCredentialsVersion" -> "1")

      def loadChanges(objectName: String): DataFrame = {
        spark.read.format("deltaSharing")
          .options(sharingOptions)
          .option("responseFormat", "parquet")
          .option("readChangeFeed", "true")
          .option("startingTimestamp", "2022-05-09T00:00:00Z")
          .option("endingTimestamp", "2022-05-11T00:00:00Z")
          .load(s"view_share.default.$objectName")
      }

      val viewChanges = loadChanges("view")
      assert(viewChanges.columns.sameElements(
        Array("value", "_change_type", "_commit_timestamp")))
      val commitTimestamp = new Timestamp(1652140800000L)
      checkAnswer(viewChanges, Seq(
        Row("first", "insert", commitTimestamp),
        Row("second", "delete", commitTimestamp)))

      // The control table receives the same client capability but must retain commit versions.
      val tableChanges = loadChanges("table")
      assert(tableChanges.columns.sameElements(
        Array("value", "_commit_version", "_commit_timestamp", "_change_type")))
      checkAnswer(tableChanges, Seq(
        Row("first", 1L, 1652140800000L, "insert"),
        Row("second", 1L, 1652140800000L, "delete")))
    } finally {
      Files.createFile(stopFile)
      try {
        if (!process.waitFor(30, TimeUnit.SECONDS)) {
          process.destroy()
          if (!process.waitFor(10, TimeUnit.SECONDS)) {
            process.destroyForcibly()
            process.waitFor(10, TimeUnit.SECONDS)
          }
        }
      } finally {
        FileUtils.deleteDirectory(serverDir.toFile)
      }
    }
  }
}
