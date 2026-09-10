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
import java.sql.Timestamp

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkEnv
import org.apache.spark.delta.sharing.{CachedTableManager, PreSignedUrlCache}
import org.apache.spark.sql.{DeltaSharingScanUtils, QueryTest, Row}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Literal}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.sparkproject.jetty.server.Server
import org.sparkproject.jetty.servlet.{DefaultServlet, ServletContextHandler, ServletHolder}

import io.delta.sharing.client.{DeltaSharingFileSystem, DeltaSharingRestClient}
import io.delta.sharing.client.model.{
  AddFile,
  DeltaTableFiles,
  DeltaTableMetadata,
  Format,
  Metadata,
  Protocol,
  Table
}
import io.delta.sharing.spark.TestUtils.withTempDir

class RemoteDeltaCDFRelationSuite extends QueryTest with SharedSparkSession {
  override def beforeAll(): Unit = {
    super.beforeAll()
    PreSignedUrlCache.registerIfNeeded(SparkEnv.get)
  }

  private val table = Table("view", "schema", "share")
  private val cdfOptions = Map(
    DeltaSharingOptions.CDF_START_TIMESTAMP -> "2026-01-01T00:00:00Z",
    DeltaSharingOptions.CDF_END_TIMESTAMP -> "2026-01-02T00:00:00Z")
  private val viewSchema = StructType(Seq(
    StructField("value", StringType),
    StructField("part", StringType),
    StructField("_change_type", StringType),
    StructField("_commit_timestamp", TimestampType)))
  private val viewMetadata = Metadata(
    id = "view-id",
    format = Format(),
    schemaString = viewSchema.json,
    partitionColumns = Seq("part"))

  for {
    ioCacheEnabled <- Seq(false, true)
    empty <- Seq(false, true)
    wrapperMetadata <- Seq(false, true)
  } {
    test(s"Parquet view CDF: ioCache=$ioCacheEnabled, empty=$empty, " +
      s"wrapperMetadata=$wrapperMetadata") {
      withSQLConf("spark.delta.sharing.client.sparkParquetIOCache.enabled" ->
        ioCacheEnabled.toString) {
        withTempDir { dir =>
          val expected = Seq(
            Row("first", "a", "delete", Timestamp.valueOf("2026-01-01 01:00:00")),
            Row("second", "z", "update_postimage", Timestamp.valueOf("2026-01-01 02:00:00")))
          val input = spark.createDataFrame(
            spark.sparkContext.parallelize(expected), viewSchema)
          if (!empty) {
            Seq("a", "z").foreach { part =>
              input.filter(col("part") === part).drop("part").coalesce(1)
                .write.parquet(new File(dir, part).getAbsolutePath)
            }
          }

          val server = new Server(0)
          val context = new ServletContextHandler()
          context.setContextPath("/")
          val servlet = new ServletHolder(new DefaultServlet())
          servlet.setInitParameter("resourceBase", dir.toURI.toString)
          context.addServlet(servlet, "/")
          server.setHandler(context)
          server.start()
          try {
            val files = if (empty) Nil else Seq("a", "z").map { part =>
              val file = new File(dir, part).listFiles().find(_.getName.endsWith(".parquet")).get
              AddFile(
                url = s"http://127.0.0.1:${server.getURI.getPort}/$part/${file.getName}",
                id = part,
                partitionValues = Map("part" -> part),
                size = file.length(),
                version = if (wrapperMetadata) Long.box(99L) else null,
                timestamp = if (wrapperMetadata) Long.box(9999L) else null)
            }
            val response = DeltaTableFiles(
              version = 0L,
              protocol = Protocol(1),
              metadata = viewMetadata,
              files = files,
              respondedFormat = DeltaSharingRestClient.RESPONSE_FORMAT_PARQUET,
              isVersionlessCDF = true)
            var cdfCalls = 0
            val client = new TestDeltaSharingClient() {
              override def getCDFFiles(
                  requestedTable: Table,
                  options: Map[String, String],
                  includeHistoricalMetadata: Boolean,
                  fileIdHash: Option[String],
                  includeHistoricalProtocol: Boolean = false): DeltaTableFiles = {
                assert(requestedTable == table)
                assert(options == cdfOptions)
                assert(!includeHistoricalMetadata)
                cdfCalls += 1
                response
              }

              override def getMetadata(
                  table: Table,
                  versionAsOf: Option[Long],
                  timestampAsOf: Option[String]): DeltaTableMetadata = {
                fail("Versionless CDF must use metadata from the changes response")
              }
            }
            val path = new Path(new File(dir, "view").getAbsolutePath)
            val deltaLog = new RemoteDeltaLog(table, path, client)
            val relation = deltaLog.createRelation(None, None, cdfOptions)
              .asInstanceOf[RemoteDeltaCDFRelation]
            assert(relation.schema == viewSchema)
            assert(!relation.schema.fieldNames.contains("_commit_version"))
            val df = DeltaSharingScanUtils.ofRows(spark, LogicalRelation(relation))
            checkAnswer(df, if (empty) Nil else expected)
            checkAnswer(df.filter(col("part") === "a").select("value", "_change_type"),
              if (empty) Nil else Seq(Row("first", "delete")))
            assert(cdfCalls == 1)

            val params = relation.fileIndexParams
            val index = RemoteDeltaBatchFileIndex(params, files)
            assert(index.partitionSchema.fieldNames.sameElements(Array("part")))
            assert(index.sizeInBytes == files.map(_.size).sum)
            val partFilter = EqualTo(AttributeReference("part", StringType)(), Literal("a"))
            val selected = index.listFiles(Seq(partFilter), Nil).flatMap(_.files)
            assert(selected.size == (if (empty) 0 else 1))
            if (!empty) {
              assert(DeltaSharingFileSystem.decode(selected.head.getPath).fileId == "a")
            }
            assert(index.inputFiles.length == files.size)
            index.inputFiles.foreach { filePath =>
              val encoded = DeltaSharingFileSystem.decode(new Path(filePath))
              val file = files.find(_.id == encoded.fileId).get
              assert(CachedTableManager.INSTANCE.getPreSignedUrl(
                encoded.tablePath, encoded.fileId)._1 == file.url)
            }
          } finally {
            server.stop()
          }
        }
      }
    }
  }

  test("streaming rejects a versionless CDF response") {
    val client = new TestDeltaSharingClient(forStreaming = true) {
      override def getForStreaming(): Boolean = true

      override def getTableVersion(table: Table, startingTimestamp: Option[String]): Long = {
        if (startingTimestamp.isDefined) 0L else 1L
      }

      override def getCDFFiles(
          table: Table,
          options: Map[String, String],
          includeHistoricalMetadata: Boolean,
          fileIdHash: Option[String],
          includeHistoricalProtocol: Boolean = false): DeltaTableFiles = {
        DeltaTableFiles(
          version = 0L,
          protocol = Protocol(1),
          metadata = viewMetadata,
          respondedFormat = DeltaSharingRestClient.RESPONSE_FORMAT_PARQUET,
          isVersionlessCDF = true)
      }
    }
    val deltaLog = new RemoteDeltaLog(table, new Path("streaming-view"), client)
    val source = DeltaSharingSource(spark, deltaLog, new DeltaSharingOptions(
      Map("readChangeFeed" -> "true", "startingTimestamp" ->
        cdfOptions(DeltaSharingOptions.CDF_START_TIMESTAMP))))
    val error = intercept[UnsupportedOperationException] {
      source.latestOffset(null, source.getDefaultReadLimit)
    }
    assert(error.getMessage ==
      "Change data feed on shared views is supported only for batch reads.")
  }
}
