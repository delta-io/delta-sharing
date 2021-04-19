package io.delta.exchange.spark

import java.net.{URI, URL}

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.test.SharedSparkSession

class RemoteDeltaLogSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  test("RemoteDeltaTable") {
    val uuid = java.util.UUID.randomUUID().toString
    val table = RemoteDeltaTable(s"delta://$uuid:token@databricks.com")
    assert(table == RemoteDeltaTable(new URL("https://databricks.com"), "token", uuid))
  }

  test("RemoteDeltaLog") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath
      spark.range(1, 10).write.format("delta").save(path)
      val deltaLog = DeltaLog.forTable(spark, path)
      val uuid = java.util.UUID.randomUUID().toString
      val client = new DeltaLogLocalClient(deltaLog, new DummyFileSigner)
      val remoteDeltaLog = new RemoteDeltaLog(uuid, 0, new Path(path), client)
      assert(remoteDeltaLog.snapshot.version == deltaLog.snapshot.version)
      assert(remoteDeltaLog.snapshot.metadata == deltaLog.snapshot.metadata)
      checkAnswer(
        remoteDeltaLog.snapshot.allFiles.map { add =>
          val uri = DeltaFileSystem.restoreUri(new Path(new URI(add.path)))._1
          add.copy(path = uri.toString)
        }.toDF,
        deltaLog.snapshot.allFiles.map { add =>
          add.copy(path = s"file:$path/${add.path}")
        }.toDF
      )
    }
  }

  test("local file system") {
    withTempDir { tempDir =>
      withSQLConf("delta.exchange.localClient.path" -> tempDir.getCanonicalPath) {
        val uuid = java.util.UUID.randomUUID().toString
        val localPath = new java.io.File(tempDir, uuid).getCanonicalPath
        spark.range(1, 10).write.format("delta").save(localPath)

        val path = s"delta://$uuid:token@databricks.com"
        val df = spark.read.format("delta-exchange").load(path)
        checkAnswer(df, spark.range(1, 10).toDF)
      }
    }
  }

  test("partition pruning") {
    withTempDir { tempDir =>
      withSQLConf("delta.exchange.localClient.path" -> tempDir.getCanonicalPath) {
        val uuid = java.util.UUID.randomUUID().toString
        val localPath = new java.io.File(tempDir, uuid).getCanonicalPath
        spark.range(1, 10).map(x => (x, x % 2)).toDF("c1", "c2")
          .write
          .partitionBy("c2")
          .format("delta")
          .save(localPath)

        val path = s"delta://$uuid:token@databricks.com"
        val df = spark.read.format("delta-exchange").load(path).filter("c2 = 0")
        checkAnswer(df, spark.range(1, 10).map(x => (x, x % 2)).toDF("c1", "c2").filter("c2 = 0"))
      }
    }
  }

  test("metastore table") {
    withTable("results") {
      withTempDir { tempDir =>
        withSQLConf("delta.exchange.localClient.path" -> tempDir.getCanonicalPath) {
          val uuid = java.util.UUID.randomUUID().toString
          val localPath = new java.io.File(tempDir, uuid).getCanonicalPath
          spark.range(1, 10).write.format("delta").save(localPath)

          val path = s"delta://$uuid:token@databricks.com"
          sql(s"CREATE TABLE results (id LONG) USING `delta-exchange` LOCATION '$path' " +
            s"TBLPROPERTIES ('hithere' = '1')")
          val df = spark.read.table("results")
          checkAnswer(df, spark.range(1, 10).toDF())
        }
      }
    }
  }

  test("metastore table with token file") {
    withTable("results") {
      withTempPaths(2) { case Seq(dataDir, localTokenDir) =>
        withSQLConf("delta.exchange.localClient.path" -> dataDir.getCanonicalPath) {
          val uuid = java.util.UUID.randomUUID().toString
          val localPath = new java.io.File(dataDir, uuid).getCanonicalPath
          spark.range(1, 10).write.format("delta").save(localPath)

          val pw = new java.io.PrintWriter(new java.io.File(localTokenDir, "myTokenFile"))
          try {
            pw.write("tokenFromLocalFile")
          } finally {
            pw.close()
          }

          val tablePath = s"delta://$uuid@databricks.com"
          sql(s"CREATE TABLE results (id LONG) USING `delta-exchange` LOCATION '$tablePath' " +
            s"TBLPROPERTIES ('tokenFile' = '$localTokenDir/myTokenFile')")
          val df = spark.read.table("results")
          checkAnswer(df, spark.range(1, 10).toDF())
        }
      }
    }
  }
}

class S3IntegerationSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  override def sparkConf: SparkConf = {
    super.sparkConf.set(
      "spark.delta.logStore.class",
      "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
  }

  test("s3: sign locally") {
    assume(sys.env.get("DELTA_EXCHANGE_TEST_AWS_ACCESS_KEY").nonEmpty)

    import TestResource.AWS._
    val s3RootPath = s"s3a://$bucket/ryan_delta_remote_test"
    withSQLConf(
      "delta.exchange.localClient.path" -> s3RootPath,
      "fs.s3a.access.key" -> awsAccessKey,
      "fs.s3a.secret.key" -> awsSecretKey
    ) {
      val uuid = java.util.UUID.randomUUID().toString
      val s3Path = new Path(s3RootPath, uuid).toString
      try {
        spark.range(1, 10).write.format("delta").save(s3Path)

        val path = s"delta://$uuid:token@databricks.com"
        val df = spark.read.format("delta-exchange").load(path)
        checkAnswer(df, spark.range(1, 10).toDF)
      } finally {
        new Path(s3Path).getFileSystem(spark.sessionState.newHadoopConf)
          .delete(new Path(s3Path), true)
      }
    }
  }

  // Use https://github.com/databricks/universe/pull/90204 to test
  ignore("s3: sign in databricks") {
    assume(sys.env.get("DELTA_EXCHANGE_TEST_AWS_ACCESS_KEY").nonEmpty)

    import TestResource.AWS._
    val s3RootPath = s"s3a://$bucket/ryan_delta_remote_test"
    val uuid = java.util.UUID.randomUUID().toString
    val s3Path = new Path(s3RootPath, uuid).toString
    try {
      withSQLConf(
        "fs.s3a.access.key" -> awsAccessKey,
        "fs.s3a.secret.key" -> awsSecretKey
      ) {

        spark.range(1, 10).map(x => x -> x.toString).write.format("delta").save(s3Path)
      }

      val token = "dapi5e3574ec767ca1548ae5bbed1a2dc04d"
      val host = "pr-90204.dev.databricks.com"
      val path = s"delta://$uuid:$token@$host"
      val df = spark.read.format("delta-exchange").load(path)
      checkAnswer(df, spark.range(1, 10).map(x => x -> x.toString).toDF)
    } finally {
      new Path(s3Path).getFileSystem(spark.sessionState.newHadoopConf)
        .delete(new Path(s3Path), true)
    }
  }

  // Run `build/sbt "server/runMain io.delta.exchange.server.DeltaExchangeService"` in a separate
  // shell before running this test
  test("s3: sign in local server") {
    assume(sys.env.get("DELTA_EXCHANGE_TEST_AWS_ACCESS_KEY").nonEmpty)

    import TestResource.AWS._
    val s3RootPath = s"s3a://$bucket/ryan_delta_remote_test"
    val uuid = java.util.UUID.randomUUID().toString
    val s3Path = new Path(s3RootPath, uuid).toString
    try {
      withSQLConf(
        "fs.s3a.access.key" -> awsAccessKey,
        "fs.s3a.secret.key" -> awsSecretKey
      ) {

        spark.range(1, 10).map(x => x -> x.toString).write.format("delta").save(s3Path)
      }

      val token = "dapi4e0a5ee596e45c73931d16df478d5234"
      val host = "localhost"
      val path = s"delta://$uuid:$token@$host"
      val df = spark.read.format("delta-exchange").load(path)
      checkAnswer(df, spark.range(1, 10).map(x => x -> x.toString).toDF)
    } finally {
      new Path(s3Path).getFileSystem(spark.sessionState.newHadoopConf)
        .delete(new Path(s3Path), true)
    }
  }

  // Run `build/sbt "server/runMain io.delta.exchange.server.DeltaExchangeService"` in a separate
  // shell before running this test
  test("s3: sign in local server with partition filter") {
    assume(sys.env.get("DELTA_EXCHANGE_TEST_AWS_ACCESS_KEY").nonEmpty)

    import TestResource.AWS._
    val s3RootPath = s"s3a://$bucket/ryan_delta_remote_test"
    val uuid = java.util.UUID.randomUUID().toString
    val s3Path = new Path(s3RootPath, uuid).toString
    withSQLConf(
      "fs.s3a.access.key" -> awsAccessKey,
      "fs.s3a.secret.key" -> awsSecretKey
    ) {
      spark.range(1, 10).map(x => x -> x % 2).toDF("c1", "c2")
        .write.partitionBy("c2").format("delta").save(s3Path)
    }

    try {
      val token = "dapi4e0a5ee596e45c73931d16df478d5234"
      val host = "localhost"
      val path = s"delta://$uuid:$token@$host"
      val df = spark.read.format("delta-exchange").load(path).where("c2 = 0")
      checkAnswer(
        df,
        spark.range(1, 10).map(x => x -> x % 2).toDF("c1", "c2").where("c2 = 0").toDF)
    } finally {
      new Path(s3Path).getFileSystem(spark.sessionState.newHadoopConf)
        .delete(new Path(s3Path), true)
    }
  }
}

class AzureIntegrationSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  override def sparkConf: SparkConf = {
    super.sparkConf.set(
      "spark.delta.logStore.class",
      "org.apache.spark.sql.delta.storage.AzureLogStore")
  }

  test("azure: sign locally") {
    assume(sys.env.get("DELTA_EXCHANGE_TEST_AZURE_STORAGE_ACCOUNT").nonEmpty)

    import TestResource.Azure._
    val testContainer = s"ryan-delta-remote-test"
    val wasbRootPath =
      s"wasbs://$testContainer@$storageAccount.blob.core.windows.net/ryan_delta_remote_test"
    withSQLConf(
        s"fs.azure.account.key.$storageAccount.blob.core.windows.net" -> credential,
        "delta.exchange.localClient.path" -> wasbRootPath) {
      val uuid = java.util.UUID.randomUUID().toString
      val wasbPath = new Path(wasbRootPath, uuid).toString
      try {
        spark.range(1, 10).write.format("delta").save(wasbPath)

        val path = s"delta://$uuid:token@databricks.com"
        val df = spark.read.format("delta-exchange").load(path)
        checkAnswer(df, spark.range(1, 10).toDF)
      } finally {
        new Path(wasbPath).getFileSystem(spark.sessionState.newHadoopConf)
          .delete(new Path(wasbPath), true)
      }
    }
  }
}

class GCSIntegrationSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  override def sparkConf: SparkConf = {
    super.sparkConf.set(
      "spark.delta.logStore.class",
      "org.apache.spark.sql.delta.storage.AzureLogStore")
  }

  test("gcs: sign locally") {
    assume(sys.env.get("DELTA_EXCHANGE_TEST_GCP_KEY").nonEmpty)

    import TestResource.GCP._
    val gcsRootPath = s"gs://$bucket/ryan_delta_remote_test"
    withSQLConf(
      "fs.gs.project.id" -> projectId,
      "google.cloud.auth.service.account.enable" -> "true",
      "fs.gs.auth.service.account.email" -> clientEmail,
      "fs.gs.auth.service.account.private.key.id" -> keyId,
      "fs.gs.auth.service.account.private.key" -> key,
      "delta.exchange.localClient.path" -> gcsRootPath
    ) {
      val uuid = java.util.UUID.randomUUID().toString
      val gcsPath = new Path(gcsRootPath, uuid).toString
      try {
        spark.range(1, 10).write.format("delta").save(gcsPath)

        val path = s"delta://$uuid:token@databricks.com"
        val df = spark.read.format("delta-exchange").load(path)
        checkAnswer(df, spark.range(1, 10).toDF)
      } finally {
        new Path(gcsPath).getFileSystem(spark.sessionState.newHadoopConf)
          .delete(new Path(gcsPath), true)
      }
    }
  }
}
