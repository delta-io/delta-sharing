package io.delta.exchange.spark

import io.delta.exchange.sql.DeltaExchangeSparkSessionExtension
import org.apache.spark.SparkConf
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.test.SharedSparkSession

class DescribeShareCommandSuite extends QueryTest with SharedSparkSession {

  override def sparkConf: SparkConf = {
    super.sparkConf.set("spark.sql.extensions", classOf[DeltaExchangeSparkSessionExtension].getName)
  }

  test("DESC SHARE: mock") {
    withSQLConf("spark.delta-exchange.client.class" -> classOf[MockClient].getName) {
      checkAnswer(sql("DESC SHARE test"), Row("xyz", "bar", "delta-exchange://foo.bar.xyz"))
      checkAnswer(sql("DESCRIBE SHARE test"), Row("xyz", "bar", "delta-exchange://foo.bar.xyz"))
    }
  }

  // Run `build/sbt "server/runMain io.delta.exchange.server.DeltaExchangeService"` in a separate
  // shell before running this test
  ignore("DESC SHARE: remote") {
    checkAnswer(sql("DESC SHARE vaccine_share"),
      Row("vaccine_ingredints", "acme_vaccine_data", "delta-exchange://vaccine_share.acme_vaccine_data.vaccine_ingredints") ::
        Row("vaccine_patients", "acme_vaccine_data", "delta-exchange://vaccine_share.acme_vaccine_data.vaccine_patients") :: Nil)
    checkAnswer(sql("DESCRIBE SHARE vaccine_share"),
      Row("vaccine_ingredints", "acme_vaccine_data", "delta-exchange://vaccine_share.acme_vaccine_data.vaccine_ingredints") ::
        Row("vaccine_patients", "acme_vaccine_data", "delta-exchange://vaccine_share.acme_vaccine_data.vaccine_patients") :: Nil)
  }
}

class MockClient extends DeltaLogClient {

  override def listShares(): ListSharesResponse = {
    ListSharesResponse(Seq("foo", "bar"))
  }

  override def getShare(request: GetShareRequest): GetShareResponse = {
    GetShareResponse(SharedTable("xyz", "bar", "foo") :: Nil)
  }

  override def getTableInfo(request: GetTableInfoRequest): GetTableInfoResponse = throw new UnsupportedOperationException

  override def getMetadata(request: GetMetadataRequest): GetMetadataResponse = throw new UnsupportedOperationException

  override def getFiles(request: GetFilesRequest): GetFilesResponse = throw new UnsupportedOperationException
}
