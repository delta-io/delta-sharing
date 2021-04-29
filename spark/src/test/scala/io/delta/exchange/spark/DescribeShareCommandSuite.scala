package io.delta.exchange.spark

import io.delta.exchange.client.MockClient
import io.delta.exchange.sql.DeltaExchangeSparkSessionExtension
import org.apache.spark.SparkConf
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.test.SharedSparkSession

class DescribeShareCommandSuite extends QueryTest with SharedSparkSession {

  override def sparkConf: SparkConf = {
    super.sparkConf.set("spark.sql.extensions", classOf[DeltaExchangeSparkSessionExtension].getName)
  }

  test("DESC SHARE: mock") {
    withSQLConf("spark.delta.exchange.client.class" -> classOf[MockClient].getName) {
      checkAnswer(sql("DESC SHARE test"), Row("xyz", "bar", "delta-exchange://foo.bar.xyz"))
      checkAnswer(sql("DESCRIBE SHARE test"), Row("xyz", "bar", "delta-exchange://foo.bar.xyz"))
    }
  }

  // Run `build/sbt "server/runMain io.delta.exchange.server.DeltaExchangeService"` in a separate
  // shell before running this test
  test("DESC SHARE: remote") {
    assume(sys.env.get("DELTA_EXCHANGE_TEST_AWS_ACCESS_KEY").nonEmpty)
    checkAnswer(sql("DESC SHARE vaccine_share"),
      Row("vaccine_ingredints", "acme_vaccine_data", "delta-exchange://vaccine_share.acme_vaccine_data.vaccine_ingredints") ::
        Row("vaccine_patients", "acme_vaccine_data", "delta-exchange://vaccine_share.acme_vaccine_data.vaccine_patients") :: Nil)
    checkAnswer(sql("DESCRIBE SHARE vaccine_share"),
      Row("vaccine_ingredints", "acme_vaccine_data", "delta-exchange://vaccine_share.acme_vaccine_data.vaccine_ingredints") ::
        Row("vaccine_patients", "acme_vaccine_data", "delta-exchange://vaccine_share.acme_vaccine_data.vaccine_patients") :: Nil)
  }
}
