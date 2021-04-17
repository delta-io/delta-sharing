package io.delta.exchange.spark

import io.delta.exchange.sql.DeltaExchangeSparkSessionExtension
import org.apache.spark.SparkConf
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.test.SharedSparkSession

class ShowSharesCommandSuite extends QueryTest with SharedSparkSession {

  override def sparkConf: SparkConf = {
    super.sparkConf.set("spark.sql.extensions", classOf[DeltaExchangeSparkSessionExtension].getName)
  }

  test("SHOW SHARES: mock") {
    withSQLConf("spark.delta-exchange.client.class" -> classOf[MockClient].getName) {
      checkAnswer(sql("SHOW SHARES"), Row("foo", "INBOUND") :: Row("bar", "INBOUND") :: Nil)
    }
  }

  // Run `build/sbt "server/runMain io.delta.exchange.server.DeltaExchangeService"` in a separate
  // shell before running this test
  ignore("SHOW SHARES: remote") {
    checkAnswer(sql("SHOW SHARES"), Row("vaccine_share", "INBOUND") :: Row("sales_share", "INBOUND") :: Nil)
  }
}
