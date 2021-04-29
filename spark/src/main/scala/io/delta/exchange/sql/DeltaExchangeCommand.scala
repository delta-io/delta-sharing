package io.delta.exchange.sql

import io.delta.exchange.client.DeltaExchangeClient
import io.delta.exchange.spark.SparkDeltaExchangeRestClient
import org.apache.spark.sql.SparkSession

trait DeltaExchangeCommand {
  protected lazy val client = {
    val clazz =
      SparkSession.active.sessionState.conf.getConfString(
        "spark.delta.exchange.client.class",
        classOf[SparkDeltaExchangeRestClient].getName)
    Class.forName(clazz).newInstance().asInstanceOf[DeltaExchangeClient]
  }
}
