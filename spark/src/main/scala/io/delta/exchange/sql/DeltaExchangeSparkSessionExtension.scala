package io.delta.exchange.sql

import io.delta.exchange.sql.parser.DeltaExchangeSqlParser
import org.apache.spark.sql.SparkSessionExtensions

class DeltaExchangeSparkSessionExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectParser { (_, parser) =>
      new DeltaExchangeSqlParser(parser)
    }
  }
}
