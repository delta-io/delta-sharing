package io.delta.exchange.sql

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.types.StringType

case class ShowSharesCommand() extends RunnableCommand with DeltaExchangeCommand {

  override val output: Seq[Attribute] =
    Seq(
      AttributeReference("name", StringType, nullable = true)(),
      AttributeReference("kind", StringType, nullable = true)())

  override def run(sparkSession: SparkSession): Seq[Row] = {
    client.listShares().name.map { share =>
      Row(share, "INBOUND")
    }
  }
}
