package io.delta.exchange.sql

import io.delta.exchange.client.GetShareRequest
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.types.StringType

case class DescribeShareCommand(shareName: String) extends RunnableCommand with DeltaExchangeCommand {

  override val output: Seq[Attribute] =
    Seq(AttributeReference("name", StringType, nullable = true)(),
      AttributeReference("schema", StringType, nullable = true)(),
      AttributeReference("uri", StringType, nullable = true)())

  override def run(sparkSession: SparkSession): Seq[Row] = {
    client.getShare(GetShareRequest(shareName)).table.map { table =>
      Row(table.name, table.schema,
        s"delta-exchange://${table.shareName}.${table.schema}.${table.name}")
    }
  }
}
