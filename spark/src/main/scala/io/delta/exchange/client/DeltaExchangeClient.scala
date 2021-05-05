package io.delta.exchange.client

import io.delta.exchange.client.model._

trait DeltaExchangeClient {
  def listAllTables(): Seq[Table]

  def getMetadata(table: Table): DeltaTableMetadata

  def getFiles(table: Table, predicates: Seq[String], limit: Option[Int]): DeltaTableFiles
}
