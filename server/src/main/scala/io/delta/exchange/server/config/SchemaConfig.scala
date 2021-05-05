package io.delta.exchange.server.config

import scala.beans.BeanProperty

case class SchemaConfig(@BeanProperty var name: String, @BeanProperty var tables: java.util.List[TableConfig]) {
  def this() {
    this(null, null)
  }
}
