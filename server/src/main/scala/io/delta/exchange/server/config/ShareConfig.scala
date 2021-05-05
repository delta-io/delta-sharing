package io.delta.exchange.server.config

import scala.beans.BeanProperty

case class ShareConfig(@BeanProperty var name: String, @BeanProperty var schemas: java.util.List[SchemaConfig]) {
  def this() {
    this(null, null)
  }
}
