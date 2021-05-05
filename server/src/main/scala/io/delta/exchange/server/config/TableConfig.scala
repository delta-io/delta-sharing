package io.delta.exchange.server.config

import scala.beans.BeanProperty

case class TableConfig(@BeanProperty name: String, @BeanProperty var location: String) {

  def this() {
    this(null, null)
  }
}
