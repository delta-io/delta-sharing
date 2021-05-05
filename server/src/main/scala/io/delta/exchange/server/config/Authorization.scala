package io.delta.exchange.server.config

import scala.beans.BeanProperty

case class Authorization(@BeanProperty var bearerToken: String) {
  def this() {
    this(null)
  }
}
