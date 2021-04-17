package io.delta.exchange.server

import io.delta.exchange.protocol.Table

class FileBasedShareManagement {

  // TODO load from file
  private val shares = Map(
    "vaccine_share" -> Seq(
      Table().withName("vaccine_ingredints")
        .withSchema("acme_vaccine_data")
        .withShareName("vaccine_share"),
      Table().withName("vaccine_patients")
        .withSchema("acme_vaccine_data")
        .withShareName("vaccine_share")),
    "sales_share" -> Nil)

  def listShares(): Seq[String] = synchronized {
    shares.keys.toSeq
  }

  def getShare(shareName: String): Seq[Table] = synchronized {
    shares(shareName)
  }
}
