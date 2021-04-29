package io.delta.exchange.server.model

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude}
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import io.delta.exchange.server.JsonUtils
import org.apache.spark.util.Utils
import org.codehaus.jackson.annotate.JsonRawValue

case class SingleAction(
  add: AddFile = null,
  metaData: Metadata = null,
  protocol: Protocol = null) {

  def unwrap: Action = {
    if (add != null) {
      add
    } else if (metaData != null) {
      metaData
    } else if (protocol != null) {
      protocol
    } else {
      null
    }
  }
}

case class Format(provider: String = "parquet")

case class Metadata(
  id: String = null,
  name: String = null,
  description: String = null,
  format: Format = Format(),
  schemaString: String = null,
  partitionColumns: Seq[String] = Nil) extends Action {
  override def wrap: SingleAction = SingleAction(metaData = this)
}

sealed trait Action {
  def wrap: SingleAction

  def json: String = JsonUtils.toJson(wrap)
}

case class Protocol(minReaderVersion: Int) extends Action {
  override def wrap: SingleAction = SingleAction(protocol = this)
}

case class AddFile(
  path: String,
  @JsonInclude(JsonInclude.Include.ALWAYS)
  partitionValues: Map[String, String],
  size: Long,
  dataChange: Boolean,
  // TODO stats
  tags: Map[String, String] = null) extends Action {

  require(path.nonEmpty)

  override def wrap: SingleAction = SingleAction(add = this)
}
