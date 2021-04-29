package io.delta.exchange.spark.actions

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude}
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types.{DataType, StructType}
import org.codehaus.jackson.annotate.JsonRawValue

case class Format(
  provider: String = "parquet",
  options: Map[String, String] = Map.empty)

/**
 * Updates the metadata of the table. Only the last update to the [[Metadata]]
 * of a table is kept. It is the responsibility of the writer to ensure that
 * any data already present in the table is still valid after any change.
 */
case class Metadata(
  id: String = java.util.UUID.randomUUID().toString,
  name: String = null,
  description: String = null,
  format: Format = Format(),
  schemaString: String = null,
  partitionColumns: Seq[String] = Nil,
  configuration: Map[String, String] = Map.empty,
  @JsonDeserialize(contentAs = classOf[java.lang.Long])
  createdTime: Option[Long] = Some(System.currentTimeMillis())) {

  // The `schema` and `partitionSchema` methods should be vals or lazy vals, NOT
  // defs, because parsing StructTypes from JSON is extremely expensive and has
  // caused perf. problems here in the past:

  /** Returns the schema as a [[StructType]] */
  @JsonIgnore
  lazy val schema: StructType =
  Option(schemaString).map { s =>
    DataType.fromJson(s).asInstanceOf[StructType]
  }.getOrElse(StructType.apply(Nil))

  /** Returns the partitionSchema as a [[StructType]] */
  @JsonIgnore
  lazy val partitionSchema: StructType =
  new StructType(partitionColumns.map(c => schema(c)).toArray)

  /** Columns written out to files. */
  @JsonIgnore
  lazy val dataSchema: StructType = {
    val partitions = partitionColumns.toSet
    StructType(schema.filterNot(f => partitions.contains(f.name)))
  }
}

case class AddFile(
  path: String,
  @JsonInclude(JsonInclude.Include.ALWAYS)
  partitionValues: Map[String, String],
  size: Long,
  modificationTime: Long,
  dataChange: Boolean,
  @JsonRawValue
  stats: String = null,
  tags: Map[String, String] = null) {
  require(path.nonEmpty)
}

object SingleAction extends Logging {

  private lazy val _addFileEncoder: ExpressionEncoder[AddFile] = try {
    ExpressionEncoder[AddFile]()
  } catch {
    case e: Throwable =>
      logError(e.getMessage, e)
      throw e
  }

  implicit def addFileEncoder: Encoder[AddFile] = {
    _addFileEncoder.copy()
  }
}