package io.delta.sharing.client.util

import io.delta.kernel.client.TableClient
import io.delta.kernel.data.Row
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import io.delta.kernel.internal.types.TableSchemaSerDe
import io.delta.kernel.data.DefaultJsonRow
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.core.JsonProcessingException
import java.io.UncheckedIOException

object KernelUtils {
  private val OBJECT_MAPPER = new ObjectMapper()
  def deserializeRowFromJson(tableClient: TableClient, jsonRowWithSchema: String): Row = try {
    val jsonNode: JsonNode = OBJECT_MAPPER.readTree(jsonRowWithSchema)
    val schemaNode = jsonNode.get("schema")
    val schema = TableSchemaSerDe.fromJson(tableClient.getJsonHandler, schemaNode.asText)
    new DefaultJsonRow(jsonNode.get("row").asInstanceOf[ObjectNode], schema)
  } catch {
    case ex: JsonProcessingException =>
      throw new UncheckedIOException(ex)
  }
}
