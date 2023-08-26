package io.delta.sharing.client.util

import io.delta.kernel.client.TableClient
import io.delta.kernel.data.Row
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import io.delta.kernel.internal.types.TableSchemaSerDe
import io.delta.kernel.defaults.internal.data.DefaultJsonRow
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.core.JsonProcessingException
import java.io.UncheckedIOException
import io.delta.kernel.data.ColumnarBatch
import io.delta.kernel.data.DataReadResult
import io.delta.kernel.types.StructType
import io.delta.kernel.utils.CloseableIterator
import io.delta.kernel.data.ColumnVector
import io.delta.kernel.types._


object KernelUtils {
  private val OBJECT_MAPPER = new ObjectMapper()
  def deserializeRowFromJson(tableClient: TableClient, jsonRowWithSchema: String): Row = try {
    val processedJsonRowWithSchema = OBJECT_MAPPER.readValue(jsonRowWithSchema, classOf[String])
    val jsonNode: JsonNode = OBJECT_MAPPER.readTree(processedJsonRowWithSchema)
    val schemaNode = jsonNode.get("schema")
    val schema = TableSchemaSerDe.fromJson(tableClient.getJsonHandler, schemaNode.asText)
    new DefaultJsonRow(jsonNode.get("row").asInstanceOf[ObjectNode], schema)
  } catch {
    case ex: JsonProcessingException =>
      throw new UncheckedIOException(ex)
  }
  def convertToCloseableIterator(rows: Seq[Row]): CloseableIterator[Row] = new CloseableIterator[Row] {
    private val internalIterator = rows.iterator

    override def hasNext: Boolean = internalIterator.hasNext

    override def next(): Row = internalIterator.next()

    override def close(): Unit = {
      // Any cleanup logic if needed
    }
  }

  def printData(dataReadResult: DataReadResult, maxRowsToPrint: Int): Int = {
    var printedRowCount = 0
    val data = dataReadResult.getData
    val selectionVector = dataReadResult.getSelectionVector
    for (rowId <- 0 until data.getSize if printedRowCount < maxRowsToPrint) {
      if (selectionVector.isEmpty || selectionVector.get.getBoolean(rowId)) {
        printRow(data, rowId)
        printedRowCount += 1
      }
    }
    printedRowCount
  }

  def printRow(batch: ColumnarBatch, rowId: Int): Unit = {
    val numCols = batch.getSchema.length
    val rowValues = (0 until numCols).map { colOrdinal =>
      val columnVector = batch.getColumnVector(colOrdinal)
      // Assuming you have a utility function in Scala similar to VectorUtils.getValueAsObject
      // If not, you'll need to implement the conversion from ColumnVector to the desired object type.
      getValueAsObject(columnVector, rowId)
    }.toArray
    println(formatter(numCols).format(rowValues: _*))
  }

  // Assuming you have a Scala version of the getValueAsObject function
  // If not, you'll need to implement it based on your needs.
  def getValueAsObject(vector: ColumnVector, rowId: Int): Any = {
    val dataType = vector.getDataType

    if (vector.isNullAt(rowId)) {
      return null
    }

    dataType match {
      case _: BooleanType => vector.getBoolean(rowId)
      case _: ByteType => vector.getByte(rowId)
      case _: ShortType => vector.getShort(rowId)
      case _: IntegerType | _: DateType => vector.getInt(rowId)
      case _: LongType | _: TimestampType => vector.getLong(rowId)
      case _: FloatType => vector.getFloat(rowId)
      case _: DoubleType => vector.getDouble(rowId)
      case _: StringType => vector.getString(rowId)
      case _: BinaryType => vector.getBinary(rowId)
      case _: StructType => vector.getStruct(rowId)
      case _: MapType => vector.getMap(rowId)
      case _: ArrayType => vector.getArray(rowId)
      case _: DecimalType => vector.getDecimal(rowId)
      case _ => throw new UnsupportedOperationException(s"$dataType is not supported yet")
    }
  }

  def formatter(length: Int): String = {
    (0 until length).map(_ => "%20s").mkString("|") + "\n"
  }

}
