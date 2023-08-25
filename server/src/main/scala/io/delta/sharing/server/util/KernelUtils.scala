/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.delta.sharing.server.util

import java.io.UncheckedIOException
import java.net.{URI, URL, URLEncoder}
import java.util.Objects.requireNonNull

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.ObjectNode
import io.delta.kernel.Table
import io.delta.kernel.client.TableClient
import io.delta.kernel.data.{ColumnarBatch, Row}
import io.delta.kernel.defaults.client.DefaultTableClient
import io.delta.kernel.defaults.internal.data.DefaultJsonRow
import io.delta.kernel.internal.types.TableSchemaSerDe
import io.delta.kernel.types._
import io.delta.kernel.utils.CloseableIterator
import java.util
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.s3a.S3AFileSystem

import io.delta.sharing.server.{CloudFileSigner, S3FileSigner}
import io.delta.sharing.server.util.KernelUtils.{signedDeltaSharingURL, OBJECT_MAPPER}

class KernelUtils(tableRoot: Path) {
  private val hadoopConf = new Configuration() {
    {
      set("spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    }
  }

  private val fileSigner = {
    val fs = tableRoot.getFileSystem(hadoopConf)
    fs match {
      case _: S3AFileSystem =>
        new S3FileSigner(tableRoot.toUri, hadoopConf, 10)
      case _ =>
        throw new IllegalStateException(s"File system ${fs.getClass} is not supported")
    }
  }

  val fileSystem = tableRoot.getFileSystem(hadoopConf)

  /**
   * Utility method to get the scan state and scan files to read Delta table at the
   * given location.
   */
  def getScanStateAndFiles(): (Row, Seq[Row]) = {
    val tableClient = DefaultTableClient.create(hadoopConf)
    val table = Table.forPath(tableRoot.toUri.toString)
    val snapshot = table.getLatestSnapshot(tableClient)

    val scan = snapshot.getScanBuilder(tableClient).build()

    (
      scan.getScanState(tableClient),
      toScanFilesSeq(scan.getScanFiles(tableClient))
    )
  }

  /**
   * Utility method to serialize a {@link Row} as a JSON string
   */
  def serializeRowToJson(row: Row, pathColumns: Seq[String]): String = {
    val rowObject: util.HashMap[String, Object] =
      convertRowToJsonObject(row, pathColumns)
    try {
      val rowWithSchema = new util.HashMap[String, Object]
      rowWithSchema.put("schema", TableSchemaSerDe.toJson(row.getSchema))
      rowWithSchema.put("row", rowObject)
      OBJECT_MAPPER.writeValueAsString(rowWithSchema)
    } catch {
      case e: JsonProcessingException =>
        throw new UncheckedIOException(e);
    }
  }

  /**
   * Utility method to deserialize a {@link Row} object from the JSON form.
   */
  def deserializeRowFromJson(tableClient: TableClient, jsonRowWithSchema: String): Row = try {
    val jsonNode: JsonNode = OBJECT_MAPPER.readTree(jsonRowWithSchema)
    val schemaNode = jsonNode.get("schema")
    val schema = TableSchemaSerDe.fromJson(tableClient.getJsonHandler, schemaNode.asText)
    parseRowFromJsonWithSchema(jsonNode.get("row").asInstanceOf[ObjectNode], schema)
  } catch {
    case ex: JsonProcessingException =>
      throw new UncheckedIOException(ex)
  }

  private def convertRowToJsonObject(
    row: Row, pathColumns: Seq[String]): util.HashMap[String, Object] = {
    val rowType = row.getSchema
    val rowObject = new util.HashMap[String, Object]()

    Seq(0, rowType.length() - 1).foreach {
      fieldId => {
        val field = rowType.at(fieldId)
        val fieldType = field.getDataType
        val name = field.getName
        if (row.isNullAt(fieldId)) {
          rowObject.put(name, null)
        } else {
          var value: Object = null
          if (fieldType.isInstanceOf[BooleanType]) value =
            row.getBoolean(fieldId).asInstanceOf[Object]
          else if (fieldType.isInstanceOf[ByteType]) value =
            row.getByte(fieldId).byteValue().asInstanceOf[Object]
          else if (fieldType.isInstanceOf[ShortType]) value =
            row.getShort(fieldId).shortValue().asInstanceOf[Object]
          else if (fieldType.isInstanceOf[IntegerType]) value =
            row.getInt(fieldId).intValue().asInstanceOf[Object]
          else if (fieldType.isInstanceOf[LongType]) value =
            row.getLong(fieldId).longValue().asInstanceOf[Object]
          else if (fieldType.isInstanceOf[FloatType]) value =
            row.getFloat(fieldId).floatValue().asInstanceOf[Object]
          else if (fieldType.isInstanceOf[DoubleType]) value =
            row.getDouble(fieldId).doubleValue().asInstanceOf[Object]
          else if (fieldType.isInstanceOf[StringType]) value = {
            val baseValue = row.getString(fieldId);
            if (pathColumns.contains(name)) {
              val absPath = KernelUtils.absolutePath(tableRoot, baseValue);
              val fileStatus = fileSystem.getFileStatus(absPath);
              signedDeltaSharingURL(absPath, fileStatus.getLen, fileSigner)
            } else baseValue
          }
          else if (fieldType.isInstanceOf[ArrayType]) value = row.getArray(fieldId)
          else if (fieldType.isInstanceOf[MapType]) value = row.getMap(fieldId)
          else if (fieldType.isInstanceOf[StructType]) {
            val subRow = row.getStruct(fieldId)
            value = convertRowToJsonObject(subRow, pathColumns)
          }
          else throw new UnsupportedOperationException("NYI");
          rowObject.put(name, value)
        }
      }
    }
    rowObject
  }

  private def parseRowFromJsonWithSchema(rowJsonNode: ObjectNode, rowType: StructType): Row = {
    new DefaultJsonRow(rowJsonNode, rowType)
  }

  /**
   * Iterate over the scan file batches and return a sequence of scan file rows.
   * TODO: This could end up in OOM. Figure out a way to paginate the results.
   *
   * @param scanFileBatchIter
   * @return
   */
  private def toScanFilesSeq(scanFileBatchIter: CloseableIterator[ColumnarBatch]): Seq[Row] = {
    requireNonNull(scanFileBatchIter)

    val scanFileRows = Seq.newBuilder[Row]
    try {
      while (scanFileBatchIter.hasNext) {
        val scanFilesBatch = scanFileBatchIter.next
        val scanFilesBatchIter = scanFilesBatch.getRows
        while (scanFilesBatchIter.hasNext) {
          scanFileRows += scanFilesBatchIter.next
        }
        scanFilesBatchIter.close();
      }
    } finally {
      scanFileBatchIter.close();
    }

    scanFileRows.result()
  }
}

object KernelUtils {
  private val OBJECT_MAPPER = new ObjectMapper()

  private def signedDeltaSharingURL(path: Path, size: Long, signer: CloudFileSigner): String = {
    val signedURL = signer.sign(path);

    s"delta-sharing://${URLEncoder.encode(signedURL.url)}/${size}"
  }

  private def absolutePath(root: Path, child: String): Path = {
    val p = new Path(new URI(child))
    if (p.isAbsolute) {
      // throw new IllegalStateException("table containing absolute paths cannot be shared")
      // TODO: fix the above - Kernel already returns a absolute path
      p
    } else {
      new Path(root, p)
    }
  }
}
