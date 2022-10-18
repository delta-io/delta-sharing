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

package io.delta.sharing.spark

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.connector.read.streaming.{Offset => OffsetV2}
import org.apache.spark.sql.execution.streaming.{Offset, SerializedOffset}
import org.json4s._
import org.json4s.jackson.JsonMethods.parse

import io.delta.sharing.spark.util.JsonUtils

/**
 * Tracks how far we processed in when reading changes from the [[Delta Sharing Server]].
 *
 * Note this class retains the naming of `Reservoir` to maintain compatibility
 * with serialized offsets from the beta period.
 *
 * @param sourceVersion     The version of serialization that this offset is encoded with.
 * @param reservoirId       The id of the table we are reading from. Used to detect
 *                          misconfiguration when restarting a query.
 * @param reservoirVersion  The version of the table that we are currently processing.
 * @param index             The index in the sequence of AddFiles in this version. Used to
 *                          break large commits into multiple batches. This index is created by
 *                          sorting on url.
 * @param isStartingVersion Whether this offset denotes a query that is starting rather than
 *                          processing changes. When starting a new query, we first process
 *                          all data present in the table at the start and then move on to
 *                          processing new data that has arrived.
 */
case class DeltaSharingSourceOffset(
  sourceVersion: Long,
  reservoirId: String,
  reservoirVersion: Long,
  index: Long,
  isStartingVersion: Boolean
) extends Offset {

  override def json: String = JsonUtils.toJson(this)

  /**
   * Compare two DeltaSharingSourceOffsets which are on the same table.
   * @return 0 for equivalent offsets. negative if this offset is less than `otherOffset`. Positive
   *         if this offset is greater than `otherOffset`
   */
  def compare(otherOffset: DeltaSharingSourceOffset): Int = {
    assert(reservoirId == otherOffset.reservoirId, "Comparing offsets that do not refer to the" +
      " same table is disallowed.")
    implicitly[Ordering[(Long, Long)]].compare((reservoirVersion, index),
      (otherOffset.reservoirVersion, otherOffset.index))
  }
}

object DeltaSharingSourceOffset {

  val VERSION_1 = 1

  def apply(
    sourceVersion: Long,
    reservoirId: String,
    reservoirVersion: Long,
    index: Long,
    isStartingVersion: Boolean
  ): DeltaSharingSourceOffset = {
    new DeltaSharingSourceOffset(
      sourceVersion,
      reservoirId,
      reservoirVersion,
      index,
      isStartingVersion
    )
  }

  def apply(reservoirId: String, offset: OffsetV2): DeltaSharingSourceOffset = {
    offset match {
      case o: DeltaSharingSourceOffset => o
      case s =>
        validateSourceVersion(s.json)
        val o = JsonUtils.fromJson[DeltaSharingSourceOffset](s.json)
        if (o.reservoirId != reservoirId) {
          throw DeltaSharingErrors.nonExistentDeltaTable(o.reservoirId)
        }
        o
    }
  }

  private def validateSourceVersion(json: String): Unit = {
    val parsedJson = parse(json)
    val versionOpt = jsonOption(parsedJson \ "sourceVersion").map {
      case i: JInt => i.num.longValue
      case other => throw DeltaSharingErrors.invalidSourceVersion(other.toString)
    }
    if (versionOpt.isEmpty) {
      throw DeltaSharingErrors.cannotFindSourceVersionException(json)
    }

    val maxVersion = VERSION_1

    if (versionOpt.get > maxVersion) {
      throw DeltaSharingErrors.unsupportedTableReaderVersion(maxVersion, versionOpt.get)
    }
  }

  /** Return an option that translates JNothing to None */
  private def jsonOption(json: JValue): Option[JValue] = {
    json match {
      case JNothing => None
      case value: JValue => Some(value)
    }
  }
}
