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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.connector.read.streaming
import org.apache.spark.sql.connector.read.streaming.{
  ReadAllAvailable,
  ReadLimit,
  ReadMaxFiles,
  SupportsAdmissionControl
}
import org.apache.spark.sql.delta.sources.DeltaSource
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.types.StructType


case class DeltaSharingDeltaSource(deltaSource: DeltaSource)  extends Source
  with SupportsAdmissionControl
  with Logging {

  override val schema: StructType = deltaSource.schema

  override def getDefaultReadLimit: ReadLimit = deltaSource.getDefaultReadLimit

  override def latestOffset(startOffset: streaming.Offset, limit: ReadLimit): streaming.Offset = {
    // TODO: 1. call client.getTableVersion every 30 seconds.
    // scalastyle:off println
    Console.println(s"----[linzhou]----latestOffset for ${deltaSource.deltaLog.dataPath}")
    deltaSource.latestOffset(startOffset, limit)
    // TODO: 2. translates from DeltaSourceOffset to DeltaSharingSourceOffset
  }

  override def getOffset: Option[Offset] = {
    throw new UnsupportedOperationException(
      "latestOffset(Offset, ReadLimit) should be called instead of this method")
  }

  override def getBatch(startOffsetOption: Option[Offset], end: Offset): DataFrame = {
    // TODO: 2. translates from DeltaSharingSourceOffset to DeltaSourceOffset
    deltaSource.getBatch(startOffsetOption, end)
  }

  override def stop(): Unit = {
    deltaSource.stop()
  }

  override def toString(): String = s"DeltaSharingDeltaSource[${deltaSource.deltaLog.dataPath}]"
}
