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

package io.delta.sharing.server.actions

import java.net.URI
import java.sql.Timestamp

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude}
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.{JsonSerializer, SerializerProvider}
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}

import io.delta.sharing.server.util.JsonUtils


object DeltaAction {

  /** The maximum version of the protocol that this version of Delta Standalone understands. */
  val readerVersion = 3
  val writerVersion = 2

  def fromJson(json: String): DeltaAction = {
    JsonUtils.mapper.readValue[DeltaSingleAction](json).unwrap
  }
}

/**
 * Note: this is a stripped down version of TableFeature from Runtime.
 * Delta standalone only supports basic parsing of reader features.
 */
sealed abstract class TableFeature(val name: String) extends java.io.Serializable {
  // scalastyle:off caselocale
  def isInSet(featureSet: Set[String]): Boolean = {
    featureSet.map(_.toLowerCase).contains(name.toLowerCase)
  }
  // scalastyle:on caselocale
}

/**
 * Represents a single change to the state of a Delta table. An order sequence
 * of actions can be replayed using [[InMemoryLogReplay]] to derive the state
 * of the table at a given point in time.
 */
sealed trait DeltaAction {
  def wrap: DeltaSingleAction

  def json: String = JsonUtils.toJson(wrap)
}

/**
 * Used to block older clients from reading or writing the log when backwards
 * incompatible changes are made to the protocol. Readers and writers are
 * responsible for checking that they meet the minimum versions before performing
 * any other operations.
 *
 * Since this action allows us to explicitly block older clients in the case of a
 * breaking change to the protocol, clients should be tolerant of messages and
 * fields that they do not understand.
 */
case class DeltaProtocol(
    minReaderVersion: Int = DeltaAction.readerVersion,
    minWriterVersion: Int = DeltaAction.writerVersion,
    @JsonInclude(Include.NON_ABSENT) // write to JSON only when the field is not `None`
    readerFeatures: Option[Set[String]] = None,
    @JsonInclude(Include.NON_ABSENT) // write to JSON only when the field is not `None`
    writerFeatures: Option[Set[String]] = None)
  extends DeltaAction {
  override def wrap: DeltaSingleAction = DeltaSingleAction(protocol = this)

  @JsonIgnore
  def simpleString: String = s"($minReaderVersion,$minWriterVersion)"
}

/** Actions pertaining to the addition and removal of files. */
sealed trait DeltaFileAction extends DeltaAction {
  val path: String
  val dataChange: Boolean
  @JsonIgnore
  lazy val pathAsUri: URI = new URI(path)
}

/** Key to uniquely identify FileActions. */
case class DeltaFileActionKey(uri: URI)

object DeltaFileActionKey {
  def apply(fileAction: DeltaFileAction): DeltaFileActionKey = fileAction match {
    case addFile: DeltaAddFile => DeltaFileActionKey(addFile.pathAsUri)
    case removeFile: DeltaRemoveFile =>
      DeltaFileActionKey(removeFile.pathAsUri)
    case addCDCFile: DeltaAddCDCFile => DeltaFileActionKey(addCDCFile.pathAsUri)
  }
}

/**
 * Adds a new file to the table. When multiple [[AddFile]] file actions
 * are seen with the same `path` only the metadata from the last one is
 * kept.
 */
case class DeltaAddFile(
    path: String,
    @JsonInclude(JsonInclude.Include.ALWAYS)
    partitionValues: Map[String, String],
    size: Long,
    modificationTime: Long,
    dataChange: Boolean,
    stats: String = null,
    tags: Map[String, String] = null)
  extends DeltaFileAction {
  require(path.nonEmpty)

  override def wrap: DeltaSingleAction = DeltaSingleAction(add = this)

  def remove: DeltaRemoveFile = removeWithTimestamp()

  def removeWithTimestamp(
      timestamp: Long = System.currentTimeMillis(),
      dataChange: Boolean = true): DeltaRemoveFile = {
    // scalastyle:off
    DeltaRemoveFile(
      path = path,
      deletionTimestamp = Some(timestamp),
      dataChange = dataChange,
      extendedFileMetadata = Some(true),
      partitionValues = partitionValues,
      size = Some(size)
    )
    // scalastyle:on
  }
}

/**
 * Logical removal of a given file from the reservoir. Acts as a tombstone before a file is
 * deleted permanently.
 */
case class DeltaRemoveFile(
    path: String,
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    deletionTimestamp: Option[Long],
    dataChange: Boolean = true,
    extendedFileMetadata: Option[Boolean] = None,
    @JsonInclude(JsonInclude.Include.ALWAYS)
    partitionValues: Map[String, String] = null,
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    size: Option[Long] = None)
  extends DeltaFileAction {
  override def wrap: DeltaSingleAction = DeltaSingleAction(remove = this)

  @JsonIgnore
  val delTimestamp: Long = deletionTimestamp.getOrElse(0L)
}

/**
 * A change file containing CDC data for the Delta version it's within. Non-CDC readers should
 * ignore this, CDC readers should scan all ChangeFiles in a version rather than computing
 * changes from AddFile and RemoveFile actions.
 */
case class DeltaAddCDCFile(
    path: String,
    @JsonInclude(JsonInclude.Include.ALWAYS)
    partitionValues: Map[String, String],
    size: Long,
    tags: Map[String, String] = null)
  extends DeltaFileAction {
  override val dataChange = false

  override def wrap: DeltaSingleAction = DeltaSingleAction(cdc = this)
}

case class DeltaFormat(provider: String = "parquet", options: Map[String, String] = Map.empty)

/**
 * Updates the metadata of the table. Only the last update to the [[Metadata]]
 * of a table is kept. It is the responsibility of the writer to ensure that
 * any data already present in the table is still valid after any change.
 */
case class DeltaMetadata(
    id: String = java.util.UUID.randomUUID().toString,
    name: String = null,
    description: String = null,
    format: DeltaFormat = DeltaFormat(),
    schemaString: String = null,
    partitionColumns: Seq[String] = Nil,
    configuration: Map[String, String] = Map.empty,
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    createdTime: Option[Long] = Some(System.currentTimeMillis()))
  extends DeltaAction {

  override def wrap: DeltaSingleAction = DeltaSingleAction(metaData = this)
}

/**
 * Interface for objects that represents the information for a commit. Commits can be referred to
 * using a version and timestamp. The timestamp of a commit comes from the remote storage
 * `lastModifiedTime`, and can be adjusted for clock skew. Hence we have the method `withTimestamp`.
 */
trait DeltaCommitMarker {

  /** Get the timestamp of the commit as millis after the epoch. */
  def getTimestamp: Long

  /** Return a copy object of this object with the given timestamp. */
  def withTimestamp(timestamp: Long): DeltaCommitMarker

  /** Get the version of the commit. */
  def getVersion: Long
}

/**
 * Holds provenance information about changes to the table. This [[Action]]
 * is not stored in the checkpoint and has reduced compatibility guarantees.
 * Information stored in it is best effort (i.e. can be falsified by the writer).
 */
case class DeltaCommitInfo(
    // The commit version should be left unfilled during commit(). When reading a delta file, we can
    // infer the commit version from the file name and fill in this field then.
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    version: Option[Long],
    timestamp: Timestamp,
    userId: Option[String],
    userName: Option[String],
    operation: String,
    @JsonSerialize(using = classOf[JsonMapSerializer])
    operationParameters: Map[String, String],
    job: Option[DeltaJobInfo],
    notebook: Option[DeltaNotebookInfo],
    clusterId: Option[String],
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    readVersion: Option[Long],
    isolationLevel: Option[String],
    /** Whether this commit has blindly appended without caring about existing files */
    isBlindAppend: Option[Boolean],
    operationMetrics: Option[Map[String, String]],
    userMetadata: Option[String],
    tags: Option[Map[String, String]])
  extends DeltaAction
    with DeltaCommitMarker {
  override def wrap: DeltaSingleAction = DeltaSingleAction(commitInfo = this)

  override def withTimestamp(timestamp: Long): DeltaCommitInfo = {
    this.copy(timestamp = new Timestamp(timestamp))
  }

  override def getTimestamp: Long = timestamp.getTime
  @JsonIgnore
  override def getVersion: Long = version.get
}

object DeltaCommitInfo {
  def empty(version: Option[Long] = None): DeltaCommitInfo = {
    DeltaCommitInfo(
      version,
      null,
      None,
      None,
      null,
      null,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      tags = None
    )
  }
}

case class DeltaJobInfo(
    jobId: String,
    jobName: String,
    runId: String,
    jobOwnerId: String,
    triggerType: String)
case class DeltaNotebookInfo(notebookId: String)

/** A serialization helper to create a common action envelope. */
case class DeltaSingleAction(
    add: DeltaAddFile = null,
    remove: DeltaRemoveFile = null,
    metaData: DeltaMetadata = null,
    protocol: DeltaProtocol = null,
    cdc: DeltaAddCDCFile = null,
    commitInfo: DeltaCommitInfo = null) {

  def unwrap: DeltaAction = {
    if (add != null) {
      add
    } else if (remove != null) {
      remove
    } else if (metaData != null) {
      metaData
    } else if (protocol != null) {
      protocol
    } else if (cdc != null) {
      cdc
    } else if (commitInfo != null) {
      commitInfo
    } else {
      null
    }
  }
}

/** Serializes Maps containing JSON strings without extra escaping. */
class JsonMapSerializer extends JsonSerializer[Map[String, String]] {
  def serialize(
      parameters: Map[String, String],
      jgen: JsonGenerator,
      provider: SerializerProvider): Unit = {

    jgen.writeStartObject()
    parameters.foreach {
      case (key, value) =>
        if (value == null) {
          jgen.writeNullField(key)
        } else {
          jgen.writeFieldName(key)
          // Write value as raw data, since it's already JSON text
          jgen.writeRawValue(value)
        }
    }
    jgen.writeEndObject()
  }
}
