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

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.test.SharedSparkSession

import io.delta.sharing.spark.model.{DeltaTableFiles, DeltaTableMetadata, Metadata, Protocol, Table}

class RemoteDeltaLogSuite extends SparkFunSuite with SharedSparkSession {

  test("parsePath") {
    assert(RemoteDeltaLog.parsePath("file:///foo/bar#a.b.c") == ("file:///foo/bar", "a", "b", "c"))
    assert(RemoteDeltaLog.parsePath("file:///foo/bar#bar#a.b.c") ==
      ("file:///foo/bar#bar", "a", "b", "c"))
    assert(RemoteDeltaLog.parsePath("file:///foo/bar#bar#a.b.c ") ==
      ("file:///foo/bar#bar", "a", "b", "c "))
    intercept[IllegalArgumentException] {
      RemoteDeltaLog.parsePath("file:///foo/bar")
    }
    intercept[IllegalArgumentException] {
      RemoteDeltaLog.parsePath("file:///foo/bar#a.b")
    }
    intercept[IllegalArgumentException] {
      RemoteDeltaLog.parsePath("file:///foo/bar#a.b.c.d")
    }
    intercept[IllegalArgumentException] {
      RemoteDeltaLog.parsePath("#a.b.c")
    }
    intercept[IllegalArgumentException] {
      RemoteDeltaLog.parsePath("foo#a.b.")
    }
    intercept[IllegalArgumentException] {
      RemoteDeltaLog.parsePath("foo#a.b.c.")
    }
  }

  test("RemoteSnapshot getFiles with limit") {
    class DummySharingClient extends DeltaSharingClient {
      var limits = Seq.empty[Long]

      override def listAllTables(): Seq[Table] = Nil

      override def getMetadata(table: Table): DeltaTableMetadata = {
        DeltaTableMetadata(0, Protocol(0), Metadata())
      }

      override def getTableVersion(table: Table): Long = 0

      override def getFiles(
          table: Table,
          predicates: Seq[String],
          limit: Option[Long]): DeltaTableFiles = {
        limit.foreach(lim => limits = limits :+ lim)
        DeltaTableFiles(0, Protocol(0), Metadata(), Nil)
      }
    }

    val spark = SparkSession.active

    // sanity check for dummy client
    val client = new DummySharingClient()
    client.getFiles(Table("fe", "fi", "fo"), Nil, Some(2L))
    client.getFiles(Table("fe", "fi", "fo"), Nil, Some(3L))
    assert(client.limits === Seq(2L, 3L))
    client.limits = client.limits.take(0)

    // check snapshot
    val snapshot = new RemoteSnapshot(client, Table("fe", "fi", "fo"))
    snapshot.filesForScan(Nil, Some(2L))
    assert(client.limits === Seq(2L))
    client.limits = client.limits.take(0)

    // check RemoteDeltaFileIndex
    val remoteDeltaLog = new RemoteDeltaLog(Table("fe", "fi", "fo"), new Path("test"), client)
    val fileIndex = {
      new RemoteDeltaFileIndex(spark, remoteDeltaLog, new Path("test"), snapshot, Some(2L))
    }
    fileIndex.listFiles(Seq.empty, Seq.empty)
    assert(client.limits === Seq(2L))
    client.limits = client.limits.take(0)
  }
}
