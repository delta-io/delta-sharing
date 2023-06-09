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

import java.io.File
import java.nio.file.Files
import java.util.UUID

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.catalyst.util.DateTimeUtils._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.unsafe.types.UTF8String

object TestUtils {

  def withTempDir(f: File => Unit): Unit = {
    val dir = Files.createTempDirectory(UUID.randomUUID().toString).toFile
    try f(dir) finally {
      FileUtils.deleteDirectory(dir)
    }
  }

  def withTempDirs(f: (File, File) => Unit): Unit = {
    withTempDir { file1 =>
      withTempDir { file2 =>
        f(file1, file2)
      }
    }
  }

  def sqlDate(date: String): java.sql.Date = {
    toJavaDate(stringToDate(UTF8String.fromString(date)).get)
  }

  def sqlTimestamp(timestamp: String): java.sql.Timestamp = {
    toJavaTimestamp(stringToTimestamp(
      UTF8String.fromString(timestamp),
      getZoneId(SQLConf.get.sessionLocalTimeZone)).get)
  }
}
