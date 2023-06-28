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

package io.delta.sharing.client

import java.io.{ByteArrayInputStream, EOFException}
import java.net.URI

import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.{PositionedReadable, Seekable}

/** An input stream that holds the entire content in memory to provide random access. */
private[sharing] class InMemoryHttpInputStream(uri: URI)
  extends ByteArrayInputStream(IOUtils.toByteArray(uri)) with Seekable with PositionedReadable {

  override def seek(pos: Long): Unit = synchronized {
    this.pos = pos.toInt
  }

  override def getPos: Long = synchronized {
    pos
  }

  override def seekToNewSource(targetPos: Long): Boolean = {
    // We don't support this feature
    false
  }

  override def read(
      position: Long,
      buffer: Array[Byte],
      offset: Int,
      length: Int): Int = synchronized {
    val oldPos = getPos()
    var nread = -1
    try {
      seek(position)
      nread = read(buffer, offset, length)
    } finally {
      seek(oldPos)
    }
    return nread
  }

  override def readFully(
      position: Long,
      buffer: Array[Byte],
      offset: Int,
      length: Int): Unit = synchronized {
    var nread = 0
    while (nread < length) {
      val nbytes = read(position + nread, buffer, offset + nread, length - nread)
      if (nbytes < 0) {
        throw new EOFException("End of file reached before reading fully.");
      }
      nread += nbytes
    }
  }

  override def readFully(position: Long, buffer: Array[Byte]): Unit = {
    readFully(position, buffer, 0, buffer.length)
  }
}
