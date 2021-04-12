package io.delta.exchange.spark

import java.io.{ByteArrayInputStream, EOFException}
import java.net.URI

import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.{PositionedReadable, Seekable}

/** TODO Implement a real random access input stream. */
class HttpInputStream(uri: URI)
  extends ByteArrayInputStream(IOUtils.toByteArray(uri)) with Seekable with PositionedReadable {

  override def seek(pos: Long): Unit = synchronized {
    this.pos = pos.toInt
  }

  override def getPos: Long = synchronized {
    pos
  }

  override def seekToNewSource(targetPos: Long): Boolean =
    throw new UnsupportedOperationException("seekToNewSource")

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
