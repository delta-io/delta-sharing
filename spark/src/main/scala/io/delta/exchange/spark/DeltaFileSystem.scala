package io.delta.exchange.spark

import java.net.URI
import java.util.Base64
import java.nio.charset.StandardCharsets.UTF_8
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileStatus, FileSystem, Path}
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable

/** Read-only file system for delta paths. */
class DeltaFileSystem extends FileSystem {
  import DeltaFileSystem._

  override def getScheme: String = "delta"
  override def getUri(): URI = URI.create("delta:///")

  override def open(f: Path, bufferSize: Int): FSDataInputStream = {
    new FSDataInputStream(new HttpInputStream(restoreUri(f)._1))
  }

  override def create(
      f: Path,
      permission: FsPermission,
      overwrite: Boolean,
      bufferSize: Int,
      replication: Short,
      blockSize: Long,
      progress: Progressable): FSDataOutputStream =
    throw new UnsupportedOperationException("create")

  override def append(f: Path, bufferSize: Int, progress: Progressable): FSDataOutputStream =
    throw new UnsupportedOperationException("append")

  override def rename(src: Path, dst: Path): Boolean =
    throw new UnsupportedOperationException("rename")

  override def delete(f: Path, recursive: Boolean): Boolean =
    throw new UnsupportedOperationException("delete")

  override def listStatus(f: Path): Array[FileStatus] =
    throw new UnsupportedOperationException("rename")

  override def setWorkingDirectory(new_dir: Path): Unit =
    throw new UnsupportedOperationException("rename")

  override def getWorkingDirectory: Path = new Path(getUri)

  override def mkdirs(f: Path, permission: FsPermission): Boolean =
    throw new UnsupportedOperationException("rename")

  override def getFileStatus(f: Path): FileStatus = {
    new FileStatus(restoreUri(f)._2, false, 0, 1, 0, f)
  }
}

object DeltaFileSystem {

  def createPath(uri: URI, size: Long): Path = {
    val uriWithSize = s"$uri?size=$size"
    val encoded = new String(Base64.getUrlEncoder.encode(uriWithSize.getBytes(UTF_8)), UTF_8)
    new Path(new URI("delta:///" + encoded))
  }

  def restoreUri(path: Path): (URI, Long) = {
    val encoded = path.toUri.toString.stripPrefix("delta:///").getBytes(UTF_8)
    val uriWithSize = new String(Base64.getUrlDecoder.decode(encoded), UTF_8)
    val i = uriWithSize.lastIndexOf("?size=")
    assert(i >= 0)
    val uri = uriWithSize.substring(0, i)
    val size = uriWithSize.substring(i + "?size=".length).toLong
    (new URI(uri), size)
  }
}
