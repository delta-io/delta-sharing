package io.whitefox.api.deltasharing.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

/**
 * This class is test-only and it's needed because we can't know in advance pre-signed urls, in this way we can
 * easily run assertions on FileObjects that contain pre-signed urls ignoring the url.
 */
public class FileObjectWithoutPresignedUrl {

  private FileObjectFileWithoutPresignedUrl _file;

  public FileObjectWithoutPresignedUrl _file(FileObjectFileWithoutPresignedUrl _file) {
    this._file = _file;
    return this;
  }

  @JsonProperty("file")
  public FileObjectFileWithoutPresignedUrl getFile() {
    return _file;
  }

  @JsonProperty("file")
  public void setFile(FileObjectFileWithoutPresignedUrl _file) {
    this._file = _file;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FileObjectWithoutPresignedUrl fileObject = (FileObjectWithoutPresignedUrl) o;
    return Objects.equals(this._file, fileObject._file);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_file);
  }
}
