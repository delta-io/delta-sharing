package io.whitefox.core.services;

import io.whitefox.core.Storage;
import org.apache.iceberg.io.FileIO;

public interface FileIOFactory {
  FileIO newFileIO(Storage storage);
}
