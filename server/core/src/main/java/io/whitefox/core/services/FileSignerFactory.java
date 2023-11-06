package io.whitefox.core.services;

import io.whitefox.core.Storage;

public interface FileSignerFactory {

  FileSigner newFileSigner(Storage storage);
}
