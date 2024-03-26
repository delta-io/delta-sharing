package io.whitefox.core.services;

import io.whitefox.core.TableFile;
import io.whitefox.core.TableFileToBeSigned;

public interface FileSigner extends AutoCloseable {
  TableFile sign(TableFileToBeSigned s);
}
