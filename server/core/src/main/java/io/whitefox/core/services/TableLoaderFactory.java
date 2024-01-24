package io.whitefox.core.services;

import io.whitefox.core.InternalTable;

public interface TableLoaderFactory {

  TableLoader newTableLoader(InternalTable internalTable);
}
