package io.whitefox.core.services;

import io.whitefox.core.SharedTable;

public interface TableLoader<T> {

  DeltaSharedTable loadTable(SharedTable sharedTable);
}
