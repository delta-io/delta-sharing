package io.whitefox.core.services;

import io.whitefox.core.Table;

public interface TableLoader<T> {

  DeltaSharedTable loadTable(Table table);
}
