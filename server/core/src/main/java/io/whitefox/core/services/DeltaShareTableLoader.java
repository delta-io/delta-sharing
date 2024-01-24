package io.whitefox.core.services;

import io.whitefox.core.SharedTable;

public class DeltaShareTableLoader implements TableLoader {

  @Override
  public DeltaSharedTable loadTable(SharedTable sharedTable) {
    return DeltaSharedTable.of(sharedTable);
  }
}
