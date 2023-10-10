package io.whitefox.core.services;

import io.whitefox.core.Table;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class DeltaShareTableLoader implements TableLoader<DeltaSharedTable> {

  @Override
  public DeltaSharedTable loadTable(Table table) {
    return DeltaSharedTable.of(table);
  }
}
