package io.whitefox.api.deltasharing.loader;

import io.whitefox.api.deltasharing.DeltaSharedTable;
import io.whitefox.persistence.memory.PTable;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class DeltaShareTableLoader implements TableLoader<DeltaSharedTable> {

  @Override
  public DeltaSharedTable loadTable(PTable pTable) {
    return DeltaSharedTable.of(pTable);
  }
}
