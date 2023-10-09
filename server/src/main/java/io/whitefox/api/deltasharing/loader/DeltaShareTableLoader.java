package io.whitefox.api.deltasharing.loader;

import io.whitefox.api.deltasharing.DeltaSharedTable;
import io.whitefox.persistence.memory.PTable;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.concurrent.CompletionStage;

@ApplicationScoped
public class DeltaShareTableLoader implements TableLoader<DeltaSharedTable> {

  @Override
  public CompletionStage<DeltaSharedTable> loadTable(PTable pTable) {
    return DeltaSharedTable.of(pTable);
  }
}
