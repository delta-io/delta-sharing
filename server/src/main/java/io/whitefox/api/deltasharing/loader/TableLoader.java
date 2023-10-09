package io.whitefox.api.deltasharing.loader;

import io.whitefox.api.deltasharing.DeltaSharedTable;
import io.whitefox.persistence.memory.PTable;
import java.util.concurrent.CompletionStage;

public interface TableLoader<T> {

  CompletionStage<DeltaSharedTable> loadTable(PTable pTable);
}
