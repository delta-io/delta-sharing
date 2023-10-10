package io.whitefox.api.deltasharing.loader;

import io.whitefox.api.deltasharing.DeltaSharedTable;
import io.whitefox.persistence.memory.PTable;

public interface TableLoader<T> {

  DeltaSharedTable loadTable(PTable pTable);
}
