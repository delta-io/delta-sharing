package io.whitefox.core.services;

import io.whitefox.core.SharedTable;

public interface TableLoader {

  InternalSharedTable loadTable(SharedTable sharedTable);
}
