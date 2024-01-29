package io.whitefox.core.services;

import io.whitefox.core.Metadata;
import io.whitefox.core.ReadTableRequest;
import io.whitefox.core.ReadTableResultToBeSigned;
import java.sql.Timestamp;
import java.util.Optional;

public interface InternalSharedTable {

  Optional<Metadata> getMetadata(Optional<Timestamp> startingTimestamp);

  Optional<Long> getTableVersion(Optional<Timestamp> startingTimestamp);

  ReadTableResultToBeSigned queryTable(ReadTableRequest readTableRequest);
}
