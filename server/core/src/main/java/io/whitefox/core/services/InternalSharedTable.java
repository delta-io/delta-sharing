package io.whitefox.core.services;

import io.whitefox.core.Metadata;
import io.whitefox.core.ReadTableRequest;
import io.whitefox.core.ReadTableResultToBeSigned;
import java.util.Optional;

public interface InternalSharedTable {

  Optional<Metadata> getMetadata(Optional<String> startingTimestamp);

  Optional<Long> getTableVersion(Optional<String> startingTimestamp);

  ReadTableResultToBeSigned queryTable(ReadTableRequest readTableRequest);
}
