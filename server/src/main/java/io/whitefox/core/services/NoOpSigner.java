package io.whitefox.core.services;

import io.whitefox.core.TableFile;
import io.whitefox.core.TableFileToBeSigned;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.Optional;

@ApplicationScoped
public class NoOpSigner implements FileSigner {
  @Override
  public TableFile sign(TableFileToBeSigned s) {
    return new TableFile(
        s.url(),
        s.url(), // maybe we can hash this
        s.size(),
        Optional.of(s.version()),
        s.timestamp(),
        s.partitionValues(),
        Long.MAX_VALUE,
        Optional.of(s.stats()));
  }
}
