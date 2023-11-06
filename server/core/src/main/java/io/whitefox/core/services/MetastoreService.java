package io.whitefox.core.services;

import io.whitefox.core.Metastore;
import io.whitefox.core.actions.CreateMetastore;
import io.whitefox.persistence.StorageManager;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Clock;
import java.util.Optional;

@ApplicationScoped
public class MetastoreService {

  private final StorageManager storageManager;

  private final Clock clock;

  public MetastoreService(StorageManager storageManager, Clock clock) {
    this.storageManager = storageManager;
    this.clock = clock;
  }

  public Metastore createMetastore(CreateMetastore metastore) {
    return storageManager.createMetastore(validate(metastore));
  }

  public Optional<Metastore> getMetastore(String name) {
    return storageManager.getMetastore(name);
  }

  public Metastore validate(CreateMetastore metastore) {
    // always valid, real impl will throw an exception if not valid
    if (metastore.skipValidation()) {
      return new Metastore(
          metastore.name(),
          metastore.comment(),
          metastore.currentUser(),
          metastore.type(),
          metastore.properties(),
          Optional.empty(),
          clock.millis(),
          metastore.currentUser(),
          clock.millis(),
          metastore.currentUser());
    } else {
      return new Metastore(
          metastore.name(),
          metastore.comment(),
          metastore.currentUser(),
          metastore.type(),
          metastore.properties(),
          Optional.of(clock.millis()),
          clock.millis(),
          metastore.currentUser(),
          clock.millis(),
          metastore.currentUser());
    }
  }
}
