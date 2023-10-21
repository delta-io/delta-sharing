package io.whitefox.core.services;

import io.whitefox.core.Provider;
import io.whitefox.core.actions.CreateProvider;
import io.whitefox.core.services.exceptions.MetastoreNotFound;
import io.whitefox.core.services.exceptions.StorageNotFound;
import io.whitefox.persistence.StorageManager;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Clock;
import java.util.Optional;

@ApplicationScoped
public class ProviderService {

  private final StorageManager storageManager;

  private final MetastoreService metastoreService;
  private final StorageService storageService;

  private final Clock clock;

  @Inject
  public ProviderService(
      StorageManager storageManager,
      MetastoreService metastoreService,
      StorageService storageService,
      Clock clock) {
    this.storageManager = storageManager;
    this.metastoreService = metastoreService;
    this.storageService = storageService;
    this.clock = clock;
  }

  public Provider createProvider(CreateProvider createProvider) {
    var metastore = createProvider.metastoreName().map(mName -> metastoreService
        .getMetastore(mName)
        .orElseThrow(() -> new MetastoreNotFound(mName)));
    var storage = storageService
        .getStorage(createProvider.storageName())
        .orElseThrow(() -> new StorageNotFound(createProvider.storageName()));
    var provider = new Provider(
        createProvider.name(),
        storage,
        metastore,
        clock.millis(),
        createProvider.currentUser(),
        clock.millis(),
        createProvider.currentUser(),
        createProvider.currentUser());
    return storageManager.createProvider(provider);
  }

  public Optional<Provider> getProvider(String name) {
    return storageManager.getProvider(name);
  }
}
