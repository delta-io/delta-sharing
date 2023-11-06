package io.whitefox.core.services;

import io.whitefox.core.InternalTable;
import io.whitefox.core.Principal;
import io.whitefox.core.Provider;
import io.whitefox.core.actions.CreateInternalTable;
import io.whitefox.core.services.exceptions.ProviderNotFound;
import io.whitefox.persistence.StorageManager;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Clock;
import java.util.Optional;

@ApplicationScoped
public class TableService {

  private final StorageManager storageManager;
  private final Clock clock;
  private final ProviderService providerService;

  public TableService(StorageManager storageManager, Clock clock, ProviderService providerService) {
    this.storageManager = storageManager;
    this.clock = clock;
    this.providerService = providerService;
  }

  public InternalTable createInternalTable(
      String provider, Principal currentUser, CreateInternalTable createTable) {
    var providerObj = providerService.getProvider(provider);
    return providerObj
        .map(p -> validate(createTable, currentUser, p))
        .map(storageManager::createInternalTable)
        .orElseThrow(() -> new ProviderNotFound("Provider " + provider + " not found"));
  }

  public Optional<InternalTable> getInternalTable(String provider, String name) {
    var providerObj = storageManager
        .getProvider(provider)
        .orElseThrow(() -> new ProviderNotFound("Provider " + provider + " not found"));
    return Optional.ofNullable(providerObj.tables().get(name));
  }

  private InternalTable validate(
      CreateInternalTable createTable, Principal currentUser, Provider provider) {
    if (createTable.skipValidation()) {
      return new InternalTable(
          createTable.name(),
          createTable.comment(),
          createTable.properties(),
          Optional.empty(),
          clock.millis(),
          currentUser,
          clock.millis(),
          currentUser,
          provider);
    }
    if (createTable.properties() instanceof InternalTable.IcebergTableProperties
        && provider.metastore().isEmpty()) {
      throw new IllegalArgumentException(String.format(
          "Cannot create iceberg table %s without a metastore for provider %s",
          createTable.name(), provider.name()));
    }
    return new InternalTable(
        createTable.name(),
        createTable.comment(),
        createTable.properties(),
        Optional.of(clock.millis()),
        clock.millis(),
        currentUser,
        clock.millis(),
        currentUser,
        provider);
  }
}
