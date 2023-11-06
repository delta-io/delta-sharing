package io.whitefox.persistence.memory;

import io.whitefox.annotations.SkipCoverageGenerated;
import io.whitefox.core.*;
import io.whitefox.core.Storage;
import io.whitefox.core.services.exceptions.MetastoreAlreadyExists;
import io.whitefox.core.services.exceptions.ProviderAlreadyExists;
import io.whitefox.core.services.exceptions.StorageAlreadyExists;
import io.whitefox.persistence.StorageManager;
import io.whitefox.persistence.exceptions.InvalidPageTokenException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;

@ApplicationScoped
public class InMemoryStorageManager implements StorageManager {
  private final ConcurrentMap<String, Share> shares;
  private final ConcurrentMap<String, Metastore> metastores;
  private final ConcurrentMap<String, Storage> storages;
  private final ConcurrentMap<String, Provider> providers;

  @Inject
  public InMemoryStorageManager() {
    this.storages = new ConcurrentHashMap<>();
    this.shares = new ConcurrentHashMap<>();
    this.metastores = new ConcurrentHashMap<>();
    this.providers = new ConcurrentHashMap<>();
  }

  public InMemoryStorageManager(
      List<Share> shares, List<Metastore> metastores, List<Storage> storages) {
    this.shares = new ConcurrentHashMap<>(
        shares.stream().collect(Collectors.toMap(Share::name, Function.identity())));
    this.metastores = new ConcurrentHashMap<>(
        metastores.stream().collect(Collectors.toMap(Metastore::name, Function.identity())));
    this.storages = new ConcurrentHashMap<>(
        storages.stream().collect(Collectors.toMap(Storage::name, Function.identity())));
    this.providers = new ConcurrentHashMap<>();
  }

  public InMemoryStorageManager(List<Share> shares) {
    this(shares, Collections.emptyList(), Collections.emptyList());
  }

  @Override
  public Optional<Share> getShare(String share) {
    return Optional.ofNullable(shares.get(share));
  }

  @Override
  public Optional<SharedTable> getSharedTable(String share, String schema, String table) {

    return Optional.ofNullable(shares.get(share))
        .flatMap(shareObj -> Optional.ofNullable(shareObj.schemas().get(schema)))
        .flatMap(schemaObj ->
            schemaObj.tables().stream().filter(t -> (t.name().equals(table))).findFirst());
  }

  @Override
  public ResultAndTotalSize<List<Share>> getShares(int offset, int maxResultSize) {
    var totalSize = shares.size();
    if (offset > totalSize) {
      throw new InvalidPageTokenException(
          String.format("Invalid Next Page Token: token %s is larger than totalSize", offset));
    } else {
      return new ResultAndTotalSize<>(
          shares.values().stream().skip(offset).limit(maxResultSize).collect(Collectors.toList()),
          totalSize);
    }
  }

  @Override
  public Optional<ResultAndTotalSize<List<Schema>>> listSchemas(
      String share, int offset, int maxResultSize) {
    return Optional.ofNullable(shares.get(share)).flatMap(shareObj -> {
      var schemaMap = shareObj.schemas();
      var totalSize = schemaMap.size();
      if (offset > totalSize) {
        throw new InvalidPageTokenException(
            String.format("Invalid Next Page Token: token %s is larger than totalSize", offset));
      }
      return Optional.of(new ResultAndTotalSize<>(
          schemaMap.values().stream()
              .skip(offset)
              .limit(maxResultSize)
              .collect(Collectors.toList()),
          totalSize));
    });
  }

  @Override
  public Optional<ResultAndTotalSize<List<SharedTable>>> listTables(
      String share, String schema, int offset, int maxResultSize) {
    return Optional.ofNullable(shares.get(share))
        .flatMap(shareObj -> Optional.ofNullable(shareObj.schemas().get(schema)))
        .flatMap(schemaObj -> {
          var tableList = schemaObj.tables();
          var totalSize = tableList.size();
          if (offset > totalSize) {
            throw new InvalidPageTokenException(String.format(
                "Invalid Next Page Token: token %s is larger than totalSize", offset));
          } else {
            return Optional.of(new ResultAndTotalSize<>(
                tableList.stream().skip(offset).limit(maxResultSize).collect(Collectors.toList()),
                totalSize));
          }
        });
  }

  private static final class SharedTableAndSchema {
    private final SharedTable table;
    private final Schema schema;

    private SharedTableAndSchema(SharedTable sharedTable, Schema schema) {
      this.table = sharedTable;
      this.schema = schema;
    }

    public SharedTable table() {
      return table;
    }

    public Schema schema() {
      return schema;
    }

    @Override
    @SkipCoverageGenerated
    public boolean equals(Object obj) {
      if (obj == this) return true;
      if (obj == null || obj.getClass() != this.getClass()) return false;
      var that = (SharedTableAndSchema) obj;
      return Objects.equals(this.table, that.table) && Objects.equals(this.schema, that.schema);
    }

    @Override
    @SkipCoverageGenerated
    public int hashCode() {
      return Objects.hash(table, schema);
    }

    @Override
    @SkipCoverageGenerated
    public String toString() {
      return "SharedTableAndSchema[" + "table=" + table + ", " + "schema=" + schema + ']';
    }
  }

  @Override
  public Optional<ResultAndTotalSize<List<SharedTable>>> listTablesOfShare(
      String share, int offset, int maxResultSize) {
    return Optional.ofNullable(shares.get(share)).flatMap(shareObj -> {
      var schemaMap = shareObj.schemas();
      var tableList = schemaMap.values().stream()
          .flatMap(x -> x.tables().stream().map(t -> new SharedTableAndSchema(t, x)))
          .collect(Collectors.toList());

      var totalSize = tableList.size();
      if (offset > totalSize) {
        throw new InvalidPageTokenException(
            String.format("Invalid Next Page Token: token %s is larger than totalSize", offset));
      } else {
        return Optional.of(new ResultAndTotalSize<>(
            tableList.stream()
                .skip(offset)
                .limit(maxResultSize)
                .map(t -> t.table)
                .collect(Collectors.toList()),
            totalSize));
      }
    });
  }

  @Override
  public Metastore createMetastore(Metastore metastore) {
    if (metastores.get(metastore.name()) != null) {
      throw new MetastoreAlreadyExists(
          "Metastore with name " + metastore.name() + " already exists");
    } else {
      metastores.put(metastore.name(), metastore);
      return metastore;
    }
  }

  @Override
  public Optional<Metastore> getMetastore(String name) {
    return Optional.ofNullable(metastores.get(name));
  }

  public Storage createStorage(Storage storage) {
    if (storages.get(storage.name()) != null) {
      throw new StorageAlreadyExists("Storage with name " + storage.name() + " already exists");
    } else {
      storages.put(storage.name(), storage);
      return storage;
    }
  }

  public Optional<Storage> getStorage(String name) {
    return Optional.ofNullable(storages.get(name));
  }

  @Override
  public Provider createProvider(Provider provider) {
    if (providers.get(provider.name()) != null) {
      throw new ProviderAlreadyExists("Provider with name " + provider.name() + " already exists");
    } else {
      providers.put(provider.name(), provider);
      return provider;
    }
  }

  @Override
  public Optional<Provider> getProvider(String name) {
    return Optional.ofNullable(providers.get(name));
  }

  @Override
  public InternalTable createInternalTable(InternalTable internalTable) {
    providers.put(
        internalTable.provider().name(),
        providers.get(internalTable.provider().name()).addTable(internalTable));
    return internalTable;
  }

  @Override
  public Share createShare(Share share) {
    shares.put(share.name(), share);
    return shares.get(share.name());
  }

  @Override
  public Share updateShare(Share newShare) {
    shares.put(newShare.name(), newShare);
    return shares.get(newShare.name());
  }

  @Override
  public Share addTableToSchema(
      Share shareObj,
      Schema schemaObj,
      Provider providerObj,
      InternalTable table,
      SharedTableName sharedTableName,
      Principal currentUser,
      long millis) {
    var newSchema = schemaObj.addTable(table, sharedTableName);
    var newShare = shareObj.upsertSchema(newSchema, currentUser, millis);
    return updateShare(newShare);
  }
}
