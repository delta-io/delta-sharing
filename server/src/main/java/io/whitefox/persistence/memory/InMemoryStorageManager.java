package io.whitefox.persistence.memory;

import io.whitefox.api.deltasharing.encoders.InvalidPageTokenException;
import io.whitefox.api.deltasharing.model.Schema;
import io.whitefox.api.deltasharing.model.Share;
import io.whitefox.api.deltasharing.model.Table;
import io.whitefox.persistence.ResultAndTotalSize;
import io.whitefox.persistence.StorageManager;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;

@ApplicationScoped
public class InMemoryStorageManager implements StorageManager {
  private final ConcurrentMap<String, PShare> shares;

  private static Share pShareToShare(PShare p) {
    return new Share().id(p.id()).name(p.name());
  }

  private static Schema pSchemaToSchema(PSchema schema, PShare share) {
    return new Schema().name(schema.name()).share(share.name());
  }

  private static Table pTableToTable(PTable table, PSchema schema, PShare share) {
    return new Table().name(table.name()).share(share.name()).schema(schema.name());
  }

  @Inject
  public InMemoryStorageManager() {
    this.shares = new ConcurrentHashMap<>();
  }

  public InMemoryStorageManager(List<PShare> shares) {
    this.shares = new ConcurrentHashMap<>(
        shares.stream().collect(Collectors.toMap(PShare::name, Function.identity())));
  }

  @Override
  public CompletionStage<Optional<Share>> getShare(String share) {
    return CompletableFuture.completedFuture(
        Optional.ofNullable(shares.get(share)).map(InMemoryStorageManager::pShareToShare));
  }

  @Override
  public CompletionStage<ResultAndTotalSize<List<Share>>> getShares(int offset, int maxResultSize) {
    var totalSize = shares.size();
    if (offset > totalSize) {
      return CompletableFuture.failedFuture(new InvalidPageTokenException(
          String.format("Invalid Next Page Token: token %s is larger than totalSize", offset)));
    } else {
      return CompletableFuture.completedFuture(new ResultAndTotalSize<>(
          shares.values().stream()
              .skip(offset)
              .limit(maxResultSize)
              .map(InMemoryStorageManager::pShareToShare)
              .collect(Collectors.toList()),
          totalSize));
    }
  }

  @Override
  public CompletionStage<Optional<ResultAndTotalSize<List<Schema>>>> listSchemas(
      String share, int offset, int maxResultSize) {
    var shareObj = shares.get(share);
    if (shareObj == null) {
      return CompletableFuture.completedFuture(Optional.empty());
    } else {
      var schemaMap = shareObj.schemas();
      var totalSize = schemaMap.size();
      if (offset > totalSize) {
        return CompletableFuture.failedFuture(new InvalidPageTokenException(
            String.format("Invalid Next Page Token: token %s is larger than totalSize", offset)));
      }
      return CompletableFuture.completedFuture(Optional.of(new ResultAndTotalSize<>(
          schemaMap.values().stream()
              .skip(offset)
              .limit(maxResultSize)
              .map(s -> pSchemaToSchema(s, shareObj))
              .collect(Collectors.toList()),
          totalSize)));
    }
  }

  @Override
  public CompletionStage<Optional<ResultAndTotalSize<List<Table>>>> listTables(
      String share, String schema, int offset, int maxResultSize) {
    var shareObj = shares.get(share);
    if (shareObj == null) {
      return CompletableFuture.completedFuture(Optional.empty());
    }
    var schemaMap = shareObj.schemas();
    var schemaObj = schemaMap.get(schema);
    if (schemaObj == null) {
      return CompletableFuture.completedFuture(Optional.empty());
    }
    var tableList = schemaObj.tables();
    var totalSize = tableList.size();
    if (offset > totalSize) {
      return CompletableFuture.failedFuture(new InvalidPageTokenException(
          String.format("Invalid Next Page Token: token %s is larger than totalSize", offset)));
    } else {
      return CompletableFuture.completedFuture(Optional.of(new ResultAndTotalSize<>(
          tableList.stream()
              .skip(offset)
              .limit(maxResultSize)
              .map(t -> pTableToTable(t, schemaObj, shareObj))
              .collect(Collectors.toList()),
          totalSize)));
    }
  }

  private record TableAndSchema(PTable table, PSchema schema) {}
  ;

  @Override
  public CompletionStage<Optional<ResultAndTotalSize<List<Table>>>> listTablesOfShare(
      String share, int offset, int maxResultSize) {

    var shareObj = shares.get(share);
    if (shareObj == null) {
      return CompletableFuture.completedFuture(Optional.empty());
    }
    var schemaMap = shareObj.schemas();

    var tableList = schemaMap.values().stream()
        .flatMap(x -> x.tables().stream().map(t -> new TableAndSchema(t, x)))
        .toList();

    var totalSize = tableList.size();
    if (offset > totalSize) {
      return CompletableFuture.failedFuture(new InvalidPageTokenException(
          String.format("Invalid Next Page Token: token %s is larger than totalSize", offset)));
    } else {
      return CompletableFuture.completedFuture(Optional.of(new ResultAndTotalSize<>(
          tableList.stream()
              .skip(offset)
              .limit(maxResultSize)
              .map(t -> pTableToTable(t.table(), t.schema(), shareObj))
              .collect(Collectors.toList()),
          totalSize)));
    }
  }
}
