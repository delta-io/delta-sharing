package io.whitefox.persistence.memory;

import io.whitefox.api.deltasharing.encoders.InvalidPageTokenException;
import io.whitefox.core.ResultAndTotalSize;
import io.whitefox.core.Schema;
import io.whitefox.core.Share;
import io.whitefox.core.Table;
import io.whitefox.persistence.StorageManager;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;

@ApplicationScoped
public class InMemoryStorageManager implements StorageManager {
  private final ConcurrentMap<String, Share> shares;

  @Inject
  public InMemoryStorageManager() {
    this.shares = new ConcurrentHashMap<>();
  }

  public InMemoryStorageManager(List<Share> shares) {
    this.shares = new ConcurrentHashMap<>(
        shares.stream().collect(Collectors.toMap(Share::name, Function.identity())));
  }

  @Override
  public Optional<Share> getShare(String share) {
    return Optional.ofNullable(shares.get(share));
  }

  @Override
  public Optional<Table> getTable(String share, String schema, String table) {

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
  public Optional<ResultAndTotalSize<List<Table>>> listTables(
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

  private record TableAndSchema(Table table, Schema schema) {}

  @Override
  public Optional<ResultAndTotalSize<List<Table>>> listTablesOfShare(
      String share, int offset, int maxResultSize) {
    return Optional.ofNullable(shares.get(share)).flatMap(shareObj -> {
      var schemaMap = shareObj.schemas();
      var tableList = schemaMap.values().stream()
          .flatMap(x -> x.tables().stream().map(t -> new TableAndSchema(t, x)))
          .toList();

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
}
