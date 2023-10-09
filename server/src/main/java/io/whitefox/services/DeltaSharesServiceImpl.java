package io.whitefox.services;

import io.whitefox.api.deltasharing.encoders.DeltaPageTokenEncoder;
import io.whitefox.api.deltasharing.loader.DeltaShareTableLoader;
import io.whitefox.api.deltasharing.model.Schema;
import io.whitefox.api.deltasharing.model.Share;
import io.whitefox.api.deltasharing.model.Table;
import io.whitefox.persistence.StorageManager;
import io.whitefox.persistence.memory.PSchema;
import io.whitefox.persistence.memory.PShare;
import io.whitefox.persistence.memory.PTable;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class DeltaSharesServiceImpl implements DeltaSharesService {

  private final StorageManager storageManager;
  private final Integer defaultMaxResults;
  private final DeltaPageTokenEncoder encoder;
  private final DeltaShareTableLoader tableLoader;

  private static Share pShareToShare(PShare p) {
    return new Share().id(p.id()).name(p.name());
  }

  private static Schema pSchemaToSchema(PSchema schema) {
    return new Schema().name(schema.name()).share(schema.share());
  }

  private static Table pTableToTable(PTable table) {
    return new Table().name(table.name()).share(table.share()).schema(table.schema());
  }

  @Inject
  public DeltaSharesServiceImpl(
      StorageManager storageManager,
      @ConfigProperty(name = "io.delta.sharing.api.server.defaultMaxResults")
          Integer defaultMaxResults,
      DeltaPageTokenEncoder encoder,
      DeltaShareTableLoader tableLoader) {
    this.storageManager = storageManager;
    this.defaultMaxResults = defaultMaxResults;
    this.encoder = encoder;
    this.tableLoader = tableLoader;
  }

  @Override
  public CompletionStage<Optional<Share>> getShare(String share) {
    return storageManager
        .getShare(share)
        .thenApplyAsync(o -> o.map(DeltaSharesServiceImpl::pShareToShare));
  }

  @Override
  public CompletionStage<Optional<Long>> getTableVersion(
      String share, String schema, String table, String startingTimestamp) {
    return storageManager.getTable(share, schema, table).thenCompose(optTable -> optTable
        .map(t -> tableLoader
            .loadTable(t)
            .thenApply(dst -> dst.getTableVersion(Optional.ofNullable(startingTimestamp))))
        .orElse(CompletableFuture.completedFuture(Optional.empty())));
  }

  @Override
  public CompletionStage<ContentAndToken<List<Share>>> listShares(
      Optional<ContentAndToken.Token> nextPageToken, Optional<Integer> maxResults) {
    Integer finalMaxResults = maxResults.orElse(defaultMaxResults);
    Integer start = nextPageToken
        .map(s -> Integer.valueOf(encoder.decodePageToken(s.value)))
        .orElse(0);
    var resAndSize = storageManager.getShares(start, finalMaxResults);
    int end = start + finalMaxResults;

    return resAndSize.thenApplyAsync(pageContent -> {
      Optional<String> optionalToken =
          end < pageContent.size ? Optional.of(Integer.toString(end)) : Optional.empty();
      var content =
          pageContent.result.stream().map(DeltaSharesServiceImpl::pShareToShare).toList();
      return optionalToken
          .map(encoder::encodePageToken)
          .map(t -> ContentAndToken.of(content, t))
          .orElse(ContentAndToken.withoutToken(content));
    });
  }

  @Override
  public CompletionStage<Optional<ContentAndToken<List<Schema>>>> listSchemas(
      String share, Optional<ContentAndToken.Token> nextPageToken, Optional<Integer> maxResults) {
    Integer finalMaxResults = maxResults.orElse(defaultMaxResults);
    Integer start = nextPageToken
        .map(s -> Integer.valueOf(encoder.decodePageToken(s.value)))
        .orElse(0);
    var resAndSize = storageManager.listSchemas(share, start, finalMaxResults);
    int end = start + finalMaxResults;

    return resAndSize.thenApplyAsync(optPageContent -> optPageContent.map(pageContent -> {
      Optional<String> optionalToken =
          end < pageContent.size ? Optional.of(Integer.toString(end)) : Optional.empty();
      var content = pageContent.result.stream()
          .map(DeltaSharesServiceImpl::pSchemaToSchema)
          .toList();
      return optionalToken
          .map(encoder::encodePageToken)
          .map(t -> ContentAndToken.of(content, t))
          .orElse(ContentAndToken.withoutToken(content));
    }));
  }

  @Override
  public CompletionStage<Optional<ContentAndToken<List<Table>>>> listTables(
      String share,
      String schema,
      Optional<ContentAndToken.Token> nextPageToken,
      Optional<Integer> maxResults) {
    Integer finalMaxResults = maxResults.orElse(defaultMaxResults);
    Integer start = nextPageToken
        .map(s -> Integer.valueOf(encoder.decodePageToken(s.value)))
        .orElse(0);
    var resAndSize = storageManager.listTables(share, schema, start, finalMaxResults);
    int end = start + finalMaxResults;
    return resAndSize.thenApplyAsync(optPageContent -> optPageContent.map(pageContent -> {
      Optional<String> optionalToken =
          end < pageContent.size ? Optional.of(Integer.toString(end)) : Optional.empty();
      var content =
          pageContent.result.stream().map(DeltaSharesServiceImpl::pTableToTable).toList();
      return optionalToken
          .map(encoder::encodePageToken)
          .map(t -> ContentAndToken.of(content, t))
          .orElse(ContentAndToken.withoutToken(content));
    }));
  }

  @Override
  public CompletionStage<Optional<ContentAndToken<List<Table>>>> listTablesOfShare(
      String share, Optional<ContentAndToken.Token> nextPageToken, Optional<Integer> maxResults) {
    Integer finalMaxResults = maxResults.orElse(defaultMaxResults);
    Integer start = nextPageToken
        .map(s -> Integer.valueOf(encoder.decodePageToken(s.value)))
        .orElse(0);
    var resAndSize = storageManager.listTablesOfShare(share, start, finalMaxResults);
    int end = start + finalMaxResults;
    return resAndSize.thenApplyAsync(optPageContent -> optPageContent.map(pageContent -> {
      Optional<String> optionalToken =
          end < pageContent.size ? Optional.of(Integer.toString(end)) : Optional.empty();
      var content =
          pageContent.result.stream().map(DeltaSharesServiceImpl::pTableToTable).toList();
      return optionalToken
          .map(encoder::encodePageToken)
          .map(t -> ContentAndToken.of(content, t))
          .orElse(ContentAndToken.withoutToken(content));
    }));
  }
}
