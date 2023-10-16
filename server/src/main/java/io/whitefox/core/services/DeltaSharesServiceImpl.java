package io.whitefox.core.services;

import io.delta.standalone.actions.Metadata;
import io.whitefox.core.Schema;
import io.whitefox.core.Share;
import io.whitefox.core.Table;
import io.whitefox.persistence.StorageManager;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class DeltaSharesServiceImpl implements DeltaSharesService {

  private final StorageManager storageManager;
  private final Integer defaultMaxResults;
  private final DeltaShareTableLoader tableLoader;

  @Inject
  public DeltaSharesServiceImpl(
      StorageManager storageManager,
      @ConfigProperty(name = "io.delta.sharing.api.server.defaultMaxResults")
          Integer defaultMaxResults,
      DeltaShareTableLoader tableLoader) {
    this.storageManager = storageManager;
    this.defaultMaxResults = defaultMaxResults;
    this.tableLoader = tableLoader;
  }

  @Override
  public Optional<Share> getShare(String share) {
    return storageManager.getShare(share);
  }

  @Override
  public Optional<Long> getTableVersion(
      String share, String schema, String table, String startingTimestamp) {
    return storageManager
        .getTable(share, schema, table)
        .map(t -> tableLoader.loadTable(t).getTableVersion(Optional.ofNullable(startingTimestamp)))
        .orElse(Optional.empty());
  }

  @Override
  public ContentAndToken<List<Share>> listShares(
      Optional<ContentAndToken.Token> nextPageToken, Optional<Integer> maxResults) {
    Integer finalMaxResults = maxResults.orElse(defaultMaxResults);
    Integer start = nextPageToken.map(ContentAndToken.Token::value).orElse(0);
    var pageContent = storageManager.getShares(start, finalMaxResults);
    int end = start + finalMaxResults;
    Optional<ContentAndToken.Token> optionalToken =
        end < pageContent.size() ? Optional.of(new ContentAndToken.Token(end)) : Optional.empty();
    var content = pageContent.result();
    return optionalToken
        .map(t -> ContentAndToken.of(content, t))
        .orElse(ContentAndToken.withoutToken(content));
  }

  @Override
  public Optional<Metadata> getTableMetadata(
      String share, String schema, String table, String startingTimestamp) {
    return storageManager
        .getTable(share, schema, table)
        .flatMap(t -> tableLoader.loadTable(t).getMetadata(Optional.ofNullable(startingTimestamp)));
  }

  @Override
  public Optional<ContentAndToken<List<Schema>>> listSchemas(
      String share, Optional<ContentAndToken.Token> nextPageToken, Optional<Integer> maxResults) {
    Integer finalMaxResults = maxResults.orElse(defaultMaxResults);
    Integer start = nextPageToken.map(ContentAndToken.Token::value).orElse(0);
    var optPageContent = storageManager.listSchemas(share, start, finalMaxResults);
    int end = start + finalMaxResults;

    return optPageContent.map(pageContent -> {
      Optional<ContentAndToken.Token> optionalToken =
          end < pageContent.size() ? Optional.of(new ContentAndToken.Token(end)) : Optional.empty();
      var content = pageContent.result();
      return optionalToken
          .map(t -> ContentAndToken.of(content, t))
          .orElse(ContentAndToken.withoutToken(content));
    });
  }

  @Override
  public Optional<ContentAndToken<List<Table>>> listTables(
      String share,
      String schema,
      Optional<ContentAndToken.Token> nextPageToken,
      Optional<Integer> maxResults) {
    Integer finalMaxResults = maxResults.orElse(defaultMaxResults);
    Integer start = nextPageToken.map(ContentAndToken.Token::value).orElse(0);
    var optPageContent = storageManager.listTables(share, schema, start, finalMaxResults);
    int end = start + finalMaxResults;
    return optPageContent.map(pageContent -> {
      Optional<ContentAndToken.Token> optionalToken =
          end < pageContent.size() ? Optional.of(new ContentAndToken.Token(end)) : Optional.empty();
      var content = pageContent.result();
      return optionalToken
          .map(t -> ContentAndToken.of(content, t))
          .orElse(ContentAndToken.withoutToken(content));
    });
  }

  @Override
  public Optional<ContentAndToken<List<Table>>> listTablesOfShare(
      String share, Optional<ContentAndToken.Token> nextPageToken, Optional<Integer> maxResults) {
    Integer finalMaxResults = maxResults.orElse(defaultMaxResults);
    Integer start = nextPageToken.map(ContentAndToken.Token::value).orElse(0);
    var optPageContent = storageManager.listTablesOfShare(share, start, finalMaxResults);
    int end = start + finalMaxResults;
    return optPageContent.map(pageContent -> {
      Optional<ContentAndToken.Token> optionalToken =
          end < pageContent.size() ? Optional.of(new ContentAndToken.Token(end)) : Optional.empty();
      return optionalToken
          .map(t -> ContentAndToken.of(pageContent.result(), t))
          .orElse(ContentAndToken.withoutToken(pageContent.result()));
    });
  }
}
