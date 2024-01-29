package io.whitefox.core.services;

import io.whitefox.core.*;
import io.whitefox.core.services.exceptions.TableNotFound;
import io.whitefox.persistence.StorageManager;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.sql.Timestamp;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class DeltaSharesServiceImpl implements DeltaSharesService {

  private final StorageManager storageManager;
  private final Integer defaultMaxResults;
  private final TableLoaderFactory tableLoaderFactory;
  private final FileSignerFactory fileSignerFactory;

  @Inject
  public DeltaSharesServiceImpl(
      StorageManager storageManager,
      @ConfigProperty(name = "io.delta.sharing.api.server.defaultMaxResults")
          Integer defaultMaxResults,
      TableLoaderFactory tableLoaderFactory,
      FileSignerFactory signerFactory) {
    this.storageManager = storageManager;
    this.defaultMaxResults = defaultMaxResults;
    this.tableLoaderFactory = tableLoaderFactory;
    this.fileSignerFactory = signerFactory;
  }

  @Override
  public Optional<Long> getTableVersion(
      String share, String schema, String table, Optional<Timestamp> startingTimestamp) {
    return storageManager
        .getSharedTable(share, schema, table)
        .map(t -> tableLoaderFactory
            .newTableLoader(t.internalTable())
            .loadTable(t)
            .getTableVersion(startingTimestamp))
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
      String share, String schema, String table, Optional<Timestamp> startingTimestamp) {
    return storageManager.getSharedTable(share, schema, table).flatMap(t -> tableLoaderFactory
        .newTableLoader(t.internalTable())
        .loadTable(t)
        .getMetadata(startingTimestamp));
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
  public Optional<ContentAndToken<List<SharedTable>>> listTables(
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
  public Optional<ContentAndToken<List<SharedTable>>> listTablesOfShare(
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

  @Override
  public ReadTableResult queryTable(
      String share, String schema, String tableName, ReadTableRequest queryRequest) {
    SharedTable sharedTable = storageManager
        .getSharedTable(share, schema, tableName)
        .orElseThrow(() -> new TableNotFound(String.format(
            "Table %s not found in share %s with schema %s", tableName, share, schema)));

    var fileSigner =
        fileSignerFactory.newFileSigner(sharedTable.internalTable().provider().storage());
    var readTableResultToBeSigned = tableLoaderFactory
        .newTableLoader(sharedTable.internalTable())
        .loadTable(sharedTable)
        .queryTable(queryRequest);
    return new ReadTableResult(
        readTableResultToBeSigned.protocol(),
        readTableResultToBeSigned.metadata(),
        readTableResultToBeSigned.other().stream()
            .map(fileSigner::sign)
            .collect(Collectors.toList()),
        readTableResultToBeSigned.version());
  }
}
