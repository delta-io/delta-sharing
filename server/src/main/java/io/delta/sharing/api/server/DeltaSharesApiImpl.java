package io.delta.sharing.api.server;

import io.delta.sharing.api.server.model.QueryRequest;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.math.BigDecimal;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.eclipse.microprofile.config.inject.ConfigProperty;

public class DeltaSharesApiImpl implements SharesApi {

  private final DeltaSharesService deltaSharesService;

  private final ExecutorService executorService;

  @Inject
  public DeltaSharesApiImpl(
      DeltaSharesService deltaSharesService,
      @ConfigProperty(name = "delta.sharing.api.server.nThreads") int nThreads) {
    this.deltaSharesService = deltaSharesService;
    this.executorService = Executors.newFixedThreadPool(nThreads);
  }

  @Override
  public CompletionStage<Response> getShare(String share) {
    return CompletableFuture.supplyAsync(
        () ->
            deltaSharesService
                .getShare(share)
                .map(s -> Response.ok(s).build())
                .orElse(Response.status(Response.Status.NOT_FOUND).build()),
        executorService);
  }

  @Override
  public CompletionStage<Response> getTableChanges(
      String share,
      String schema,
      String table,
      String startingTimestamp,
      Integer startingVersion,
      Integer endingVersion,
      String endingTimestamp,
      Boolean includeHistoricalMetadata) {
    Response res = Response.ok().build();
    return CompletableFuture.completedFuture(res);
  }

  @Override
  public CompletionStage<Response> getTableMetadata(
      String share, String schema, String table, String startingTimestamp) {
    Response res = Response.ok().build();
    return CompletableFuture.completedFuture(res);
  }

  @Override
  public CompletionStage<Response> getTableVersion(
      String share, String schema, String table, String startingTimestamp) {
    Response res = Response.ok().build();
    return CompletableFuture.completedFuture(res);
  }

  @Override
  public CompletionStage<Response> listALLTables(
      String share, BigDecimal maxResults, String pageToken) {
    Response res = Response.ok().build();
    return CompletableFuture.completedFuture(res);
  }

  @Override
  public CompletionStage<Response> listSchemas(
      String share, BigDecimal maxResults, String pageToken) {
    Response res = Response.ok().build();
    return CompletableFuture.completedFuture(res);
  }

  @Override
  public CompletionStage<Response> listShares(BigDecimal maxResults, String pageToken) {
    Response res = Response.ok().build();
    return CompletableFuture.completedFuture(res);
  }

  @Override
  public CompletionStage<Response> listTables(
      String share, String schema, BigDecimal maxResults, String pageToken) {
    Response res = Response.ok().build();
    return CompletableFuture.completedFuture(res);
  }

  @Override
  public CompletionStage<Response> queryTable(
      String share,
      String schema,
      String table,
      QueryRequest queryRequest,
      String startingTimestamp) {
    Response res = Response.ok().build();
    return CompletableFuture.completedFuture(res);
  }
}
