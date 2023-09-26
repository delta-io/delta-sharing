package io.whitefox.api.server;

import io.whitefox.api.model.AddRecipientToShareRequest;
import io.whitefox.api.model.CreateShareInput;
import io.whitefox.api.model.TableReference;
import jakarta.ws.rs.core.Response;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class SharesApiImpl implements SharesApi {

  @Override
  public CompletionStage<Response> addRecipientToShare(
      String share, AddRecipientToShareRequest addRecipientToShareRequest) {
    Response res = Response.ok().build();
    return CompletableFuture.completedFuture(res);
  }

  @Override
  public CompletionStage<Response> addTableToSchema(
      String share, String schema, TableReference tableReference) {
    Response res = Response.ok().build();
    return CompletableFuture.completedFuture(res);
  }

  @Override
  public CompletionStage<Response> createSchema(String share, String schema) {
    Response res = Response.ok().build();
    return CompletableFuture.completedFuture(res);
  }

  @Override
  public CompletionStage<Response> createShare(CreateShareInput createShareInput) {
    Response res = Response.ok().build();
    return CompletableFuture.completedFuture(res);
  }

  @Override
  public CompletionStage<Response> deleteSchema(String share, String schema) {
    Response res = Response.ok().build();
    return CompletableFuture.completedFuture(res);
  }

  @Override
  public CompletionStage<Response> deleteShare(String share) {
    Response res = Response.ok().build();
    return CompletableFuture.completedFuture(res);
  }

  @Override
  public CompletionStage<Response> deleteTableFromSchema(
      String share, String schema, String table) {
    Response res = Response.ok().build();
    return CompletableFuture.completedFuture(res);
  }

  @Override
  public CompletionStage<Response> listTablesInSchema(String share, String schema) {
    Response res = Response.ok().build();
    return CompletableFuture.completedFuture(res);
  }

  @Override
  public CompletionStage<Response> updateShare(String share, CreateShareInput createShareInput) {
    Response res = Response.ok().build();
    return CompletableFuture.completedFuture(res);
  }
}
