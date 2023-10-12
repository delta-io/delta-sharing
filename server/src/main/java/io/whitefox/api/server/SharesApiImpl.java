package io.whitefox.api.server;

import io.whitefox.api.model.generated.AddRecipientToShareRequest;
import io.whitefox.api.model.generated.CreateShareInput;
import io.whitefox.api.model.generated.TableReference;
import io.whitefox.api.server.generated.SharesApi;
import jakarta.ws.rs.core.Response;

public class SharesApiImpl implements SharesApi {

  @Override
  public Response addRecipientToShare(
      String share, AddRecipientToShareRequest addRecipientToShareRequest) {
    return Response.ok().build();
  }

  @Override
  public Response addTableToSchema(String share, String schema, TableReference tableReference) {
    return Response.ok().build();
  }

  @Override
  public Response createSchema(String share, String schema) {
    return Response.ok().build();
  }

  @Override
  public Response createShare(CreateShareInput createShareInput) {
    return Response.ok().build();
  }

  @Override
  public Response deleteSchema(String share, String schema) {
    return Response.ok().build();
  }

  @Override
  public Response deleteShare(String share) {
    return Response.ok().build();
  }

  @Override
  public Response deleteTableFromSchema(String share, String schema, String table) {
    return Response.ok().build();
  }

  @Override
  public Response listTablesInSchema(String share, String schema) {
    return Response.ok().build();
  }

  @Override
  public Response updateShare(String share, CreateShareInput createShareInput) {
    return Response.ok().build();
  }
}
