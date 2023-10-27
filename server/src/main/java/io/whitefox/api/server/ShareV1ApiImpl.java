package io.whitefox.api.server;

import io.whitefox.api.model.v1.generated.AddRecipientToShareRequest;
import io.whitefox.api.model.v1.generated.AddTableToSchemaRequest;
import io.whitefox.api.model.v1.generated.CreateShareInput;
import io.whitefox.api.server.v1.generated.ShareV1Api;
import io.whitefox.core.*;
import io.whitefox.core.services.ShareService;
import jakarta.ws.rs.core.Response;

public class ShareV1ApiImpl implements ShareV1Api, ApiUtils {

  private final ShareService shareService;

  public ShareV1ApiImpl(ShareService shareService) {
    this.shareService = shareService;
  }

  @Override
  public Response addRecipientToShare(
      String share, AddRecipientToShareRequest addRecipientToShareRequest) {
    return wrapExceptions(
        () -> Response.ok(WhitefoxMappers.share2api(shareService.addRecipientsToShare(
                share,
                addRecipientToShareRequest.getPrincipals(),
                this::resolvePrincipal,
                this.getRequestPrincipal())))
            .build(),
        exceptionToResponse);
  }

  @Override
  public Response addTableToSchema(
      String share, String schema, AddTableToSchemaRequest addTableToSchemaRequest) {
    return wrapExceptions(
        () -> Response.status(Response.Status.CREATED)
            .entity(WhitefoxMappers.share2api(shareService.addTableToSchema(
                new ShareName(share),
                new SchemaName(schema),
                new SharedTableName(addTableToSchemaRequest.getName()),
                new ProviderName(addTableToSchemaRequest.getReference().getProviderName()),
                new InternalTableName(addTableToSchemaRequest.getReference().getName()),
                getRequestPrincipal())))
            .build(),
        exceptionToResponse);
  }

  @Override
  public Response createSchema(String share, String schema) {
    return wrapExceptions(
        () -> Response.status(Response.Status.CREATED)
            .entity(WhitefoxMappers.share2api(
                shareService.createSchema(share, schema, this.getRequestPrincipal())))
            .build(),
        exceptionToResponse);
  }

  @Override
  public Response createShare(CreateShareInput createShareInput) {
    return wrapExceptions(
        () -> Response.status(Response.Status.CREATED)
            .entity(WhitefoxMappers.share2api(shareService.createShare(
                WhitefoxMappers.api2createShare(createShareInput, this::resolvePrincipal),
                this.getRequestPrincipal())))
            .build(),
        exceptionToResponse);
  }

  @Override
  public Response deleteSchema(String share, String schema) {
    return Response.status(501).build();
  }

  @Override
  public Response deleteShare(String share) {
    return Response.status(501).build();
  }

  @Override
  public Response deleteTableFromSchema(String share, String schema, String table) {
    return Response.status(501).build();
  }

  @Override
  public Response listTablesInSchema(String share, String schema) {
    return Response.status(501).build();
  }

  @Override
  public Response updateShare(String share, CreateShareInput createShareInput) {
    return Response.status(501).build();
  }
}
