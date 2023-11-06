package io.whitefox.api.server;

import io.whitefox.api.model.v1.generated.CreateTableInput;
import io.whitefox.api.model.v1.generated.PatchTableInput;
import io.whitefox.api.server.v1.generated.TableV1Api;
import io.whitefox.core.services.TableService;
import jakarta.ws.rs.core.Response;

public class TableV1ApiImpl implements TableV1Api, ApiUtils {

  private final TableService tableService;

  public TableV1ApiImpl(TableService tableService) {
    this.tableService = tableService;
  }

  @Override
  public Response createTableInProvider(String providerName, CreateTableInput createTableInput) {
    return wrapExceptions(
        () -> Response.status(Response.Status.CREATED)
            .entity(WhitefoxMappers.internalTable2api(tableService.createInternalTable(
                providerName,
                getRequestPrincipal(),
                WhitefoxMappers.api2createInternalTable(createTableInput))))
            .build(),
        exceptionToResponse);
  }

  @Override
  public Response deleteTableInProvider(String providerName, String tableName, String force) {
    return Response.status(501).build();
  }

  @Override
  public Response describeTableInProvider(String providerName, String tableName) {
    return wrapExceptions(
        () -> optionalToNotFound(
            tableService
                .getInternalTable(providerName, tableName)
                .map(WhitefoxMappers::internalTable2api),
            t -> Response.ok(t).build()),
        exceptionToResponse);
  }

  @Override
  public Response listTablesInProvider(String providerName) {
    return Response.status(501).build();
  }

  @Override
  public Response patchTableInProvider(
      String providerName, String tableName, PatchTableInput patchTableInput) {
    return Response.status(501).build();
  }

  @Override
  public Response validateTable(String providerName, String tableName) {
    return Response.status(501).build();
  }
}
