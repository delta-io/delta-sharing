package io.whitefox.api.deltasharing.server;

import io.quarkus.runtime.util.ExceptionUtil;
import io.whitefox.api.deltasharing.model.*;
import io.whitefox.services.ContentAndToken;
import io.whitefox.services.DeltaSharesService;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

public class DeltaSharesApiImpl implements DeltaApiApi {

  private final DeltaSharesService deltaSharesService;
  private static final String DELTA_TABLE_VERSION_HEADER = "Delta-Table-Version";
  private static final Function<Throwable, Response> exceptionToResponse =
      t -> Response.status(Response.Status.BAD_GATEWAY)
          .entity(new CommonErrorResponse()
              .errorCode("BAD GATEWAY")
              .message(ExceptionUtil.generateStackTrace(t)))
          .build();

  private Response wrapExceptions(Supplier<Response> f, Function<Throwable, Response> mapper) {
    try {
      return f.get();
    } catch (Throwable t) {
      return mapper.apply(t);
    }
  }

  private Response notFoundResponse() {
    return Response.status(Response.Status.NOT_FOUND)
        .entity(new CommonErrorResponse().errorCode("1").message("NOT FOUND"))
        .build();
  }

  private <T> Response optionalToNotFound(Optional<T> opt, Function<T, Response> fn) {
    return opt.map(fn).orElse(notFoundResponse());
  }

  @Inject
  public DeltaSharesApiImpl(DeltaSharesService deltaSharesService) {
    this.deltaSharesService = deltaSharesService;
  }

  @Override
  public Response getShare(String share) {
    return wrapExceptions(
        () -> optionalToNotFound(
            deltaSharesService.getShare(share), s -> Response.ok(s).build()),
        exceptionToResponse);
  }

  @Override
  public Response getTableChanges(
      String share,
      String schema,
      String table,
      String startingTimestamp,
      Integer startingVersion,
      Integer endingVersion,
      String endingTimestamp,
      Boolean includeHistoricalMetadata) {
    return Response.ok().build();
  }

  @Override
  public Response getTableMetadata(
      String share, String schema, String table, String startingTimestamp) {
    return Response.ok().build();
  }

  @Override
  public Response getTableVersion(
      String share, String schema, String table, String startingTimestamp) {

    return wrapExceptions(
        () -> optionalToNotFound(
            deltaSharesService.getTableVersion(share, schema, table, startingTimestamp),
            t -> Response.ok().header(DELTA_TABLE_VERSION_HEADER, t).build()),
        exceptionToResponse);
  }

  @Override
  public Response listALLTables(String share, Integer maxResults, String pageToken) {
    return wrapExceptions(
        () -> optionalToNotFound(
            deltaSharesService.listTablesOfShare(
                share,
                Optional.ofNullable(pageToken).map(ContentAndToken.Token::new),
                Optional.ofNullable(maxResults)),
            c -> Response.ok(c.getToken()
                    .map(t -> new ListTablesResponse().items(c.getContent()).nextPageToken(t))
                    .orElse(new ListTablesResponse().items(c.getContent())))
                .build()),
        exceptionToResponse);
  }

  @Override
  public Response listSchemas(String share, Integer maxResults, String pageToken) {
    return wrapExceptions(
        () -> optionalToNotFound(
            deltaSharesService
                .listSchemas(
                    share,
                    Optional.ofNullable(pageToken).map(ContentAndToken.Token::new),
                    Optional.ofNullable(maxResults))
                .map(ct -> ct.getToken()
                    .map(t -> new ListSchemasResponse().nextPageToken(t).items(ct.getContent()))
                    .orElse(new ListSchemasResponse().items(ct.getContent()))),
            l -> Response.ok(l).build()),
        exceptionToResponse);
  }

  @Override
  public Response listShares(Integer maxResults, String pageToken) {
    return wrapExceptions(
        () -> {
          var c = deltaSharesService.listShares(
              Optional.ofNullable(pageToken).map(ContentAndToken.Token::new),
              Optional.ofNullable(maxResults));
          return Response.ok(c.getToken()
                  .map(t -> new ListShareResponse().items(c.getContent()).nextPageToken(t))
                  .orElse(new ListShareResponse().items(c.getContent())))
              .build();
        },
        exceptionToResponse);
  }

  @Override
  public Response listTables(String share, String schema, Integer maxResults, String pageToken) {
    return wrapExceptions(
        () -> optionalToNotFound(
            deltaSharesService.listTables(
                share,
                schema,
                Optional.ofNullable(pageToken).map(ContentAndToken.Token::new),
                Optional.ofNullable(maxResults)),
            c -> Response.ok(c.getToken()
                    .map(t -> new ListTablesResponse().items(c.getContent()).nextPageToken(t))
                    .orElse(new ListTablesResponse().items(c.getContent())))
                .build()),
        exceptionToResponse);
  }

  @Override
  public Response queryTable(
      String share,
      String schema,
      String table,
      QueryRequest queryRequest,
      String startingTimestamp) {
    return Response.ok().build();
  }
}
