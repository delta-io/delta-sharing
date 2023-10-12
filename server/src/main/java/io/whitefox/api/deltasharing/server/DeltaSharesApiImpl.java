package io.whitefox.api.deltasharing.server;

import static io.whitefox.api.deltasharing.Mappers.mapList;

import io.quarkus.runtime.util.ExceptionUtil;
import io.whitefox.api.deltasharing.Mappers;
import io.whitefox.api.deltasharing.encoders.DeltaPageTokenEncoder;
import io.whitefox.api.deltasharing.model.generated.*;
import io.whitefox.api.deltasharing.server.generated.DeltaApiApi;
import io.whitefox.core.services.ContentAndToken;
import io.whitefox.core.services.DeltaSharesService;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

public class DeltaSharesApiImpl implements DeltaApiApi {
  private final DeltaSharesService deltaSharesService;
  private final DeltaPageTokenEncoder tokenEncoder;

  @Inject
  public DeltaSharesApiImpl(DeltaSharesService deltaSharesService, DeltaPageTokenEncoder encoder) {
    this.deltaSharesService = deltaSharesService;
    this.tokenEncoder = encoder;
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
                share, parseToken(pageToken), Optional.ofNullable(maxResults)),
            c -> Response.ok(c.getToken()
                    .map(t -> new ListTablesResponse()
                        .items(mapList(c.getContent(), Mappers::table2api))
                        .nextPageToken(tokenEncoder.encodePageToken(t)))
                    .orElse(new ListTablesResponse()
                        .items(mapList(c.getContent(), Mappers::table2api))))
                .build()),
        exceptionToResponse);
  }

  @Override
  public Response listSchemas(String share, Integer maxResults, String pageToken) {
    return wrapExceptions(
        () -> optionalToNotFound(
            deltaSharesService
                .listSchemas(share, parseToken(pageToken), Optional.ofNullable(maxResults))
                .map(ct -> ct.getToken()
                    .map(t -> new ListSchemasResponse()
                        .nextPageToken(tokenEncoder.encodePageToken(t))
                        .items(mapList(ct.getContent(), Mappers::schema2api)))
                    .orElse(new ListSchemasResponse()
                        .items(mapList(ct.getContent(), Mappers::schema2api)))),
            l -> Response.ok(l).build()),
        exceptionToResponse);
  }

  @Override
  public Response listShares(Integer maxResults, String pageToken) {
    return wrapExceptions(
        () -> {
          var c =
              deltaSharesService.listShares(parseToken(pageToken), Optional.ofNullable(maxResults));
          var response = new ListShareResponse().items(mapList(c.getContent(), Mappers::share2api));
          return Response.ok(c.getToken()
                  .map(t -> response.nextPageToken(tokenEncoder.encodePageToken(t)))
                  .orElse(response))
              .build();
        },
        exceptionToResponse);
  }

  @Override
  public Response listTables(String share, String schema, Integer maxResults, String pageToken) {
    return wrapExceptions(
        () -> optionalToNotFound(
            deltaSharesService.listTables(
                share, schema, parseToken(pageToken), Optional.ofNullable(maxResults)),
            c -> Response.ok(c.getToken()
                    .map(t -> new ListTablesResponse()
                        .items(mapList(c.getContent(), Mappers::table2api))
                        .nextPageToken(tokenEncoder.encodePageToken(t)))
                    .orElse(new ListTablesResponse()
                        .items(mapList(c.getContent(), Mappers::table2api))))
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

  private Optional<ContentAndToken.Token> parseToken(String t) {
    return Optional.ofNullable(t).map(tokenEncoder::decodePageToken);
  }
}
