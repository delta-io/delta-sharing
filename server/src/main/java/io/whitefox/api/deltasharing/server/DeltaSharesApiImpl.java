package io.whitefox.api.deltasharing.server;

import static io.whitefox.api.deltasharing.Mappers.mapList;

import io.whitefox.api.deltasharing.Mappers;
import io.whitefox.api.deltasharing.encoders.DeltaPageTokenEncoder;
import io.whitefox.api.deltasharing.model.DeltaTableMetadata;
import io.whitefox.api.deltasharing.model.v1.generated.ListSchemasResponse;
import io.whitefox.api.deltasharing.model.v1.generated.ListShareResponse;
import io.whitefox.api.deltasharing.model.v1.generated.ListTablesResponse;
import io.whitefox.api.deltasharing.model.v1.generated.QueryRequest;
import io.whitefox.api.deltasharing.serializers.Serializer;
import io.whitefox.api.deltasharing.server.restdto.TableResponseMetadata;
import io.whitefox.api.deltasharing.server.v1.generated.DeltaApiApi;
import io.whitefox.api.server.ApiUtils;
import io.whitefox.core.services.ContentAndToken;
import io.whitefox.core.services.DeltaSharesService;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.Optional;

public class DeltaSharesApiImpl implements DeltaApiApi, ApiUtils {

  private final MediaType ndjsonMediaType = new MediaType("application", "x-ndjson");
  private final DeltaSharesService deltaSharesService;
  private final DeltaPageTokenEncoder tokenEncoder;
  private final Serializer<TableResponseMetadata> serializer;

  @Inject
  public DeltaSharesApiImpl(
      DeltaSharesService deltaSharesService,
      DeltaPageTokenEncoder encoder,
      Serializer<TableResponseMetadata> serializer) {
    this.deltaSharesService = deltaSharesService;
    this.tokenEncoder = encoder;
    this.serializer = serializer;
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
      String share,
      String schema,
      String table,
      String startingTimestamp,
      String deltaSharingCapabilities) {
    return wrapExceptions(
        () -> optionalToNotFound(
            deltaSharesService.getTableMetadata(share, schema, table, startingTimestamp),
            m -> optionalToNotFound(
                deltaSharesService.getTableVersion(share, schema, table, startingTimestamp),
                v -> Response.ok(
                        serializer.serialize(
                            Mappers.toTableResponseMetadata(new DeltaTableMetadata(v, m))),
                        ndjsonMediaType)
                    .status(Response.Status.OK.getStatusCode())
                    .header(DELTA_TABLE_VERSION_HEADER, String.valueOf(v))
                    .header(
                        DELTA_SHARE_CAPABILITIES_HEADER,
                        getResponseFormatHeader(
                            Mappers.toHeaderCapabilitiesMap(deltaSharingCapabilities)))
                    .build())),
        exceptionToResponse);
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

  private Optional<ContentAndToken.Token> parseToken(String t) {
    return Optional.ofNullable(t).map(tokenEncoder::decodePageToken);
  }
}
