package io.whitefox.api.deltasharing.server;

import static io.whitefox.api.server.CommonMappers.mapList;

import io.whitefox.api.deltasharing.DeltaMappers;
import io.whitefox.api.deltasharing.encoders.DeltaPageTokenEncoder;
import io.whitefox.api.deltasharing.model.v1.generated.ListSchemasResponse;
import io.whitefox.api.deltasharing.model.v1.generated.ListShareResponse;
import io.whitefox.api.deltasharing.model.v1.generated.ListTablesResponse;
import io.whitefox.api.deltasharing.model.v1.generated.QueryRequest;
import io.whitefox.api.deltasharing.serializers.TableMetadataSerializer;
import io.whitefox.api.deltasharing.serializers.TableQueryResponseSerializer;
import io.whitefox.api.deltasharing.server.v1.generated.DeltaApiApi;
import io.whitefox.api.server.ApiUtils;
import io.whitefox.core.services.ContentAndToken;
import io.whitefox.core.services.DeltaSharesService;
import io.whitefox.core.services.ShareService;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.Optional;

public class DeltaSharesApiImpl implements DeltaApiApi, ApiUtils {

  private final MediaType ndjsonMediaType = new MediaType("application", "x-ndjson");
  private final DeltaSharesService deltaSharesService;
  private final ShareService shareService;
  private final DeltaPageTokenEncoder tokenEncoder;
  private final TableMetadataSerializer tableResponseSerializer;
  private final TableQueryResponseSerializer tableQueryResponseSerializer;

  @Inject
  public DeltaSharesApiImpl(
      DeltaSharesService deltaSharesService,
      ShareService shareService,
      DeltaPageTokenEncoder encoder,
      TableMetadataSerializer tableResponseSerializer,
      TableQueryResponseSerializer tableQueryResponseSerializer) {
    this.deltaSharesService = deltaSharesService;
    this.tokenEncoder = encoder;
    this.tableResponseSerializer = tableResponseSerializer;
    this.tableQueryResponseSerializer = tableQueryResponseSerializer;
    this.shareService = shareService;
  }

  @Override
  public Response getShare(String share) {
    return wrapExceptions(
        () ->
            optionalToNotFound(shareService.getShare(share), s -> Response.ok(s).build()),
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
                        tableResponseSerializer.serialize(DeltaMappers.toTableResponseMetadata(m)),
                        ndjsonMediaType)
                    .status(Response.Status.OK.getStatusCode())
                    .header(DELTA_TABLE_VERSION_HEADER, String.valueOf(v))
                    .header(
                        DELTA_SHARE_CAPABILITIES_HEADER,
                        getResponseFormatHeader(
                            DeltaMappers.toHeaderCapabilitiesMap(deltaSharingCapabilities)))
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
                        .items(mapList(c.getContent(), DeltaMappers::table2api))
                        .nextPageToken(tokenEncoder.encodePageToken(t)))
                    .orElse(new ListTablesResponse()
                        .items(mapList(c.getContent(), DeltaMappers::table2api))))
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
                        .items(mapList(ct.getContent(), DeltaMappers::schema2api)))
                    .orElse(new ListSchemasResponse()
                        .items(mapList(ct.getContent(), DeltaMappers::schema2api)))),
            l -> Response.ok(l).build()),
        exceptionToResponse);
  }

  @Override
  public Response listShares(Integer maxResults, String pageToken) {
    return wrapExceptions(
        () -> {
          var c =
              deltaSharesService.listShares(parseToken(pageToken), Optional.ofNullable(maxResults));
          var response =
              new ListShareResponse().items(mapList(c.getContent(), DeltaMappers::share2api));
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
                        .items(mapList(c.getContent(), DeltaMappers::table2api))
                        .nextPageToken(tokenEncoder.encodePageToken(t)))
                    .orElse(new ListTablesResponse()
                        .items(mapList(c.getContent(), DeltaMappers::table2api))))
                .build()),
        exceptionToResponse);
  }

  /**
   * <pre>
   * {"protocol":{"minReaderVersion":1}}
   * {"metaData":{"id":"f8d5c169-3d01-4ca3-ad9e-7dc3355aedb2","format":{"provider":"parquet"},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"eventTime\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},{\"name\":\"date\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":["date"]}}
   * {"file":{"url":"https://<s3-bucket-name>.s3.us-west-2.amazonaws.com/delta-exchange-test/table2/date%3D2021-04-28/part-00000-8b0086f2-7b27-4935-ac5a-8ed6215a6640.c000.snappy.parquet?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20210501T010516Z&X-Amz-SignedHeaders=host&X-Amz-Expires=900&X-Amz-Credential=AKIAISZRDL4Q4Q7AIONA%2F20210501%2Fus-west-2%2Fs3%2Faws4_request&X-Amz-Signature=97b6762cfd8e4d7e94b9d707eff3faf266974f6e7030095c1d4a66350cfd892e","id":"8b0086f2-7b27-4935-ac5a-8ed6215a6640","partitionValues":{"date":"2021-04-28"},"size":573,"stats":"{\"numRecords\":1,\"minValues\":{\"eventTime\":\"2021-04-28T23:33:57.955Z\"},\"maxValues\":{\"eventTime\":\"2021-04-28T23:33:57.955Z\"},\"nullCount\":{\"eventTime\":0}}"}}
   * {"file":{"url":"https://<s3-bucket-name>.s3.us-west-2.amazonaws.com/delta-exchange-test/table2/date%3D2021-04-28/part-00000-591723a8-6a27-4240-a90e-57426f4736d2.c000.snappy.parquet?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20210501T010516Z&X-Amz-SignedHeaders=host&X-Amz-Expires=899&X-Amz-Credential=AKIAISZRDL4Q4Q7AIONA%2F20210501%2Fus-west-2%2Fs3%2Faws4_request&X-Amz-Signature=0f7acecba5df7652457164533a58004936586186c56425d9d53c52db574f6b62","id":"591723a8-6a27-4240-a90e-57426f4736d2","partitionValues":{"date":"2021-04-28"},"size":573,"stats":"{\"numRecords\":1,\"minValues\":{\"eventTime\":\"2021-04-28T23:33:48.719Z\"},\"maxValues\":{\"eventTime\":\"2021-04-28T23:33:48.719Z\"},\"nullCount\":{\"eventTime\":0}}"}}
   * </pre>
   */
  @Override
  public Response queryTable(
      String share,
      String schema,
      String table,
      QueryRequest queryRequest,
      String deltaSharingCapabilities) {
    return wrapExceptions(
        () -> {
          var readResult = deltaSharesService.queryTable(
              share, schema, table, DeltaMappers.api2ReadTableRequest(queryRequest));
          var serializedReadResult =
              tableQueryResponseSerializer.serialize(DeltaMappers.readTableResult2api(readResult));
          return Response.ok(serializedReadResult, ndjsonMediaType)
              .header(DELTA_TABLE_VERSION_HEADER, readResult.version())
              .header(
                  DELTA_SHARE_CAPABILITIES_HEADER,
                  getResponseFormatHeader(
                      DeltaMappers.toHeaderCapabilitiesMap(deltaSharingCapabilities)))
              .build();
        },
        exceptionToResponse);
  }

  private Optional<ContentAndToken.Token> parseToken(String t) {
    return Optional.ofNullable(t).map(tokenEncoder::decodePageToken);
  }
}
