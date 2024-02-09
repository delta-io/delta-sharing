package io.whitefox.api.deltasharing;

import io.whitefox.api.deltasharing.model.v1.Format;
import io.whitefox.api.deltasharing.model.v1.TableMetadataResponse;
import io.whitefox.api.deltasharing.model.v1.TableQueryResponse;
import io.whitefox.api.deltasharing.model.v1.generated.*;
import io.whitefox.api.deltasharing.model.v1.parquet.ParquetFile;
import io.whitefox.api.deltasharing.model.v1.parquet.ParquetMetadata;
import io.whitefox.api.deltasharing.model.v1.parquet.ParquetProtocol;
import io.whitefox.api.server.CommonMappers;
import io.whitefox.core.*;
import io.whitefox.core.Schema;
import io.whitefox.core.Share;
import io.whitefox.core.services.capabilities.ResponseFormat;
import java.util.*;
import java.util.stream.Collectors;

public class DeltaMappers {

  public static io.whitefox.api.deltasharing.model.v1.generated.Share share2api(Share p) {
    return new io.whitefox.api.deltasharing.model.v1.generated.Share()
        .id(p.id())
        .name(p.name());
  }

  public static io.whitefox.api.deltasharing.model.v1.generated.Schema schema2api(Schema schema) {
    return new io.whitefox.api.deltasharing.model.v1.generated.Schema()
        .name(schema.name())
        .share(schema.share());
  }

  public static ReadTableRequest api2ReadTableRequest(QueryRequest request) {
    if (request.getStartingVersion() != null && request.getEndingVersion() != null) {
      throw new IllegalArgumentException("The startingVersion and endingVersion are not supported");
    } else if (request.getStartingVersion() != null) {
      throw new IllegalArgumentException("The startingVersion is not supported");
    } else if (request.getEndingVersion() != null) {
      throw new IllegalArgumentException("The endingVersion is not supported");
    } else if (request.getVersion() != null && request.getVersion() < 0) {
      throw new IllegalArgumentException("version cannot be negative.");
    } else if (request.getVersion() != null && request.getTimestamp() == null) {
      return new ReadTableRequest.ReadTableVersion(
          Optional.ofNullable(request.getPredicateHints()),
          Optional.ofNullable(request.getJsonPredicateHints()),
          Optional.ofNullable(request.getLimitHint()),
          request.getVersion());
    } else if (request.getVersion() == null && request.getTimestamp() != null) {
      return new ReadTableRequest.ReadTableAsOfTimestamp(
          Optional.ofNullable(request.getPredicateHints()),
          Optional.ofNullable(request.getJsonPredicateHints()),
          Optional.ofNullable(request.getLimitHint()),
          CommonMappers.parseTimestamp(request.getTimestamp()));
    } else if (request.getVersion() == null && request.getTimestamp() == null) {
      return new ReadTableRequest.ReadTableCurrentVersion(
          Optional.ofNullable(request.getPredicateHints()),
          Optional.ofNullable(request.getJsonPredicateHints()),
          Optional.ofNullable(request.getLimitHint()));
    } else {
      throw new IllegalArgumentException("Cannot specify both version and timestamp");
    }
  }

  public static TableQueryResponse readTableResult2api(ReadTableResult readTableResult) {
    return new TableQueryResponse(
        protocol2Api(readTableResult.protocol()),
        metadata2Api(readTableResult.metadata()),
        readTableResult.files().stream().map(DeltaMappers::file2Api).collect(Collectors.toList()));
  }

  private static ParquetMetadata metadata2Api(Metadata metadata) {
    switch (metadata.format()) {
      case parquet:
        return ParquetMetadata.builder()
            .metadata(ParquetMetadata.Metadata.builder()
                .id(metadata.id())
                .name(metadata.name())
                .description(metadata.description())
                .format(new Format())
                .schemaString(metadata.tableSchema().structType().toJson())
                .partitionColumns(metadata.partitionColumns())
                .configuration(Optional.ofNullable(metadata.configuration()))
                .version(metadata.version())
                .numFiles(metadata.numFiles())
                .build())
            .build();
      case delta:
        throw new IllegalArgumentException("Delta response format is not supported");
      default:
        throw new IllegalArgumentException(
            String.format("%s response format is not supported", metadata.format()));
    }
  }

  private static ParquetProtocol protocol2Api(Protocol protocol) {
    return ParquetProtocol.ofMinReaderVersion(protocol.minReaderVersion().orElse(1));
  }

  private static ParquetFile file2Api(TableFile f) {
    return ParquetFile.builder()
        .file(ParquetFile.File.builder()
            .id(f.id())
            .url(f.url())
            .partitionValues(f.partitionValues())
            .size(f.size())
            .stats(f.stats())
            .version(f.version())
            .timestamp(f.timestamp())
            .expirationTimestamp(Optional.of(f.expirationTimestamp()))
            .build())
        .build();
  }

  public static TableReferenceAndReadRequest api2TableReferenceAndReadRequest(
      QueryRequest request, String share, String schema, String table) {
    return new TableReferenceAndReadRequest(share, schema, table, api2ReadTableRequest(request));
  }

  public static io.whitefox.api.deltasharing.model.v1.generated.Table table2api(
      SharedTable sharedTable) {
    return new io.whitefox.api.deltasharing.model.v1.generated.Table()
        .name(sharedTable.name())
        .share(sharedTable.share())
        .schema(sharedTable.schema());
  }

  /**
   * Serializes the response format in its text-based representation
   */
  public static String toResponseFormatHeader(ResponseFormat responseFormat) {
    return responseFormat.stringRepresentation();
  }

  public static TableMetadataResponse toTableResponseMetadata(Metadata m) {
    return new TableMetadataResponse(
        ParquetProtocol.ofMinReaderVersion(1), // smell
        metadata2Api(m));
  }
}
