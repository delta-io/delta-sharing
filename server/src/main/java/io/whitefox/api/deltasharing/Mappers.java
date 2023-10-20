package io.whitefox.api.deltasharing;

import io.whitefox.api.deltasharing.model.v1.generated.*;
import io.whitefox.core.*;
import io.whitefox.core.Schema;
import io.whitefox.core.Share;
import io.whitefox.core.Table;
import io.whitefox.core.storage.CreateStorage;
import io.whitefox.core.storage.Storage;
import io.whitefox.core.storage.StorageType;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.jboss.resteasy.reactive.common.NotImplementedYet;

public class Mappers {
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

  public static io.whitefox.api.deltasharing.model.v1.generated.Table table2api(Table table) {
    return new io.whitefox.api.deltasharing.model.v1.generated.Table()
        .name(table.name())
        .share(table.share())
        .schema(table.schema());
  }

  public static io.whitefox.api.model.v1.generated.Storage storage2api(Storage storage) {
    return new io.whitefox.api.model.v1.generated.Storage()
        .name(storage.name())
        .comment(storage.comment().orElse(null))
        .owner(storage.owner().name())
        .type(storage.type().value)
        .uri(storage.uri())
        .createdAt(storage.createdAt())
        .createdBy(storage.createdBy().name())
        .updatedAt(storage.updatedAt())
        .updatedBy(storage.updatedBy().name())
        .validatedAt(storage.validatedAt().orElse(null));
  }

  public static CreateStorage api2createStorage(
      io.whitefox.api.model.v1.generated.CreateStorage storage, Principal principal) {
    return new CreateStorage(
        storage.getName(),
        Optional.ofNullable(storage.getComment()),
        api2storageType(storage.getType()),
        principal,
        api2storageCredentials(storage.getType(), storage.getCredentials()),
        storage.getUri(),
        storage.getSkipValidation());
  }

  public static AwsCredentials api2storageCredentials(
      io.whitefox.api.model.v1.generated.CreateStorage.TypeEnum type,
      io.whitefox.api.model.v1.generated.StorageCredentials credentials) {
    switch (type) {
      case S3:
        return new AwsCredentials.SimpleAwsCredentials(
            credentials.getAwsAccessKeyId(),
            credentials.getAwsSecretAccessKey(),
            credentials.getRegion());
      default:
        throw new IllegalArgumentException("Unknown storage type " + type);
    }
  }

  public static io.whitefox.api.model.v1.generated.Metastore metastore2api(Metastore metastore) {
    return new io.whitefox.api.model.v1.generated.Metastore()
        .name(metastore.name())
        .comment(metastore.comment().orElse(null))
        .owner(metastore.owner().name())
        .type(metastore.type().value)
        .properties(metastoreProperties2api(metastore.properties()))
        .validatedAt(metastore.validatedAt().orElse(null))
        .createdAt(metastore.createdAt())
        .createdBy(metastore.createdBy().name())
        .updatedAt(metastore.updatedAt())
        .updatedBy(metastore.updatedBy().name());
  }

  private static io.whitefox.api.model.v1.generated.MetastoreProperties metastoreProperties2api(
      MetastoreProperties properties) {
    if (properties instanceof MetastoreProperties.GlueMetastoreProperties) {
      var glueMetastoreProperties = (MetastoreProperties.GlueMetastoreProperties) properties;
      return new io.whitefox.api.model.v1.generated.MetastoreProperties()
          .catalogId(glueMetastoreProperties.catalogId())
          .credentials(simpleAwsCredentials2api(glueMetastoreProperties.credentials()));
    }
    throw new IllegalArgumentException("Unknown type of metastore properties: " + properties);
  }

  private static io.whitefox.api.model.v1.generated.SimpleAwsCredentials simpleAwsCredentials2api(
      AwsCredentials credentials) {
    if (credentials instanceof AwsCredentials.SimpleAwsCredentials) {
      var simpleAwsCredentials = (AwsCredentials.SimpleAwsCredentials) credentials;
      return new io.whitefox.api.model.v1.generated.SimpleAwsCredentials()
          .awsAccessKeyId(simpleAwsCredentials.awsAccessKeyId())
          .awsSecretAccessKey(simpleAwsCredentials.awsSecretAccessKey())
          .region(simpleAwsCredentials.region());
    }
    throw new IllegalArgumentException("Unknown type of aws credentials: " + credentials);
  }

  public static CreateMetastore api2createMetastore(
      io.whitefox.api.model.v1.generated.CreateMetastore createMetastore, Principal principal) {
    var type = api2MetastoreType(createMetastore.getType());
    var res = new CreateMetastore(
        createMetastore.getName(),
        Optional.ofNullable(createMetastore.getComment()),
        type,
        api2CreateMetastoreProperties(createMetastore.getProperties(), type),
        principal,
        createMetastore.getSkipValidation());
    return res;
  }

  public static MetastoreProperties api2CreateMetastoreProperties(
      io.whitefox.api.model.v1.generated.MetastoreProperties createMetastore, MetastoreType type) {
    switch (type) {
      case GLUE:
        return new MetastoreProperties.GlueMetastoreProperties(
            createMetastore.getCatalogId(),
            api2awsCredentials(createMetastore.getCredentials()),
            type);
      default:
        throw new IllegalArgumentException("Unknown metastore type " + type);
    }
  }

  public static StorageType api2storageType(
      io.whitefox.api.model.v1.generated.CreateStorage.TypeEnum type) {
    switch (type) {
      case S3:
        return StorageType.S3;
      case GCS:
        return StorageType.GCS;
      case ABFS:
        return StorageType.ABFS;
      default:
        throw new IllegalArgumentException("Unknown storage type " + type.value());
    }
  }

  public static AwsCredentials api2awsCredentials(
      io.whitefox.api.model.v1.generated.SimpleAwsCredentials credentials) {
    return new AwsCredentials.SimpleAwsCredentials(
        credentials.getAwsAccessKeyId(),
        credentials.getAwsSecretAccessKey(),
        credentials.getRegion());
  }

  public static MetastoreType api2MetastoreType(
      io.whitefox.api.model.v1.generated.CreateMetastore.TypeEnum type) {
    switch (type) {
      case GLUE:
        return MetastoreType.GLUE;
      default:
        throw new IllegalArgumentException("Unknown metastore type " + type.value());
    }
  }

  public static TableMetadataResponseObject toTableResponseMetadata(Metadata m) {
    return new TableMetadataResponseObject()
        .protocol(new ProtocolObject().protocol(new ProtocolObjectProtocol().minReaderVersion(1)))
        .metadata(metadata2Api(m));
  }

  private static MetadataObject metadata2Api(Metadata metadata) {
    return new MetadataObject()
        .metadata(new MetadataObjectMetadata()
            .id(metadata.id())
            .name(metadata.name().orElse(null))
            .description(metadata.description().orElse(null))
            .format(new FormatObject().provider(metadata.format().provider()))
            .schemaString(metadata.tableSchema().structType().toJson())
            .partitionColumns(metadata.partitionColumns())
            ._configuration(metadata.configuration())
            .version(metadata.version().orElse(null))
            .numFiles(metadata.numFiles().orElse(null)));
  }

  /**
   * NOTE: this is an undocumented feature of the reference impl of delta-sharing, it's not part of the
   * protocol
   * ----
   * Return the [[io.whitefox.api.server.DeltaHeaders.DELTA_SHARE_CAPABILITIES_HEADER]] header
   * that will be set in the response w/r/t the one received in the request.
   * If the request did not contain any, we will return an empty one.
   */
  public static Map<String, String> toHeaderCapabilitiesMap(String headerCapabilities) {
    if (headerCapabilities == null) {
      return Map.of();
    }
    return Arrays.stream(headerCapabilities.toLowerCase().split(";"))
        .map(h -> h.split("="))
        .filter(h -> h.length == 2)
        .map(splits -> Map.entry(splits[0], splits[1]))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  public static ReadTableRequest api2ReadTableRequest(QueryRequest request) {
    if (request.getEndingVersion() != null || request.getStartingVersion() != null)
      throw new NotImplementedYet();
    if (request.getVersion() != null && request.getTimestamp() == null) {
      return new ReadTableRequest.ReadTableVersion(
          request.getPredicateHints(),
          Optional.ofNullable(request.getLimitHint()),
          request.getVersion());
    } else if (request.getVersion() == null && request.getTimestamp() != null) {
      return new ReadTableRequest.ReadTableAsOfTimestamp(
          request.getPredicateHints(),
          Optional.ofNullable(request.getLimitHint()),
          parse(request.getTimestamp()));
    } else if (request.getVersion() == null && request.getTimestamp() == null) {
      return new ReadTableRequest.ReadTableCurrentVersion(
          request.getPredicateHints(), Optional.ofNullable(request.getLimitHint()));
    } else {
      throw new IllegalArgumentException("Cannot specify both version and timestamp");
    }
  }

  public static TableQueryResponseObject readTableResult2api(ReadTableResult readTableResult) {
    return new TableQueryResponseObject()
        .metadata(metadata2Api(readTableResult.metadata()))
        .protocol(protocol2Api(readTableResult.protocol()))
        .files(
            readTableResult.files().stream().map(Mappers::file2Api).collect(Collectors.toList()));
  }

  private static FileObject file2Api(TableFile f) {
    return new FileObject()
        ._file(new FileObjectFile()
            .id(f.id())
            .url(f.url())
            .partitionValues(f.partitionValues())
            .size(f.size())
            .stats(f.stats().orElse(null))
            .version(f.version().orElse(null))
            .timestamp(f.timestamp().orElse(null))
            .expirationTimestamp(f.expirationTimestamp()));
  }

  private static ProtocolObject protocol2Api(Protocol protocol) {
    return new ProtocolObject()
        .protocol(new ProtocolObjectProtocol()
            .minReaderVersion(protocol.minReaderVersion().orElse(1)));
  }

  public static TableReferenceAndReadRequest api2TableReferenceAndReadRequest(
      QueryRequest request, String share, String schema, String table) {
    return new TableReferenceAndReadRequest(share, schema, table, api2ReadTableRequest(request));
  }

  public static <A, B> List<B> mapList(List<A> list, Function<A, B> f) {
    return list.stream().map(f).collect(Collectors.toList());
  }

  private static long parse(String ts) {
    return OffsetDateTime.parse(ts, java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME)
        .toInstant()
        .toEpochMilli();
  }
}
