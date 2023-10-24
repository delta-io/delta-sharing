package io.whitefox.api.server;

import io.whitefox.api.deltasharing.model.v1.generated.*;
import io.whitefox.api.model.v1.generated.*;
import io.whitefox.core.*;
import io.whitefox.core.Metastore;
import io.whitefox.core.MetastoreProperties;
import io.whitefox.core.Provider;
import io.whitefox.core.Share;
import io.whitefox.core.Storage;
import io.whitefox.core.StorageProperties;
import io.whitefox.core.StorageType;
import io.whitefox.core.actions.*;
import io.whitefox.core.actions.CreateMetastore;
import io.whitefox.core.actions.CreateStorage;
import java.util.ArrayList;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public class WhitefoxMappers {

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
        .properties(storageProperties2api(storage.properties()))
        .validatedAt(storage.validatedAt().orElse(null));
  }

  private static io.whitefox.api.model.v1.generated.StorageProperties storageProperties2api(
      StorageProperties properties) {
    if (properties instanceof StorageProperties.S3Properties) {
      var s3Properties = (StorageProperties.S3Properties) properties;
      return new io.whitefox.api.model.v1.generated.StorageProperties()
          .credentials(awsCredentials2api(s3Properties.credentials()));
    } else {
      throw new IllegalArgumentException("Unknown type of storage properties: " + properties);
    }
  }

  public static CreateStorage api2createStorage(
      io.whitefox.api.model.v1.generated.CreateStorage storage, Principal principal) {
    return new CreateStorage(
        storage.getName(),
        Optional.ofNullable(storage.getComment()),
        api2storageType(storage.getType()),
        principal,
        storage.getUri(),
        storage.getSkipValidation(),
        api2storageProperties(storage.getType(), storage.getProperties()));
  }

  private static StorageProperties api2storageProperties(
      io.whitefox.api.model.v1.generated.CreateStorage.TypeEnum type,
      io.whitefox.api.model.v1.generated.StorageProperties properties) {
    switch (type) {
      case S3:
        return new StorageProperties.S3Properties(api2credentials(properties.getCredentials()));
      default:
        throw new IllegalArgumentException("Unknown storage type " + type);
    }
  }

  public static AwsCredentials api2credentials(
      io.whitefox.api.model.v1.generated.SimpleAwsCredentials credentials) {
    return new AwsCredentials.SimpleAwsCredentials(
        credentials.getAwsAccessKeyId(),
        credentials.getAwsSecretAccessKey(),
        credentials.getRegion());
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
          .credentials(awsCredentials2api(glueMetastoreProperties.credentials()));
    }
    throw new IllegalArgumentException("Unknown type of metastore properties: " + properties);
  }

  private static io.whitefox.api.model.v1.generated.SimpleAwsCredentials awsCredentials2api(
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

  private static ProtocolObject protocol2Api(Protocol protocol) {
    return new ProtocolObject()
        .protocol(new ProtocolObjectProtocol()
            .minReaderVersion(protocol.minReaderVersion().orElse(1)));
  }

  public static CreateProvider api2CreateProvider(
      ProviderInput providerInput, Principal currentUser) {
    return new CreateProvider(
        providerInput.getName(),
        providerInput.getStorageName(),
        Optional.ofNullable(providerInput.getMetastoreName()),
        currentUser);
  }

  public static io.whitefox.api.model.v1.generated.Provider provider2Api(Provider provider) {
    return new io.whitefox.api.model.v1.generated.Provider()
        .name(provider.name())
        .storage(storage2api(provider.storage()))
        .metastore(provider.metastore().map(WhitefoxMappers::metastore2api).orElse(null))
        .createdAt(provider.createdAt())
        .createdBy(provider.createdBy().name())
        .updatedAt(provider.updatedAt())
        .updatedBy(provider.updatedBy().name())
        .owner(provider.owner().name());
  }

  public static TableInfo internalTable2api(InternalTable internalTable) {
    return new TableInfo()
        .providerName(internalTable.provider().name())
        .name(internalTable.name())
        .comment(internalTable.comment().orElse(null))
        .properties(internalTable.properties().asMap())
        .validatedAt(internalTable.validatedAt().orElse(null))
        .createdAt(internalTable.createdAt())
        .createdBy(internalTable.createdBy().name())
        .updatedAt(internalTable.updatedAt())
        .updatedBy(internalTable.updatedBy().name());
  }

  public static CreateShare api2createShare(
      CreateShareInput createShare, Function<String, Principal> principalResolver) {
    return new CreateShare(
        createShare.getName(),
        Optional.ofNullable(createShare.getComment()),
        createShare.getRecipients().stream().map(principalResolver).collect(Collectors.toList()),
        createShare.getSchemas());
  }

  public static CreateInternalTable api2createInternalTable(CreateTableInput createTableInput) {
    return new CreateInternalTable(
        createTableInput.getName(),
        Optional.ofNullable(createTableInput.getComment()),
        createTableInput.getSkipValidation(),
        InternalTable.InternalTableProperties.fromMap(createTableInput.getProperties()));
  }

  public static ShareInfo share2api(Share share) {
    return new ShareInfo()
        .name(share.name())
        .comment(share.comment().orElse(null))
        .recipients(share.recipients().stream().map(Principal::name).collect(Collectors.toList()))
        .schemas(new ArrayList<>(share.schemas().keySet()))
        .createdAt(share.createdAt())
        .createdBy(share.createdBy().name())
        .updatedAt(share.updatedAt())
        .updatedBy(share.updatedBy().name())
        .owner(share.owner().name());
  }
}
