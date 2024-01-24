package io.whitefox.api.utils;

import static io.whitefox.api.client.ApiUtils.configureApiClientFromResource;
import static io.whitefox.api.client.ApiUtils.ignoreConflict;

import io.whitefox.api.client.*;
import io.whitefox.api.client.model.*;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class StorageManagerInitializer {
  private final S3TestConfig s3TestConfig;
  private final StorageV1Api storageV1Api;
  private final MetastoreV1Api metastoreV1Api;
  private final ProviderV1Api providerV1Api;
  private final TableV1Api tableV1Api;
  private final ShareV1Api shareV1Api;
  private final SchemaV1Api schemaV1Api;

  public StorageManagerInitializer() {
    ApiClient apiClient = configureApiClientFromResource("MrFoxProfile.json");
    this.s3TestConfig = S3TestConfig.loadFromEnv();
    this.storageV1Api = new StorageV1Api(apiClient);
    this.providerV1Api = new ProviderV1Api(apiClient);
    this.tableV1Api = new TableV1Api(apiClient);
    this.shareV1Api = new ShareV1Api(apiClient);
    this.schemaV1Api = new SchemaV1Api(apiClient);
    this.metastoreV1Api = new MetastoreV1Api(apiClient);
  }

  public void initStorageManager() {
    ignoreConflict(() -> storageV1Api.createStorage(createStorageRequest(s3TestConfig)));
    ignoreConflict(() -> shareV1Api.createShare(createShareRequest()));
  }

  public void createS3DeltaTable() {
    var providerRequest = addProviderRequest(Optional.empty(), TableFormat.delta);
    ignoreConflict(() -> providerV1Api.addProvider(providerRequest));
    var createTableRequest = createDeltaTableRequest();
    ignoreConflict(
        () -> tableV1Api.createTableInProvider(providerRequest.getName(), createTableRequest));
    var shareRequest = createShareRequest();
    var schemaRequest = createSchemaRequest(TableFormat.delta);
    ignoreConflict(() -> schemaV1Api.createSchema(shareRequest.getName(), schemaRequest));
    ignoreConflict(() -> schemaV1Api.addTableToSchema(
        shareRequest.getName(),
        schemaRequest,
        addTableToSchemaRequest(providerRequest.getName(), createTableRequest.getName())));
  }

  public TableInfo createIcebergTableWithGlueMetastore() {
    var metastoreRequest = createMetastoreRequest(s3TestConfig, CreateMetastore.TypeEnum.GLUE);
    var metastore = ApiUtils.recoverConflictLazy(
        () -> metastoreV1Api.createMetastore(metastoreRequest),
        () -> metastoreV1Api.describeMetastore(metastoreRequest.getName()));
    var providerRequest = addProviderRequest(Optional.of(metastore.getName()), TableFormat.iceberg);
    var provider = ApiUtils.recoverConflictLazy(
        () -> providerV1Api.addProvider(providerRequest),
        () -> providerV1Api.getProvider(providerRequest.getName()));
    var createTableRequest = createIcebergTableRequest();
    return ApiUtils.recoverConflictLazy(
        () -> tableV1Api.createTableInProvider(provider.getName(), createTableRequest),
        () -> tableV1Api.describeTableInProvider(provider.getName(), createTableRequest.getName()));
  }

  private String createSchemaRequest(TableFormat tableFormat) {
    return format("s3schema", tableFormat);
  }

  private AddTableToSchemaRequest addTableToSchemaRequest(String providerName, String tableName) {
    return new AddTableToSchemaRequest()
        .name(tableName)
        .reference(new TableReference().providerName(providerName).name(tableName));
  }

  private CreateShareInput createShareRequest() {
    return new CreateShareInput().name("s3share").recipients(List.of("Mr.Fox")).schemas(List.of());
  }

  private CreateTableInput createDeltaTableRequest() {
    return new CreateTableInput()
        .name("s3Table1")
        .skipValidation(true)
        .properties(Map.of(
            "type", "delta",
            "location", "s3a://whitefox-s3-test-bucket/delta/samples/delta-table"));
  }

  private CreateTableInput createIcebergTableRequest() {
    return new CreateTableInput()
        .name("s3IcebergTable1")
        .skipValidation(true)
        .properties(Map.of(
            "type", "iceberg", "databaseName", "test_glue_db", "tableName", "icebergtable1"));
  }

  private ProviderInput addProviderRequest(
      Optional<String> metastoreName, TableFormat tableFormat) {
    return new ProviderInput()
        .name(format("MrFoxProvider", tableFormat))
        .storageName("MrFoxStorage")
        .metastoreName(metastoreName.orElse(null));
  }

  private CreateStorage createStorageRequest(S3TestConfig s3TestConfig) {
    return new CreateStorage()
        .name("MrFoxStorage")
        .type(CreateStorage.TypeEnum.S3)
        .properties(new StorageProperties(new S3Properties()
            .credentials(new SimpleAwsCredentials()
                .region(s3TestConfig.getRegion())
                .awsAccessKeyId(s3TestConfig.getAccessKey())
                .awsSecretAccessKey(s3TestConfig.getSecretKey()))))
        .skipValidation(true);
  }

  private CreateMetastore createMetastoreRequest(
      S3TestConfig s3TestConfig, CreateMetastore.TypeEnum type) {
    return new CreateMetastore()
        .name("MrFoxMetastore")
        .type(type)
        .skipValidation(true)
        .properties(new MetastoreProperties(new GlueProperties()
            .catalogId("catalogId") // TODO
            .credentials(new SimpleAwsCredentials()
                .region(s3TestConfig.getRegion())
                .awsAccessKeyId(s3TestConfig.getAccessKey())
                .awsSecretAccessKey(s3TestConfig.getSecretKey()))));
  }

  private String format(String value, TableFormat tableFormat) {
    return String.format("%s%s", value, tableFormat.name());
  }
}
