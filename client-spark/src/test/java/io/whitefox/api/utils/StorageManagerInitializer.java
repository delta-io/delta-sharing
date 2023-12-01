package io.whitefox.api.utils;

import io.whitefox.api.client.*;
import io.whitefox.api.client.model.*;
import java.util.List;
import java.util.Map;

public class StorageManagerInitializer {
  private final S3TestConfig s3TestConfig;
  private final StorageV1Api storageV1Api;
  private final ProviderV1Api providerV1Api;
  private final TableV1Api tableV1Api;
  private final ShareV1Api shareV1Api;
  private final SchemaV1Api schemaV1Api;

  public StorageManagerInitializer() {
    var apiClient = new ApiClient();
    this.s3TestConfig = S3TestConfig.loadFromEnv();
    this.storageV1Api = new StorageV1Api(apiClient);
    this.providerV1Api = new ProviderV1Api(apiClient);
    this.tableV1Api = new TableV1Api(apiClient);
    this.shareV1Api = new ShareV1Api(apiClient);
    this.schemaV1Api = new SchemaV1Api(apiClient);
  }

  public void initStorageManager() {
    storageV1Api.createStorage(createStorageRequest(s3TestConfig));
    providerV1Api.addProvider(addProviderRequest());
    tableV1Api.createTableInProvider(addProviderRequest().getName(), createTableRequest());
    shareV1Api.createShare(createShareRequest());
    schemaV1Api.createSchema(createShareRequest().getName(), createSchemaRequest());
    schemaV1Api.addTableToSchema(
        createShareRequest().getName(), createSchemaRequest(), addTableToSchemaRequest());
  }

  private String createSchemaRequest() {
    return "s3schema";
  }

  private AddTableToSchemaRequest addTableToSchemaRequest() {
    return new AddTableToSchemaRequest()
        .name("s3Table1")
        .reference(new TableReference().providerName("MrFoxProvider").name("s3Table1"));
  }

  private CreateShareInput createShareRequest() {
    return new CreateShareInput().name("s3share").recipients(List.of("Mr.Fox")).schemas(List.of());
  }

  private CreateTableInput createTableRequest() {
    return new CreateTableInput()
        .name("s3Table1")
        .skipValidation(true)
        .properties(Map.of(
            "type", "delta",
            "location", "s3a://whitefox-s3-test-bucket/delta/samples/delta-table"));
  }

  private ProviderInput addProviderRequest() {
    return new ProviderInput()
        .name("MrFoxProvider")
        .storageName("MrFoxStorage")
        .metastoreName(null);
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
}
