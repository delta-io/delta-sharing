package io.lake.sharing.api.client;

import io.lake.sharing.api.client.model.*;
import java.util.List;
import java.util.Map;

public class SoundnessTest {

  // this is commented because we only want to compile the test, not run it
  // @org.junit.jupiter.api.Test
  public void testHelloEndpoint() {
    final var metastoreApi = new MetastoreApi();
    final var storageApi = new StorageApi();
    final var providerApi = new ProviderApi();
    final var tableApi = new TableApi();
    final var shareApi = new ShareApi();
    Metastore glueMetastoreDefault = metastoreApi.createMetastore(new CreateMetastore()
        .name("glueMetastoreDefault")
        .type(CreateMetastore.TypeEnum.GLUE)
        .skipValidation(false));
    Storage storage = storageApi.createStorage(new CreateStorage()
        .name("s3StorageDefault")
        .type(CreateStorage.TypeEnum.S3)
        .uri("s3://my-test-bucket")
        .credentials(new StorageCredentials(new SimpleAwsCredentials()
            .awsAccessKeyId("1234")
            .awsSecretAccessKey("secret")
            .region("eu-west-1"))));
    Provider defaultS3GlueProvider = providerApi.addProvider(new ProviderInput()
        .name("defaultS3GlueProvider")
        .metastoreName(glueMetastoreDefault.getName())
        .storageName(storage.getName()));
    TableInfo table = tableApi.createTableInProvider(
        defaultS3GlueProvider.getName(),
        new CreateTableInput()
            .name("myTable")
            .properties(Map.of(
                "database", "finance",
                "tableName", "customer_123",
                "type", "iceberg")));
    ShareInfo share = shareApi.createShare(new CreateShareInput()
        .name("test-share")
        .schemas(List.of("test-schema"))
        .recipients(List.of("test@lakesharing.io")));
    tableApi.addTableToSchema(
        share.getName(),
        share.getSchemas().get(0),
        new TableReference().name(table.getName()).providerName(table.getProviderName()));

    OfficialApi officialApi = new OfficialApi();
    String queryResult = officialApi.queryTable(
        share.getName(), share.getSchemas().get(0), table.getName(), new QueryRequest(), null);
  }
}
