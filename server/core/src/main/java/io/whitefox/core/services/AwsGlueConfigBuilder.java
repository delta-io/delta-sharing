package io.whitefox.core.services;

import io.whitefox.core.AwsCredentials;
import io.whitefox.core.MetastoreProperties;
import io.whitefox.core.aws.utils.StaticCredentialsProvider;
import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.AwsProperties;

public class AwsGlueConfigBuilder {

  public Map<String, String> buildConfig(
      MetastoreProperties.GlueMetastoreProperties glueMetastoreProperties) {
    if (glueMetastoreProperties.credentials() instanceof AwsCredentials.SimpleAwsCredentials) {
      AwsCredentials.SimpleAwsCredentials credentials =
          (AwsCredentials.SimpleAwsCredentials) glueMetastoreProperties.credentials();
      Map<String, String> config = new HashMap<>();
      config.put(CatalogProperties.CATALOG_IMPL, "org.apache.iceberg.aws.glue.GlueCatalog");
      config.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");
      config.put(AwsProperties.GLUE_CATALOG_ID, glueMetastoreProperties.catalogId());
      config.put(AwsClientProperties.CLIENT_REGION, credentials.region());
      config.put(
          AwsClientProperties.CLIENT_CREDENTIALS_PROVIDER,
          StaticCredentialsProvider.class.getName());
      config.put(
          String.format("%s.%s", AwsClientProperties.CLIENT_CREDENTIALS_PROVIDER, "accessKeyId"),
          credentials.awsAccessKeyId());
      config.put(
          String.format(
              "%s.%s", AwsClientProperties.CLIENT_CREDENTIALS_PROVIDER, "secretAccessKey"),
          credentials.awsSecretAccessKey());
      return config;
    } else {
      throw new IllegalArgumentException(String.format(
          "Credentials type not supported with glue metastore %s", glueMetastoreProperties));
    }
  }
}
