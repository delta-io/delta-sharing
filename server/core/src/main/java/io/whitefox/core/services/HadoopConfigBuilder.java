package io.whitefox.core.services;

import io.whitefox.core.AwsCredentials;
import io.whitefox.core.Storage;
import io.whitefox.core.StorageProperties;
import org.apache.hadoop.conf.Configuration;

public class HadoopConfigBuilder {

  public Configuration buildConfig(Storage storage) {
    var configuration = new Configuration();
    switch (storage.type()) {
      case S3: {
        if (storage.properties() instanceof StorageProperties.S3Properties) {
          AwsCredentials.SimpleAwsCredentials credentials = (AwsCredentials.SimpleAwsCredentials)
              ((StorageProperties.S3Properties) storage.properties()).credentials();
          configuration.set(
              "fs.s3a.aws.credentials.provider",
              "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
          configuration.set("fs.s3a.access.key", credentials.awsAccessKeyId());
          configuration.set("fs.s3a.secret.key", credentials.awsSecretAccessKey());
          configuration.set("fs.s3a.endpoint.region", credentials.region());
          return configuration;
        } else {
          throw new RuntimeException("missing S3 storage properties");
        }
      }
      case LOCAL: {
        return configuration;
      }
      default:
        throw new RuntimeException(
            String.format("storage type %s not handled", storage.type().value));
    }
  }
}
