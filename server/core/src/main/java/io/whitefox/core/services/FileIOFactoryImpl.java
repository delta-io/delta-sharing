package io.whitefox.core.services;

import io.whitefox.core.AwsCredentials;
import io.whitefox.core.Storage;
import io.whitefox.core.StorageProperties;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.ResolvingFileIO;

public class FileIOFactoryImpl implements FileIOFactory {

  @Override
  public FileIO newFileIO(Storage storage) {
    storage.properties().validateTypeAndUri(storage.type(), storage.uri());
    var propsAndConfs = buildPropsFromStorage(storage);
    String className = resolveClassNameFromStorage(storage);
    return CatalogUtil.loadFileIO(className, propsAndConfs.props(), propsAndConfs.conf());
  }

  private static String resolveClassNameFromStorage(Storage storage) {
    String className = null;
    try (ResolvingFileIO rfi = new ResolvingFileIO()) {
      className = rfi.ioClass(storage.uri()).getCanonicalName();
    }
    return className;
  }

  private PropsAndConf buildPropsFromStorage(Storage storage) {
    switch (storage.type()) {
      case S3:
        if (storage.properties() instanceof StorageProperties.S3Properties) {
          AwsCredentials credentials =
              ((StorageProperties.S3Properties) storage.properties()).credentials();
          if (credentials instanceof AwsCredentials.SimpleAwsCredentials simpleAwsCredentials) {
            return new PropsAndConf(
                Map.of(
                    S3FileIOProperties.ACCESS_KEY_ID, simpleAwsCredentials.awsAccessKeyId(),
                    S3FileIOProperties.SECRET_ACCESS_KEY, simpleAwsCredentials.awsSecretAccessKey(),
                    AwsClientProperties.CLIENT_REGION, simpleAwsCredentials.region()),
                null);
          } else {
            throw new RuntimeException("Missing aws credentials"); // TODO better message
          }
        } else {
          throw new RuntimeException("Missing s3 properties"); // TODO better message
        }
      case LOCAL:
        if (storage.properties() instanceof StorageProperties.LocalProperties) {
          return new PropsAndConf(
              Map.of(), ((StorageProperties.LocalProperties) storage.properties()).hadoopConf());
        } else {
          throw new RuntimeException("Missing local properties"); // TODO better message
        }

      default:
        throw new IllegalArgumentException(
            String.format("Unsupported storage type: [%s]", storage.type()));
    }
  }

  private record PropsAndConf(Map<String, String> props, Configuration conf) {}
}
