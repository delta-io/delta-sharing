package io.whitefox;

import io.whitefox.core.*;
import java.util.Optional;

public class TestUtils {

  public static Storage getStorage(
      Principal principal, StorageType storageType, S3TestConfig s3TestConfig) {
    return new Storage(
        "storage",
        Optional.empty(),
        principal,
        storageType,
        Optional.empty(),
        "uri",
        0L,
        principal,
        0L,
        principal,
        new StorageProperties.S3Properties(new AwsCredentials.SimpleAwsCredentials(
            s3TestConfig.accessKey(), s3TestConfig.secretKey(), s3TestConfig.region())));
  }

  public static Provider getProvider(
      Storage storage, Principal principal, Optional<Metastore> metastore) {
    return new Provider("provider", storage, metastore, 0L, principal, 0L, principal, principal);
  }

  public static Storage getLocalStorage(Principal principal) {
    return getStorage(
        principal,
        StorageType.LOCAL,
        new S3TestConfig("fakeRegion", "fakeAccessKey", "fakeSecretKey"));
  }

  public static Metastore getMetastore(
      Principal principal, MetastoreType metastoreType, MetastoreProperties metastoreProperties) {
    return new Metastore(
        "metastore",
        Optional.empty(),
        principal,
        metastoreType,
        metastoreProperties,
        Optional.empty(),
        0L,
        principal,
        0L,
        principal);
  }

  public static Storage getS3Storage(Principal principal, S3TestConfig s3TestConfig) {
    return new Storage(
        "storage",
        Optional.empty(),
        principal,
        StorageType.S3,
        Optional.empty(),
        "uri",
        0L,
        principal,
        0L,
        principal,
        new StorageProperties.S3Properties(new AwsCredentials.SimpleAwsCredentials(
            s3TestConfig.accessKey(), s3TestConfig.secretKey(), s3TestConfig.region())));
  }
}
