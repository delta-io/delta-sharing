package io.whitefox;

import io.whitefox.core.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

public class DeltaTestUtils {

  private static final Path deltaTablesRoot = Paths.get(".")
      .toAbsolutePath()
      .getParent()
      .getParent()
      .resolve("core")
      .resolve("src/testFixtures/resources/delta/samples")
      .toAbsolutePath();

  public static String deltaTableUri(String tableName) {
    return deltaTablesRoot
        .resolve(tableName)
        .toAbsolutePath()
        .normalize()
        .toUri()
        .toString();
  }

  public static InternalTable deltaTable(String tableName) {
    var mrFoxPrincipal = new Principal("Mr. Fox");
    return new InternalTable(
        tableName,
        Optional.empty(),
        new InternalTable.DeltaTableProperties(deltaTableUri(tableName)),
        Optional.of(0L),
        0L,
        mrFoxPrincipal,
        0L,
        mrFoxPrincipal,
        getProvider(getLocalStorage(mrFoxPrincipal), mrFoxPrincipal));
  }

  public static Provider getProvider(Storage storage, Principal principal) {
    return new Provider(
        "provider", storage, Optional.empty(), 0L, principal, 0L, principal, principal);
  }

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

  public static Storage getLocalStorage(Principal principal) {
    return getStorage(
        principal,
        StorageType.LOCAL,
        new S3TestConfig("fakeRegion", "fakeAccessKey", "fakeSecretKey"));
  }

  public static Storage getS3Storage(Principal principal, S3TestConfig s3TestConfig) {
    return getStorage(principal, StorageType.S3, s3TestConfig);
  }

  public static String s3DeltaTableUri(String s3TableName) {
    return String.format("s3a://whitefox-s3-test-bucket/delta/samples/%s", s3TableName);
  }

  public static InternalTable s3DeltaTable(String s3TableName, S3TestConfig s3TestConfig) {
    var mrFoxPrincipal = new Principal("Mr. Fox");
    return new InternalTable(
        s3TableName,
        Optional.empty(),
        new InternalTable.DeltaTableProperties(s3DeltaTableUri(s3TableName)),
        Optional.of(0L),
        0L,
        mrFoxPrincipal,
        0L,
        mrFoxPrincipal,
        getProvider(getS3Storage(mrFoxPrincipal, s3TestConfig), mrFoxPrincipal));
  }
}
