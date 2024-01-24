package io.whitefox;

import io.whitefox.core.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

public class IcebergTestUtils extends TestUtils {

  public static final Path icebergTablesRoot = Paths.get(".")
      .toAbsolutePath()
      .getParent()
      .getParent()
      .resolve("core")
      .resolve("src/testFixtures/resources/iceberg/samples")
      .toAbsolutePath();

  public static InternalTable icebergTableWithHadoopCatalog(String database, String tableName) {
    var mrFoxPrincipal = new Principal("Mr. Fox");
    return new InternalTable(
        tableName,
        Optional.empty(),
        new InternalTable.IcebergTableProperties(database, tableName),
        Optional.of(0L),
        0L,
        mrFoxPrincipal,
        0L,
        mrFoxPrincipal,
        getProvider(
            getLocalStorage(mrFoxPrincipal),
            mrFoxPrincipal,
            Optional.of(getLocalHadoopMetastore(mrFoxPrincipal, icebergTablesRoot.toString()))));
  }

  public static Metastore getLocalHadoopMetastore(Principal principal, String location) {
    return getMetastore(
        principal,
        MetastoreType.HADOOP,
        new MetastoreProperties.HadoopMetastoreProperties(location, MetastoreType.HADOOP));
  }

  public static InternalTable s3IcebergTableWithAwsGlueCatalog(
      S3TestConfig s3TestConfig,
      AwsGlueTestConfig awsGlueTestConfig,
      String database,
      String tableName) {
    var mrFoxPrincipal = new Principal("Mr. Fox");
    return new InternalTable(
        tableName,
        Optional.empty(),
        new InternalTable.IcebergTableProperties(database, tableName),
        Optional.of(0L),
        0L,
        mrFoxPrincipal,
        0L,
        mrFoxPrincipal,
        getProvider(
            getS3Storage(mrFoxPrincipal, s3TestConfig),
            mrFoxPrincipal,
            Optional.of(
                getAwsGlueMetastore(mrFoxPrincipal, awsGlueTestConfig.catalogId(), s3TestConfig))));
  }

  public static Metastore getAwsGlueMetastore(
      Principal principal, String catalogId, S3TestConfig s3TestConfig) {
    return getMetastore(
        principal,
        MetastoreType.GLUE,
        new MetastoreProperties.GlueMetastoreProperties(
            catalogId,
            new AwsCredentials.SimpleAwsCredentials(
                s3TestConfig.accessKey(), s3TestConfig.secretKey(), s3TestConfig.region()),
            MetastoreType.GLUE));
  }
}
