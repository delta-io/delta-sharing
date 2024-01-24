package io.whitefox.core.services;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.condition.OS.WINDOWS;

import io.whitefox.IcebergTestUtils;
import io.whitefox.S3TestConfig;
import io.whitefox.core.aws.utils.StaticCredentialsProvider;
import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;

@DisabledOnOs(WINDOWS)
public class IcebergCatalogServiceTest {

  private final S3TestConfig s3TestConfig = S3TestConfig.loadFromEnv();

  /**
   * This is some sample code that you need to run in your spark shell to generate new iceberg tables for new test cases:
   * To run the spark-shell with iceberg support execute:
   * {{{
   * spark-shell --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.apache.iceberg:iceberg-aws-bundle:1.4.2 \
   *         										--conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkCatalog \
   *         										--conf spark.sql.catalog.spark_catalog.type=hadoop \
   *         										--conf spark.sql.catalog.spark_catalog.warehouse=/Volumes/repos/oss/whitefox/server/core/src/testFixtures/resources/iceberg/samples/ \
   *                                                --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
   *
   * Take care that the version of iceberg must be compatible with the version of spark and scala you are using
   * (i.e. I'm using iceberg 3.5 on scala 2.12 because my local spark-shell is version 3.5.0 using scala 2.12
   *
   * First, you need to create an iceberg table with your local hadoop catalog
   * {{{
   * 		import org.apache.iceberg.catalog.Namespace
   * 		import org.apache.iceberg.Schema
   * 		import org.apache.iceberg.catalog.TableIdentifier
   * 		import org.apache.iceberg.hadoop.HadoopCatalog
   * 		import java.util.Map
   * 		import org.apache.hadoop.conf.Configuration
   *
   *  		val catalog = new HadoopCatalog()
   *     	catalog.setConf(new Configuration())
   *     	catalog.initialize("test_hadoop_catalog",
   *       	    Map.of("warehouse", "/Volumes/repos/oss/whitefox/server/core/src/testFixtures/resources/iceberg/samples/"))
   *        catalog.createNamespace(Namespace.of("test_db"))
   *        val schema = new Schema(org.apache.iceberg.types.Types.NestedField.required(1, "id", org.apache.iceberg.types.Types.LongType.get()))
   * 		catalog.createTable(TableIdentifier.of("test_db", "icebergtable1"),  schema)
   * }}}
   *
   * Then, you can append data on your iceberg table
   * {{{
   * 		val data = spark.range(0, 5)
   * 		data.writeTo("test_db.icebergtable1").append()
   * }}}
   */
  @Test
  void localIcebergTableWithHadoopCatalogTest() throws IOException {
    try (HadoopCatalog hadoopCatalog = new HadoopCatalog()) {
      // Initialize catalog
      hadoopCatalog.setConf(new Configuration());
      hadoopCatalog.initialize(
          "test_hadoop_catalog",
          Map.of("warehouse", IcebergTestUtils.icebergTablesRoot.toString()));
      TableIdentifier tableIdentifier = TableIdentifier.of("test_db", "icebergtable1");

      // Load the Iceberg table
      Table table = hadoopCatalog.loadTable(tableIdentifier);
      assertEquals("test_hadoop_catalog.test_db.icebergtable1", table.name());
    }
  }

  /**
   * This is some sample code that you need to run in your spark shell to generate new iceberg tables on s3 for new test cases:
   * First, you need to retrieve the aws credentials of your aws account, then:
   *
   * {{{
   * export AWS_ACCESS_KEY_ID='************'
   * export AWS_SECRET_ACCESS_KEY='*******************************'
   * spark-shell --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.apache.iceberg:iceberg-aws-bundle:1.4.2 \
   *                                                                              --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkCatalog \
   *                                                                              --conf spark.sql.catalog.spark_catalog.warehouse=specify-your-s3-bucket \
   *                                                                              --conf spark.sql.catalog.spark_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog \
   *                                                                              --conf spark.sql.catalog.spark_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
   *                                                                              --conf spark.sql.catalog.spark_catalog.glue.skip-name-validation=false
   * }}}
   *
   * Take care that the version of iceberg must be compatible with the version of spark and scala you are using
   * (i.e. I'm using iceberg 3.5 on scala 2.12 because my local spark-shell is version 3.5.0 using scala 2.12
   *
   * Now, you need to create an iceberg table with your aws glue catalog
   * {{{
   * 		import org.apache.iceberg.catalog.Namespace
   * 		import org.apache.iceberg.Schema
   * 		import org.apache.iceberg.catalog.TableIdentifier
   * 		import org.apache.iceberg.hadoop.HadoopCatalog
   * 		import java.util.Map
   * 		import org.apache.hadoop.conf.Configuration
   *
   *  		val catalog = new GlueCatalog()
   *     	catalog.setConf(new Configuration())
   *     	catalog.initialize("test_glue_catalog", Map.of(
   *             "warehouse", "specify-your-s3-bucket"))
   *         catalog.createNamespace(Namespace.of("test_glue_db"))
   *         val schema = new Schema(org.apache.iceberg.types.Types.NestedField.required(1, "id", org.apache.iceberg.types.Types.LongType.get()))
   * 		catalog.createTable(TableIdentifier.of("test_glue_db", "icebergtable1"),  schema)
   *
   * Then, you can append data on your iceberg table
   * {{{
   * 		val data = spark.range(0, 5)
   * 		data.writeTo("test_glue_db.icebergtable1").append()
   * }}}
   */
  @Test
  void s3IcebergTableWithAwsGlueCatalogTest() throws IOException {
    try (GlueCatalog glueCatalog = new GlueCatalog()) {

      // Initialize catalog
      glueCatalog.setConf(new Configuration());
      glueCatalog.initialize(
          "test_glue_catalog",
          Map.of(
              AwsClientProperties.CLIENT_REGION,
              s3TestConfig.region(),
              AwsClientProperties.CLIENT_CREDENTIALS_PROVIDER,
              StaticCredentialsProvider.class.getName(),
              String.format(
                  "%s.%s", AwsClientProperties.CLIENT_CREDENTIALS_PROVIDER, "accessKeyId"),
              s3TestConfig.accessKey(),
              String.format(
                  "%s.%s", AwsClientProperties.CLIENT_CREDENTIALS_PROVIDER, "secretAccessKey"),
              s3TestConfig.secretKey()));

      TableIdentifier tableIdentifier = TableIdentifier.of("test_glue_db", "icebergtable1");
      // Load the Iceberg table
      Table table = glueCatalog.loadTable(tableIdentifier);
      assertEquals("test_glue_catalog.test_glue_db.icebergtable1", table.name());
    }
  }
}
