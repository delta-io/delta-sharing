package io.whitefox.api.client;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.mrpowers.spark.fast.tests.DatasetComparer;
import io.whitefox.api.models.MrFoxDeltaTableSchema;
import io.whitefox.api.utils.StorageManagerInitializer;
import java.util.List;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import scala.collection.GenMap;

@Tag("clientSparkTest")
public class ITDeltaSharingClient implements DatasetComparer {

  private final String tablePath = String.format(
      "%s#%s.%s.%s",
      getClass().getClassLoader().getResource("MrFoxProfile.json"),
      "s3share",
      "s3schema",
      "s3Table1");

  private final SparkSession spark = SparkSession.builder()
      .appName("delta sharing client test")
      .config("spark.driver.host", "localhost")
      .master("local[1, 4]")
      .getOrCreate();

  @BeforeAll
  static void initStorageManager() {
    new StorageManagerInitializer().initStorageManager();
  }

  @Test
  void showS3Table1withQueryTableApi() {
    var ds = spark.read().format("deltaSharing").load(tablePath);
    var expectedSchema = new StructType(new StructField[] {
      new StructField("id", DataType.fromDDL("long"), true, new Metadata(GenMap.empty()))
    });
    var expectedData = spark
        .createDataFrame(
            List.of(
                new MrFoxDeltaTableSchema(0),
                new MrFoxDeltaTableSchema(3),
                new MrFoxDeltaTableSchema(2),
                new MrFoxDeltaTableSchema(1),
                new MrFoxDeltaTableSchema(4)),
            MrFoxDeltaTableSchema.class)
        .toDF();

    assertEquals(expectedSchema.json(), ds.schema().json());
    assertEquals(5, ds.count());
    assertSmallDatasetEquality(ds, expectedData, true, false, false, 500);
  }
}
