package io.whitefox.services;

import static org.junit.jupiter.api.condition.OS.WINDOWS;

import io.delta.standalone.DeltaLog;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;

@DisabledOnOs(WINDOWS)
public class DeltaLogServiceTest {

  /**
   * Given that using Delta standalone is not easy to generate delta tables, we do generate them
   * beforehand using an external/local spark-shell.
   * This is some sample code that you need to run in your spark shell to generate new tables for new test cases:
   * To run the spark-shell with delta support execute:
   * {{{
   * spark-shell --packages io.delta:delta-core_2.13:2.3.0 \
   *      --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
   *      --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
   * }}}
   * Take care that the version of delta must be compatible with the version of spark and scala you are using
   * (i.e. I'm using delta 2.3.0 on scala 2.13 because my local spark-shell is version 3.3.0 using scala 2.13
   * To generate the table simply:
   * {{{
   *  val data = spark.range(0, 5)
   *  data.write.format("delta").save("/Volumes/repos/oss/whitefox/server/src/test/resources/delta/samples/delta-table")
   * }}}
   */
  private static final Path deltaTablesRoot = Paths.get(".")
      .toAbsolutePath()
      .resolve("src/test/resources/delta/samples")
      .toAbsolutePath();

  private static String tablePath(String tableName) {
    return deltaTablesRoot.resolve(tableName).toUri().toString();
  }

  @Test
  void simpleTest() {
    var log = DeltaLog.forTable(new Configuration(), tablePath("delta-table"));
    System.out.println("****");
    System.out.println(log.snapshot().getAllFiles().get(0));
    System.out.println("****");
  }
}
