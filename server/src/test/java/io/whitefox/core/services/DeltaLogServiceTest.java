package io.whitefox.core.services;

import static io.whitefox.api.server.DeltaTestUtils.tablePath;
import static org.junit.jupiter.api.condition.OS.WINDOWS;

import io.delta.standalone.DeltaLog;
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
   *
   * or if you want to write to s3:
   * {{{
   * export AWS_ACCESS_KEY_ID='************'
   * export AWS_SECRET_ACCESS_KEY='*******************************'
   * spark-shell33 --packages io.delta:delta-core_2.13:2.3.0,org.apache.hadoop:hadoop-aws:3.2.4 \
   *        --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
   *        --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
   *        --conf "spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.EnvironmentVariableCredentialsProvider"
   * }}}
   * Take care that the version of delta must be compatible with the version of spark and scala you are using
   * (i.e. I'm using delta 2.3.0 on scala 2.13 because my local spark-shell is version 3.3.0 using scala 2.13
   *
   * To generate the table simply (replace local path with s3a path if needed):
   * {{{
   *  val data = spark.range(0, 5)
   *  data.write.format("delta").save("/Volumes/repos/oss/whitefox/server/src/test/resources/delta/samples/delta-table")
   * }}}
   * if you want to append to the table:
   * {{{
   * val data = spark.range(10, 20)
   * data.write.format("delta").mode("append").save("/Volumes/repos/oss/whitefox/server/src/test/resources/delta/samples/delta-table")
   * }}}
   */
  @Test
  void simpleTest() {
    var log = DeltaLog.forTable(new Configuration(), tablePath("delta-table"));
    System.out.println("****");
    System.out.println(log.snapshot().getAllFiles().get(0));
    System.out.println("****");
  }
}
