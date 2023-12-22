package io.whitefox.api.utils;

import org.apache.spark.sql.SparkSession;

public interface SparkUtil {

  default SparkSession newSparkSession() {
    return SparkSession.builder()
        .appName("delta sharing client test")
        .config("spark.driver.host", "localhost")
        .master("local[1, 4]")
        .getOrCreate();
  }
}
