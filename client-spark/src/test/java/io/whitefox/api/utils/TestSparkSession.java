package io.whitefox.api.utils;

import org.apache.spark.sql.SparkSession;

public class TestSparkSession {

  private static final class SparkHolder {
    private static final SparkSession spark = SparkSession.builder()
        .appName("delta sharing client test")
        .config("spark.driver.host", "localhost")
        .master("local[1, 4]")
        .getOrCreate();
  }

  public static SparkSession newSparkSession() {
    return SparkHolder.spark.newSession();
  }
}
