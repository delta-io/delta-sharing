//> using scala "2.12.18"

//> using dep "org.apache.spark::spark-core:3.4.0"
//> using dep "org.apache.spark::spark-sql:3.4.0"
//> using dep "org.apache.spark::spark-catalyst:3.4.0"
//> using dep "io.delta::delta-sharing-spark:0.7.0"
//> using dep "io.delta::delta-core:2.4.0"
//> using dep "org.json4s::json4s-ast:3.5.3"
//> using dep "org.codehaus.jackson:jackson-mapper-asl:1.9.13"
//> using dep "com.fasterxml.jackson.core:jackson-core:2.8.8"
//> using dep "org.json4s::json4s-core:3.5.3"
//> using dep "org.json4s::json4s-jackson:3.5.3"
//> using dep "org.apache.httpcomponents:httpclient:4.5.14"

// When using Java 9+, remove the first slash character from the line 17 and 18

///> using javaOptions "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
///> using javaOptions "--add-opens=java.base/sun.security.action=ALL-UNNAMED"

// hack to violate encapsulation to use DeltaSharingRestClient
package io.delta.sharing.spark

import org.apache.spark.sql.SparkSession
import scala.collection.JavaConverters._
import java.sql.Timestamp
import java.nio.file.Path
import org.apache.hadoop.conf.Configuration

object App {
  def main(args: Array[String]): Unit = {
    val share = "open-datasets.share"
    val conf = new Configuration()
    val provider: DeltaSharingProfileProvider =
      new io.delta.sharing.spark.DeltaSharingFileProfileProvider(
        conf,
        share
      )
    val client = new io.delta.sharing.spark.DeltaSharingRestClient(provider)
    println(client.listAllTables().mkString("\n"))

    val spark = SparkSession
      .builder()
      .appName("example")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._
    val result = spark.read
      .format("deltaSharing")
      .load(
        share + "/#delta_sharing.default.owid-covid-data"
      )
      .where("iso_code == 'USA'")
      .select(
        $"iso_code",
        $"total_cases",
        $"human_development_index"
      )
    result.show(10)

  }

}
