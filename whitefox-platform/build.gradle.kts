plugins {
    `java-platform`
}

dependencies {
    constraints {
        api("org.hamcrest:hamcrest:2.2")
        api("org.openapitools:jackson-databind-nullable:0.2.6")
        api("org.eclipse.microprofile.config:microprofile-config-api:3.0.3")
        api("org.eclipse.microprofile.openapi:microprofile-openapi-api:3.1.1")
        api("jakarta.validation:jakarta.validation-api:3.0.2")
        api("jakarta.inject:jakarta.inject-api:2.0.1")
        api("jakarta.annotation:jakarta.annotation-api:2.1.1")
        api("org.openapi4j:openapi-operation-validator:1.0.7")
        api("org.openapi4j:openapi-operation-restassured:1.0.7")
        api("org.junit.jupiter:junit-jupiter:5.10.2")
        api("org.apache.hadoop:hadoop-common:3.4.0")
        api("org.apache.hadoop:hadoop-aws:3.4.0")
        api("org.apache.hadoop:hadoop-client-api:3.4.0")
        api("org.apache.hadoop:hadoop-client-runtime:3.4.0")
        api("io.delta:delta-standalone_2.13:3.1.0")
        api("io.delta:delta-sharing-spark_2.13:3.1.0")
        api("org.apache.spark:spark-sql_2.13:3.5.1")
        api("org.apache.iceberg:iceberg-api:1.5.0")
        api("org.apache.iceberg:iceberg-core:1.5.0")
        api("org.apache.iceberg:iceberg-aws:1.5.0")
        api("software.amazon.awssdk:bom:2.25.25")
        api("software.amazon.awssdk:glue:2.25.25")
        api("software.amazon.awssdk:sts:2.25.25")
        api("software.amazon.awssdk:s3:2.25.25")
        api("software.amazon.awssdk:s3-transfer-manager:2.25.25")
        api("software.amazon.awssdk:utils:2.25.25")
        api("software.amazon.awssdk:url-connection-client:2.25.25")
        api("com.github.jsqlparser:jsqlparser:4.9")
    }
}