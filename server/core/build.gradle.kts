plugins {
    `java-library`
    `java-test-fixtures`
    id("whitefox.java-conventions")
}

val quarkusPlatformGroupId: String by project
val quarkusPlatformArtifactId: String by project
val quarkusPlatformVersion: String by project
val hadoopVersion: String by project
val jakartaVersion: String by project
val microprofileConfigVersion: String by project
val awsSdkVersion: String by project
val hamcrestVersion: String by project

// region dependencies

dependencies {
    implementation(enforcedPlatform("${quarkusPlatformGroupId}:${quarkusPlatformArtifactId}:${quarkusPlatformVersion}"))
    // QUARKUS
    compileOnly("jakarta.enterprise:jakarta.enterprise.cdi-api")
    compileOnly("jakarta.ws.rs:jakarta.ws.rs-api")
    compileOnly("org.eclipse.microprofile.config:microprofile-config-api")


    testFixturesImplementation(String.format("jakarta.inject:jakarta.inject-api:%s", jakartaVersion))
    testFixturesImplementation(String.format("org.eclipse.microprofile.config:microprofile-config-api:%s", microprofileConfigVersion))

    // DELTA
    implementation("io.delta:delta-standalone_2.13:3.0.0")
    implementation(String.format("org.apache.hadoop:hadoop-common:%s", hadoopVersion))

    //ICEBERG
    implementation("org.apache.iceberg:iceberg-api:1.4.3")
    implementation("org.apache.iceberg:iceberg-core:1.4.3")
    implementation("org.apache.iceberg:iceberg-aws:1.4.3")
    implementation("software.amazon.awssdk:glue:2.23.10")
    implementation("software.amazon.awssdk:sts:2.23.10")
    implementation("software.amazon.awssdk:s3:2.23.10")

    //AWS
    compileOnly(String.format("com.amazonaws:aws-java-sdk-bom:%s", awsSdkVersion))
    compileOnly(String.format("com.amazonaws:aws-java-sdk-s3:%s", awsSdkVersion))
    implementation(String.format("org.apache.hadoop:hadoop-aws:%s", hadoopVersion))

    // TEST
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("io.quarkus:quarkus-arc")
    testImplementation(String.format("org.hamcrest:hamcrest:%s", hamcrestVersion))
    testImplementation(project(":server:persistence:memory"))
}

// endregion

// region java compile

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
    options.compilerArgs.add("-parameters")
}

// endregion

// region test running

tasks.withType<Test> {
    environment = env.allVariables()
}

// endregion

// region code coverage

tasks.jacocoTestCoverageVerification {
    if (!isWindowsBuild()) {
        violationRules {
            rule {
                limit {
                    minimum = BigDecimal.valueOf(0.66)
                }
            }
        }
    }
}

// endregion