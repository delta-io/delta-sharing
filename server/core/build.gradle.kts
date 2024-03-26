plugins {
    `java-library`
    `java-test-fixtures`
    id("whitefox.java-conventions")
}

val quarkusPlatformGroupId: String by project
val quarkusPlatformArtifactId: String by project
val quarkusPlatformVersion: String by project


// region dependencies
dependencies {
    api(platform(project(":whitefox-platform")))

    implementation(enforcedPlatform("${quarkusPlatformGroupId}:${quarkusPlatformArtifactId}:${quarkusPlatformVersion}"))
    // QUARKUS
    compileOnly("jakarta.enterprise:jakarta.enterprise.cdi-api")
    compileOnly("jakarta.ws.rs:jakarta.ws.rs-api")
    compileOnly("org.eclipse.microprofile.config:microprofile-config-api")


    testFixturesImplementation("jakarta.inject:jakarta.inject-api")
    testFixturesImplementation("org.eclipse.microprofile.config:microprofile-config-api")

    // DELTA
    implementation("io.delta:delta-standalone_2.13")
    implementation("org.apache.hadoop:hadoop-common")

    //ICEBERG
    implementation("org.apache.iceberg:iceberg-api")
    implementation("org.apache.iceberg:iceberg-core")
    implementation("org.apache.iceberg:iceberg-aws")

    //AWS
    implementation("software.amazon.awssdk:glue")
    implementation("software.amazon.awssdk:sts")
    implementation("software.amazon.awssdk:s3")
    implementation("software.amazon.awssdk:utils")
    implementation("software.amazon.awssdk:url-connection-client")
    implementation("software.amazon.awssdk:s3-transfer-manager")
    implementation("org.apache.hadoop:hadoop-aws") {
        exclude(group = "software.amazon.awssdk", module = "bundle")
    }

    //PREDICATE PARSER
    implementation("com.github.jsqlparser:jsqlparser")

    // TEST
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("io.quarkus:quarkus-arc")
    testImplementation("org.hamcrest:hamcrest")
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
                    minimum = BigDecimal.valueOf(0.72)
                }
            }
        }
    }
}

// endregion