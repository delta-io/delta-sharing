import org.openapitools.generator.gradle.plugin.tasks.GenerateTask

plugins {
    java
    id("whitefox.java-conventions")
}

repositories {
    mavenCentral()
}

dependencies {
    implementation(platform(project(":whitefox-platform")))

    // OPENAPI
    implementation("org.eclipse.microprofile.openapi:microprofile-openapi-api")
    implementation("org.openapitools:jackson-databind-nullable")
    testImplementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")
    testImplementation("jakarta.annotation:jakarta.annotation-api")

    // DELTA
    testImplementation("org.apache.hadoop:hadoop-common")
    testImplementation("io.delta:delta-sharing-spark_2.13")

    //SPARK
    testImplementation("org.apache.spark:spark-sql_2.13")
    testImplementation("com.github.mrpowers:spark-fast-tests_2.13:1.3.0")

    //JUNIT
    testImplementation("org.junit.jupiter:junit-jupiter")
}


tasks.getByName<Test>("test") {
    useJUnitPlatform {
        excludeTags.add("clientSparkTest")
    }
}

tasks.withType<Test> {
    environment = env.allVariables()
    systemProperty(
        "java.util.logging.manager",
        "java.util.logging.LogManager"
    ) //TODO modularize the whitefox-conventions plugin
}

tasks.register<Test>("clientSparkTest") {
    useJUnitPlatform {
        includeTags.add("clientSparkTest")
    }
    jvmArgs(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
        "--add-opens=java.base/java.io=ALL-UNNAMED",
        "--add-opens=java.base/java.net=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
        "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
        "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
    )
}

val openApiCodeGenDir = "generated/openapi"
val generatedCodeDirectory = generatedCodeDirectory(layout, openApiCodeGenDir)

val whiteFoxGenerate = tasks.register<GenerateTask>("openapiGenerateClientApi") {
    dependsOn(tasks.spotlessApply)
    generatorName.set("java")
    inputSpec.set("$rootDir/protocol/whitefox-protocol-api.yml")
    library.set("native")
    outputDir.set(generatedCodeDirectory)
    additionalProperties.set(
        mapOf(
            "apiPackage" to "io.whitefox.api.client",
            "invokerPackage" to "io.whitefox.api.utils",
            "modelPackage" to "io.whitefox.api.client.model",
            "dateLibrary" to "java8",
            "sourceFolder" to "src/gen/java",
            "openApiNullable" to "true",
            "annotationLibrary" to "none",
            "serializationLibrary" to "jackson",
            "useJakartaEe" to "true",
            "useRuntimeException" to "true"
        )
    )
}

sourceSets {
    getByName("test") {
        java {
            srcDir("${generatedCodeDirectory(layout, openApiCodeGenDir)}/src/gen/java")
        }
    }
}

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
    options.compilerArgs.add("-parameters")
    dependsOn(whiteFoxGenerate)
}