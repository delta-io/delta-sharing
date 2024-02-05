import org.openapitools.generator.gradle.plugin.tasks.GenerateTask

plugins {
    java
    id("com.diffplug.spotless")
    id("whitefox.java-conventions")
}

repositories {
    mavenCentral()
}

dependencies {
    // OPENAPI
    implementation("org.eclipse.microprofile.openapi:microprofile-openapi-api:3.1.1")
    implementation("org.openapitools:jackson-databind-nullable:0.2.6")
    testImplementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")
    testImplementation("jakarta.annotation:jakarta.annotation-api:2.1.1")

    // DELTA
    testImplementation("org.apache.hadoop:hadoop-common:3.3.6")
    testImplementation("io.delta:delta-sharing-spark_2.13:1.0.4")

    //SPARK
    testImplementation("org.apache.spark:spark-sql_2.13:3.5.0")
    testImplementation("com.github.mrpowers:spark-fast-tests_2.13:1.3.0")

    //JUNIT
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.2")
}


tasks.getByName<Test>("test") {
    useJUnitPlatform {
        excludeTags.add("clientSparkTest")
    }
}

tasks.withType<Test> {
    environment = env.allVariables()
    systemProperty ("java.util.logging.manager", "java.util.logging.LogManager") //TODO modularize the whitefox-conventions plugin
}

tasks.register<Test>("clientSparkTest") {
    useJUnitPlatform {
        includeTags.add("clientSparkTest")
    }
}

val openApiCodeGenDir = "generated/openapi"
val generatedCodeDirectory = generatedCodeDirectory(layout, openApiCodeGenDir)

val whiteFoxGenerate = tasks.register<GenerateTask>("openapiGenerateClientApi") {
    dependsOn(tasks.spotlessApply)
    generatorName.set("java")
    inputSpec.set("$rootDir/protocol/whitefox-protocol-api.yml")
    library.set("native")
    outputDir.set(generatedCodeDirectory)
    additionalProperties.set(mapOf(
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
    ))
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