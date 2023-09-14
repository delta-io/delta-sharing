import org.openapitools.generator.gradle.plugin.tasks.GenerateTask

val generatedSourcesDir = "${layout.buildDirectory.get()}/generated/openapi"

plugins {
    java
    id("io.quarkus")
    id("org.openapi.generator")
}
buildscript {
    configurations.all {
        resolutionStrategy {
            force("org.yaml:snakeyaml:1.33")
        }
    }
}
repositories {
    mavenCentral()
    mavenLocal()
}

val quarkusPlatformGroupId: String by project
val quarkusPlatformArtifactId: String by project
val quarkusPlatformVersion: String by project

dependencies {
    implementation(enforcedPlatform("${quarkusPlatformGroupId}:${quarkusPlatformArtifactId}:${quarkusPlatformVersion}"))
    implementation("io.quarkus:quarkus-rest-client-reactive-jackson") // TODO review
    implementation("io.quarkus:quarkus-arc") // TODO review
    implementation("io.quarkus:quarkus-resteasy-reactive") // TODO review
    implementation("org.openapitools:jackson-databind-nullable:0.2.6")
    testImplementation("io.quarkus:quarkus-junit5")
    testImplementation("io.rest-assured:rest-assured") // TODO review
}

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}
tasks.register<GenerateTask>("openapiGenerateLakeSharing") {
    generatorName.set("java")
    inputSpec.set("$rootDir/docs/protocol/lake-sharing-protocol-api.yml")
    library.set("native")
    outputDir.set(generatedSourcesDir)
    additionalProperties.set(
        mapOf(
            "apiPackage" to "io.lake.sharing.api.client",
            "invokerPackage" to "io.lake.sharing.api.utils",
            "modelPackage" to "io.lake.sharing.api.model",
            "dateLibrary" to "java8",
            "openApiNullable" to "true",
            "serializationLibrary" to "jackson",
            "useJakartaEe" to "true",
            "useRuntimeException" to "true"
        )
    )
}

tasks.register<GenerateTask>("openapiGenerateDeltaSharing") {
    generatorName.set("java")
    inputSpec.set("$rootDir/docs/protocol/delta-sharing-protocol-api.yml")
    library.set("native")
    outputDir.set(generatedSourcesDir)
    additionalProperties.set(
        mapOf(
            "apiPackage" to "io.delta.sharing.api.client",
            "invokerPackage" to "io.delta.sharing.api.utils",
            "modelPackage" to "io.delta.sharing.api.model",
            "dateLibrary" to "java8",
            "openApiNullable" to "true",
            "serializationLibrary" to "jackson",
            "useJakartaEe" to "true",
            "useRuntimeException" to "true"
        )
    )
}

tasks.withType<Test> {
    systemProperty("java.util.logging.manager", "org.jboss.logmanager.LogManager")
}
tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
    options.compilerArgs.add("-parameters")
    dependsOn(tasks.named("openapiGenerateLakeSharing"), tasks.named("openapiGenerateDeltaSharing"))
}

sourceSets {
    getByName("main") {
        java {
            srcDir("$generatedSourcesDir/src/main/java")
        }
    }
}