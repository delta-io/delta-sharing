import org.openapitools.generator.gradle.plugin.tasks.GenerateTask

val openApiGeneratedSourcesRelativeToBuildPath = "generated/openapi"

val openApiGeneratedSourcesAbsolutePath =
    "${layout.buildDirectory.get()}/${openApiGeneratedSourcesRelativeToBuildPath}"

val openApiGeneratedSourcesRelativeToProjectPath =
    "${layout.buildDirectory.get().asFile.toRelativeString(layout.projectDirectory.asFile)}/${openApiGeneratedSourcesRelativeToBuildPath}"

plugins {
    java
    id("io.quarkus")
    id("org.openapi.generator")
    id("com.diffplug.spotless")
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
    implementation("io.quarkus:quarkus-resteasy-reactive")
    implementation("io.quarkus:quarkus-resteasy-reactive-jackson")
    implementation("io.quarkus:quarkus-arc")
    implementation("org.openapitools:jackson-databind-nullable:0.2.6")
    implementation("org.eclipse.microprofile.openapi:microprofile-openapi-api:3.1.1")
    implementation("jakarta.validation:jakarta.validation-api:3.0.2")

    testImplementation("io.quarkus:quarkus-junit5")
    testImplementation("io.rest-assured:rest-assured")
}

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

buildscript {
    configurations.all {
        resolutionStrategy {
            force("org.yaml:snakeyaml:1.33")
        }
    }
}

tasks.register<GenerateTask>("openapiGenerateLakeSharing") {
    generatorName.set("jaxrs-spec")
    inputSpec.set("$rootDir/docs/protocol/lake-sharing-protocol-api.yml")
    outputDir.set(openApiGeneratedSourcesAbsolutePath)
    additionalProperties.set(
        mapOf(
            "apiPackage" to "io.lake.sharing.api.server",
            "dateLibrary" to "java8",
            "disallowAdditionalPropertiesIfNotPresent" to "false",
            "generateBuilders" to "true",
            "generatePom" to "false",
            "interfaceOnly" to "true",
            "library" to "quarkus",
            "modelPackage" to "io.lake.sharing.api.server.model",
            "returnResponse" to "true",
            "supportAsync" to "true",
            "useJakartaEe" to "true",
            "useSwaggerAnnotations" to "false"
        )
    )
}


tasks.register<GenerateTask>("openapiGenerateDeltaSharing") {
    generatorName.set("jaxrs-spec")
    inputSpec.set("$rootDir/docs/protocol/delta-sharing-protocol-api.yml")
    outputDir.set(openApiGeneratedSourcesAbsolutePath)
    additionalProperties.set(
        mapOf(
            "apiPackage" to "io.delta.sharing.api.server",
            "dateLibrary" to "java8",
            "disallowAdditionalPropertiesIfNotPresent" to "false",
            "generateBuilders" to "true",
            "generatePom" to "false",
            "interfaceOnly" to "true",
            "library" to "quarkus",
            "modelPackage" to "io.delta.sharing.api.server.model",
            "returnResponse" to "true",
            "supportAsync" to "true",
            "useJakartaEe" to "true",
            "useSwaggerAnnotations" to "false"
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

tasks.quarkusBuild {
    nativeArgs {
        "additional-build-args" to "-H:-CheckToolchain"
    }
}

sourceSets {
    getByName("main") {
        java {
            srcDir("$openApiGeneratedSourcesAbsolutePath/src/gen/java")
        }
    }
}
spotless {
    java {
        targetExclude("$openApiGeneratedSourcesRelativeToProjectPath/**/*.java")
        importOrder()
        removeUnusedImports()
        cleanthat()          // has its own section below
        googleJavaFormat()   // has its own section below
        formatAnnotations()  // fixes formatting of type annotations, see below
    }
}