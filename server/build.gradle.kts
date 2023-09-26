import org.openapitools.generator.gradle.plugin.tasks.GenerateTask

val openApiCodeGenDir = "generated/openapi"

val serverGeneratorProperties = mapOf(
    "dateLibrary" to "java8",
    "disallowAdditionalPropertiesIfNotPresent" to "false",
    "generateBuilders" to "true",
    "generatePom" to "false",
    "interfaceOnly" to "true",
    "library" to "quarkus",
    "returnResponse" to "true",
    "supportAsync" to "true",
    "useJakartaEe" to "true",
    "useSwaggerAnnotations" to "false"
)
plugins {
    java
    id("io.quarkus")
    id("whitefox.java-conventions")
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

tasks.register<GenerateTask>("openapiGenerateWhitefox") {
    generatorName.set("jaxrs-spec")
    inputSpec.set("$rootDir/docs/protocol/whitefox-protocol-api.yml")
    outputDir.set(generatedCodeDirectory(layout, openApiCodeGenDir))
    additionalProperties.set(
        serverGeneratorProperties.plus(
            mapOf(
                "apiPackage" to "io.whitefox.sharing.api.server",
                "modelPackage" to "io.whitefox.sharing.api.server.model",
            )
        )
    )
}


tasks.register<GenerateTask>("openapiGenerateDeltaSharing") {
    generatorName.set("jaxrs-spec")
    inputSpec.set("$rootDir/docs/protocol/delta-sharing-protocol-api.yml")
    outputDir.set(generatedCodeDirectory(layout, openApiCodeGenDir))
    additionalProperties.set(
        serverGeneratorProperties + mapOf(
            "apiPackage" to "io.delta.sharing.api.server",
            "modelPackage" to "io.delta.sharing.api.server.model",
        )
    )
}

tasks.withType<Test> {
    systemProperty("java.util.logging.manager", "org.jboss.logmanager.LogManager")
}
tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
    options.compilerArgs.add("-parameters")
    dependsOn(tasks.named("openapiGenerateWhitefox"), tasks.named("openapiGenerateDeltaSharing"))
}

tasks.quarkusBuild {
    nativeArgs {
        "additional-build-args" to "-H:-CheckToolchain"
    }
}

buildscript {
    configurations.all {
        resolutionStrategy {
            force("org.yaml:snakeyaml:1.33")
        }
    }
}
spotless {
    java {
        targetExclude("${relativeGeneratedCodeDirectory(layout, openApiCodeGenDir)}/**/*.java")
    }
}

sourceSets {
    getByName("main") {
        java {
            srcDir("${generatedCodeDirectory(layout, openApiCodeGenDir)}/src/gen/java")
        }
    }
}