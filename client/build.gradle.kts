import org.openapitools.generator.gradle.plugin.tasks.GenerateTask

val openApiCodeGenDir = "generated/openapi"


val clientGeneratorProperties = mapOf(
    "apiPackage" to "io.whitefox.sharing.api.client",
    "invokerPackage" to "io.whitefox.sharing.api.utils",
    "modelPackage" to "io.whitefox.sharing.api.client.model",
    "dateLibrary" to "java8",
    "sourceFolder" to "src/gen/java",
    "openApiNullable" to "true",
    "annotationLibrary" to "none",
    "serializationLibrary" to "jackson",
    "useJakartaEe" to "true",
    "useRuntimeException" to "true"
)

plugins {
    java
    id("io.quarkus")
    id("whitefox.java-conventions")
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

tasks.register<GenerateTask>("openapiGenerateWhitefox") {
    generatorName.set("java")
    inputSpec.set("$rootDir/docs/protocol/whitefox-protocol-api.yml")
    library.set("native")
    outputDir.set(generatedCodeDirectory(layout, openApiCodeGenDir))
    additionalProperties.set(clientGeneratorProperties)
}

tasks.register<GenerateTask>("openapiGenerateDeltaSharing") {
    generatorName.set("java")
    inputSpec.set("$rootDir/docs/protocol/delta-sharing-protocol-api.yml")
    library.set("native")
    outputDir.set(generatedCodeDirectory(layout, openApiCodeGenDir))
    additionalProperties.set(clientGeneratorProperties)
}

tasks.withType<Test> {
    systemProperty("java.util.logging.manager", "org.jboss.logmanager.LogManager")
}
tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
    options.compilerArgs.add("-parameters")
    dependsOn(tasks.named("openapiGenerateWhitefox"), tasks.named("openapiGenerateDeltaSharing"))
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