import org.openapitools.generator.gradle.plugin.tasks.GenerateTask

plugins {
    java
    id("io.quarkus")
    id("whitefox.java-conventions")
}

val quarkusPlatformGroupId: String by project
val quarkusPlatformArtifactId: String by project
val quarkusPlatformVersion: String by project

// region dependencies

dependencies {
    implementation(enforcedPlatform("${quarkusPlatformGroupId}:${quarkusPlatformArtifactId}:${quarkusPlatformVersion}"))
    // QUARKUS
    implementation("io.quarkus:quarkus-container-image-docker")
    implementation("io.quarkus:quarkus-resteasy-reactive")
    implementation("io.quarkus:quarkus-resteasy-reactive-jackson")
    implementation("io.quarkus:quarkus-arc")
    implementation("org.openapitools:jackson-databind-nullable:0.2.6")
    implementation("org.eclipse.microprofile.openapi:microprofile-openapi-api:3.1.1")
    implementation("jakarta.validation:jakarta.validation-api:3.0.2")

    // DELTA
    implementation("io.delta:delta-standalone_2.13:0.6.0")
    implementation("org.apache.hadoop:hadoop-common:3.3.6")

    // TEST
    testImplementation("io.quarkus:quarkus-junit5")
    testImplementation("io.rest-assured:rest-assured")
    testImplementation("io.rest-assured:json-path")
    testImplementation("org.openapi4j:openapi-operation-validator:1.0.7")
    testImplementation("org.openapi4j:openapi-operation-restassured:1.0.7")
}

// endregion

// region openapi code generation

val openApiCodeGenDir = "generated/openapi"

val serverGeneratorProperties = mapOf(
    "dateLibrary" to "java8",
    "disallowAdditionalPropertiesIfNotPresent" to "false",
    "generateBuilders" to "true",
    "generatePom" to "false",
    "interfaceOnly" to "true",
    "library" to "quarkus",
    "returnResponse" to "true",
    "supportAsync" to "false",
    "useJakartaEe" to "true",
    "useSwaggerAnnotations" to "false"
)

val generatedCodeDirectory = generatedCodeDirectory(layout, openApiCodeGenDir)

val openapiGenerateWhitefox = tasks.register<GenerateTask>("openapiGenerateWhitefox") {
    generatorName.set("jaxrs-spec")
    inputSpec.set("$rootDir/docs/protocol/whitefox-protocol-api.yml")
    outputDir.set(generatedCodeDirectory)
    additionalProperties.set(
        serverGeneratorProperties.plus(
            mapOf(
                "apiPackage" to "io.whitefox.api.server.generated",
                "modelPackage" to "io.whitefox.api.model.generated",
                "invokerPackage" to "ignored",
            )
        )
    )
}

val openapiGenerateDeltaSharing = tasks.register<GenerateTask>("openapiGenerateDeltaSharing") {
    generatorName.set("jaxrs-spec")
    inputSpec.set("$rootDir/docs/protocol/delta-sharing-protocol-api.yml")
    outputDir.set(generatedCodeDirectory)
    additionalProperties.set(
        serverGeneratorProperties + mapOf(
            "apiPackage" to "io.whitefox.api.deltasharing.server.generated",
            "modelPackage" to "io.whitefox.api.deltasharing.model.generated",
            "invokerPackage" to "ignored",
        )
    )
}

// endregion

// region java compile

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
    options.compilerArgs.add("-parameters")
    dependsOn(openapiGenerateWhitefox, openapiGenerateDeltaSharing)
}

sourceSets {
    getByName("main") {
        java {
            srcDir("${generatedCodeDirectory(layout, openApiCodeGenDir)}/src/gen/java")
        }
    }
}

// endregion

// region test running

tasks.check {
    dependsOn(deltaTest)
}

tasks.withType<Test> {
    systemProperty("java.util.logging.manager", "org.jboss.logmanager.LogManager")
}

val deltaTestClasses =
    listOf("io.whitefox.api.deltasharing.DeltaSharedTableTest.*", "io.whitefox.services.DeltaLogServiceTest.*")

tasks.test {
    description = "Runs all other test classes by not forking the jvm."
    filter {
        deltaTestClasses.forEach { s ->
            excludeTestsMatching(s)
        }
    }
    forkEvery = 0
}
val deltaTest = tasks.register<Test>("deltaTest") {
    description = "Runs delta test classes by forking the jvm."
    group = "verification"
    filter {
        deltaTestClasses.forEach { s ->
            includeTestsMatching(s)
        }
    }
    forkEvery = 0
}

// endregion

// region code coverage

tasks.check {
    finalizedBy(tasks.jacocoTestReport)
}


val classesToExclude = listOf(
    "**" + File.separator + "generated" + File.separator + "**.class",
    "**" + File.separator + "ignored" + File.separator + "**.class"
)
tasks.jacocoTestReport {
    doFirst {
        logger.lifecycle("Excluding generated classes: ${classesToExclude}")
    }
    classDirectories.setFrom(
        files(classDirectories.files.map { fileTree(it) { exclude(classesToExclude) } })
    )
    doLast {
        logger.lifecycle("The report can be found at: file://" + reports.html.entryPoint)
    }

    finalizedBy(tasks.jacocoTestCoverageVerification)
}

tasks.jacocoTestCoverageVerification {
    classDirectories.setFrom(
        files(classDirectories.files.map { fileTree(it) { exclude(classesToExclude) } })
    )
    if (!isWindowsBuild()) {
        violationRules {
            rule {
                limit {
                    minimum = BigDecimal.valueOf(0.60)
                }
            }
        }
    }
}

// endregion

// region code formatting

spotless {
    java {
        targetExclude("${relativeGeneratedCodeDirectory(layout, openApiCodeGenDir)}/**/*.java")
    }
}

// endregion

// region container image

tasks.quarkusBuild {
    System.setProperty("quarkus.container-image.group", project.group.toString())
    System.setProperty("quarkus.container-image.additional-tags", "latest")
    System.setProperty("quarkus.native.additional-build-args", "-H:-CheckToolchain,--enable-preview")
}

// endregion