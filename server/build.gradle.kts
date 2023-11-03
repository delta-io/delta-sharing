import org.gradle.api.tasks.testing.logging.TestExceptionFormat
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
val hadoopVersion = "3.3.6"
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
    implementation(String.format("org.apache.hadoop:hadoop-common:%s", hadoopVersion))

    // TEST
    testImplementation("io.quarkus:quarkus-junit5")
    testImplementation("io.rest-assured:rest-assured")
    testImplementation("io.rest-assured:json-path")
    testImplementation("org.openapi4j:openapi-operation-validator:1.0.7")
    testImplementation("org.openapi4j:openapi-operation-restassured:1.0.7")

    //AWS
    compileOnly("com.amazonaws:aws-java-sdk-bom:1.12.367")
    compileOnly("com.amazonaws:aws-java-sdk-s3:1.12.367")
    implementation(String.format("org.apache.hadoop:hadoop-aws:%s", hadoopVersion))

}

// endregion

// region openapi code generation

val openApiCodeGenDir = "generated/openapi"

val serverGeneratorProperties = mapOf(
    "dateLibrary" to "java8",
    "disallowAdditionalPropertiesIfNotPresent" to "false",
    "generateBuilders" to "false",
    "generatePom" to "false",
    "interfaceOnly" to "true",
    "library" to "quarkus",
    "returnResponse" to "true",
    "supportAsync" to "false",
    "useJakartaEe" to "true",
    "useSwaggerAnnotations" to "false",
    "invokerPackage" to "ignored",
    "additionalModelTypeAnnotations" to "@io.whitefox.annotations.SkipCoverageGenerated;",
    "additionalEnumTypeAnnotations" to "@io.whitefox.annotations.SkipCoverageGenerated;",
    "additionalOneOfTypeAnnotations" to "@io.whitefox.annotations.SkipCoverageGenerated;"
)

val generatedCodeDirectory = generatedCodeDirectory(layout, openApiCodeGenDir)

val openapiGenerateWhitefox = tasks.register<GenerateTask>("openapiGenerateWhitefox") {
    generatorName.set("jaxrs-spec")
    inputSpec.set("$rootDir/protocol/whitefox-protocol-api.yml")
    outputDir.set(generatedCodeDirectory)
    additionalProperties.set(
        serverGeneratorProperties.plus(
            mapOf(
                "apiPackage" to "io.whitefox.api.server.v1.generated",
                "modelPackage" to "io.whitefox.api.model.v1.generated",
                "useTags" to "true",
            )
        )
    )
}

val openapiGenerateDeltaSharing = tasks.register<GenerateTask>("openapiGenerateDeltaSharing") {
    generatorName.set("jaxrs-spec")
    inputSpec.set("$rootDir/protocol/delta-sharing-protocol-api.yml")
    outputDir.set(generatedCodeDirectory)
    additionalProperties.set(
        serverGeneratorProperties + mapOf(
            "apiPackage" to "io.whitefox.api.deltasharing.server.v1.generated",
            "modelPackage" to "io.whitefox.api.deltasharing.model.v1.generated",
            "useTags" to "false",
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

tasks.register("devCheck") {
    dependsOn(tasks.spotlessApply)
    finalizedBy(tasks.check)
    description = "Useful command when iterating locally to apply spotless formatting then running all the checks"
}
tasks.withType<Test> {
    systemProperty("java.util.logging.manager", "org.jboss.logmanager.LogManager")
    testLogging {
        exceptionFormat = TestExceptionFormat.FULL
    }
    environment = env.allVariables
}

tasks.test {
    description = "Runs Unit Tests"
    useJUnitPlatform {
        excludeTags("integration","aws")
    }
}

val integrationTest = tasks.register<Test>("integrationTest") {
    description = "Runs Integration Tests"
    useJUnitPlatform {
        includeTags("integration", "aws")
    }
}

// endregion

// region code coverage

tasks.check {
    finalizedBy(tasks.jacocoTestReport)
    dependsOn(tasks.test, integrationTest)
}

tasks.jacocoTestReport {
    dependsOn(tasks.test, integrationTest) // tests are required to run before generating the report
    executionData(fileTree(layout.buildDirectory).include("/jacoco/*.exec").exclude("/jacoco/testNative.exec"))
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

val classesToExclude = listOf(
    "**" + File.separator + "generated" + File.separator + "**.class",
    "**" + File.separator + "ignored" + File.separator + "**.class"
)

tasks.jacocoTestCoverageVerification {
    executionData(fileTree(layout.buildDirectory).include("/jacoco/*.exec").exclude("/jacoco/testNative.exec"))
    classDirectories.setFrom(
        files(classDirectories.files.map { fileTree(it) { exclude(classesToExclude) } })
    )
    if (!isWindowsBuild()) {
        violationRules {
            rule {
                limit {
                    minimum = BigDecimal.valueOf(0.81)
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
    System.setProperty("quarkus.container-image.registry", "ghcr.io")
    System.setProperty("quarkus.container-image.group", "agile-lab-dev")
    System.setProperty("quarkus.container-image.name", project.group.toString() + "." + project.name)
    System.setProperty("quarkus.native.additional-build-args", "-H:-CheckToolchain,--enable-preview")
}

// endregion