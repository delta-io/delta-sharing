import org.gradle.api.tasks.testing.logging.TestExceptionFormat

// Define Java conventions for this organization.
plugins {
    java
    jacoco
    id("com.palantir.git-version")
    id("org.openapi.generator")
    id("com.diffplug.spotless")
    id("io.freefair.lombok")
}
// Projects should use Maven Central for external dependencies
repositories {
    mavenCentral()
    mavenLocal()
}

// region java compile

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }
}

// Enable deprecation messages when compiling Java code
tasks.withType<JavaCompile>().configureEach {
    // example for javac args
    // options.compilerArgs.add("-Xlint:deprecation")
}

// endregion


tasks.register("devCheck") {
    dependsOn(tasks.spotlessApply)
    finalizedBy(tasks.check)
    description = "Useful command when iterating locally to apply spotless formatting then running all the checks"
}

// region setup tests

tasks.withType<Test> {
    systemProperty("java.util.logging.manager", "org.jboss.logmanager.LogManager")
    testLogging {
        exceptionFormat = TestExceptionFormat.FULL
    }
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

tasks.check {
    finalizedBy(tasks.jacocoTestReport)
    dependsOn(tasks.test, integrationTest)
}

// endregion

// region setup test coverage

jacoco {
    toolVersion = "0.8.12"
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
}
// endregion
spotless {
    java {
        importOrder()
        removeUnusedImports()
        palantirJavaFormat().style("GOOGLE")
        formatAnnotations()
    }
}

val gitVersion: groovy.lang.Closure<String> by extra
group = "io.whitefox"
version = gitVersion()