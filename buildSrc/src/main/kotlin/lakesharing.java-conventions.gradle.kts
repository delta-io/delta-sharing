// Define Java conventions for this organization.
plugins {
    java
    id("org.openapi.generator")
    id("com.diffplug.spotless")
}
// Projects should use Maven Central for external dependencies
repositories {
    mavenCentral()
    mavenLocal()
}

// Enable deprecation messages when compiling Java code
tasks.withType<JavaCompile>().configureEach {
    options.compilerArgs.add("-Xlint:deprecation")
}
spotless {
    java {
        importOrder()
        removeUnusedImports()
        cleanthat()          // has its own section below
        googleJavaFormat()   // has its own section below
        formatAnnotations()  // fixes formatting of type annotations, see below
    }
}