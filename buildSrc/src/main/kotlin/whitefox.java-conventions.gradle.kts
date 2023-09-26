// Define Java conventions for this organization.
plugins {
    java
    id("com.palantir.git-version")
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
    // example for javac args
    // options.compilerArgs.add("-Xlint:deprecation")
}
spotless {
    java {
        importOrder()
        removeUnusedImports()
        cleanthat()
        palantirJavaFormat().style("GOOGLE")
        formatAnnotations()
    }
}
val gitVersion: groovy.lang.Closure<String> by extra
group = "io.whitefox"
version = gitVersion()