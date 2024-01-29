plugins {
    `kotlin-dsl`
}

repositories {
    gradlePluginPortal() // so that external plugins can be resolved in dependencies section
}

dependencies {
    implementation("org.openapi.generator:org.openapi.generator.gradle.plugin:6.6.0")
    implementation("com.diffplug.spotless:spotless-plugin-gradle:6.25.0")
    implementation("com.palantir.gradle.gitversion:gradle-git-version:3.0.0")
    implementation("io.freefair.lombok:io.freefair.lombok.gradle.plugin:8.4")
}