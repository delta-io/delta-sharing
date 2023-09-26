rootProject.name = "whitefox-multiproject"
include("client")
include("server")

pluginManagement {
    val quarkusPluginVersion: String by settings
    val quarkusPluginId: String by settings
    repositories {
        mavenCentral()
        gradlePluginPortal()
        mavenLocal()
    }
    plugins {
        id(quarkusPluginId) version quarkusPluginVersion
        id("org.openapi.generator") version "6.6.0"
        id("com.palantir.git-version") version "3.0.0"
        id("com.diffplug.spotless") version "6.21.0"
    }
}