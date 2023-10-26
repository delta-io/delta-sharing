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
        id("com.palantir.git-version") version "3.0.0"
        id("com.diffplug.spotless") version "6.22.0"
    }
}