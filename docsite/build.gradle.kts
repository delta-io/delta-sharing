import com.github.gradle.node.npm.task.NpmTask

abstract class KillAllDocusaurus : DefaultTask() {
    @TaskAction
    fun destroy() {
        ProcessHandle
            .allProcesses()
            .filter { p -> p.info().commandLine().orElse("").contains("docusaurus start") }
            .forEach(ProcessHandle::destroy)
    }
}

plugins {
    id("com.github.node-gradle.node") version("7.0.1")
}

node {
    // Whether to download and install a specific Node.js version or not
    // If false, it will use the globally installed Node.js
    // If true, it will download node using above parameters
    // Note that npm is bundled with Node.js
    download = true
    // Version of node to download and install (only used if download is true)
    // It will be unpacked in the workDir
    version = "20.9.0"
}

tasks.register<KillAllDocusaurus>("killAllDocusaurus")
