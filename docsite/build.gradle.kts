import com.github.gradle.node.npm.task.NpmTask
import java.util.stream.Collectors

abstract class KillAllDocusaurus : DefaultTask() {
    @TaskAction
    fun destroy() {
        project.logger.lifecycle("Killing ALL Docusaurus processes")
        val docusarusProcesses =
        ProcessHandle
            .allProcesses()
            .filter { p -> p.info().commandLine().orElse("").contains("docusaurus start") }
            .collect(Collectors.toList())
        project.logger.lifecycle("Found ${docusarusProcesses.size} processes to kill")
        docusarusProcesses
            .forEach { p ->
                project.logger.lifecycle("Killing Docusaurus process ${p.pid()}")
                p.destroy()
            }
    }
}

plugins {
    id("com.github.node-gradle.node") version("7.0.2")
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

val killAllDocusaurus = tasks.register<KillAllDocusaurus>("killAllDocusaurus")

tasks.findByName("npm_run_start")?.finalizedBy(killAllDocusaurus)
