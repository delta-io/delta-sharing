import org.gradle.api.file.ProjectLayout
import org.gradle.internal.os.OperatingSystem

fun generatedCodeDirectory(
    layout: ProjectLayout,
    openApiGeneratedSourcesRelativeToBuildPath: String
): String {
    return "${layout.buildDirectory.get()}/${openApiGeneratedSourcesRelativeToBuildPath}"
}

fun relativeGeneratedCodeDirectory(
    layout: ProjectLayout,
    openApiGeneratedSourcesRelativeToBuildPath: String
): String {
    return "${layout.buildDirectory.get().asFile.toRelativeString(layout.projectDirectory.asFile)}/${openApiGeneratedSourcesRelativeToBuildPath}"
}

fun isWindowsBuild(): Boolean {
// TODO The implementation of this function uses an internal Gradle feature
//      this may become deprecated.
//      This was the only clean solution that seems to consistently work
//      for now.
    return OperatingSystem.current().isWindows()
} // end of isWindowsBuild