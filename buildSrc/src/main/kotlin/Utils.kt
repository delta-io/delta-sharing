import org.gradle.api.file.ProjectLayout

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
