import sbt.Keys.{isSnapshot, packageDoc, packageSrc, publishArtifact, publishConfiguration, publishLocalConfiguration, publishM2Configuration, publishMavenStyle, publishTo}
import sbt.{Def, _}

// Mirrors aiq - PublishToArtifactory
object PublishToArtifactory extends AutoPlugin {

  lazy val baseSettings: Seq[Def.Setting[_]] = Seq(
    publishTo := Some("Artifactory Realm".at("https://actioniq.jfrog.io/artifactory/aiq-sbt-local")),
    publishMavenStyle := true,
    publishConfiguration := publishConfiguration.value.withOverwrite(isSnapshot.value),
    publishLocalConfiguration := publishLocalConfiguration.value.withOverwrite(isSnapshot.value),
    publishM2Configuration := publishM2Configuration.value.withOverwrite(isSnapshot.value),
    Compile / packageSrc / publishArtifact := false,
    Compile / packageDoc / publishArtifact := false,
  )

  override def requires: Plugins = ArtifactoryCredentials

  override def trigger: PluginTrigger = noTrigger

  // a group of settings that are automatically added to projects.
  override val projectSettings: Seq[Def.Setting[_]] = baseSettings

}
