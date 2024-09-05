import sbt.Keys.credentials
import sbt.{AutoPlugin, Credentials, Def, PluginTrigger}

// Mirrors aiq - ArtifactoryCredentials
object ArtifactoryCredentials extends AutoPlugin {

  lazy val credentialSettings: Seq[Def.Setting[_]] = Seq(
    // Attempts to use environment variables if available to configure Artifactory
    credentials ++= sys.env.get("ARTIFACTORY_ACCESS_TOKEN").toList.map { token =>
      Credentials("Artifactory Realm", "actioniq.jfrog.io", sys.env("ARTIFACTORY_USER"), token)
    }
  )

  // Applying this to all builds automatically for now. trigger = allRequirements with no requirements
  override def trigger: PluginTrigger = allRequirements

  override def projectSettings: Seq[Def.Setting[_]] = credentialSettings
}
