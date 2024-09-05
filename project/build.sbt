// Mirrors aiq - project/build.sbt
resolvers += "Artifactory".at("https://actioniq.jfrog.io/artifactory/aiq-sbt-local/")

credentials ++= sys.env.get("ARTIFACTORY_ACCESS_TOKEN").toList.map { token =>
  Credentials("Artifactory Realm", "actioniq.jfrog.io", sys.env("ARTIFACTORY_USER"), token)
}
