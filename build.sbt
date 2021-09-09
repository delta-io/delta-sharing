/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import sbt.ExclusionRule

ThisBuild / parallelExecution := false

val sparkVersion = "3.1.1"

lazy val commonSettings = Seq(
  organization := "io.delta",
  scalaVersion := "2.12.10",
  fork := true,
  javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
  scalacOptions += "-target:jvm-1.8",
  // Configurations to speed up tests and reduce memory footprint
  Test / javaOptions ++= Seq(
    "-Dspark.ui.enabled=false",
    "-Dspark.ui.showConsoleProgress=false",
    "-Dspark.databricks.delta.snapshotPartitions=2",
    "-Dspark.sql.shuffle.partitions=5",
    "-Dspark.sql.sources.parallelPartitionDiscovery.parallelism=5",
    "-Dspark.delta.sharing.network.sslTrustAll=true",
    "-Xmx1024m"
  )
)

lazy val root = (project in file(".")).aggregate(spark, server)

lazy val spark = (project in file("spark")) settings(
  name := "delta-sharing-spark",
  commonSettings,
  scalaStyleSettings,
  releaseSettings,
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-catalyst" % sparkVersion % "test" classifier "tests",
    "org.apache.spark" %% "spark-core" % sparkVersion % "test" classifier "tests",
    "org.apache.spark" %% "spark-sql" % sparkVersion % "test" classifier "tests",
    "org.scalatest" %% "scalatest" % "3.2.3" % "test"
  )
)

lazy val server = (project in file("server")) enablePlugins(JavaAppPackaging) settings(
  name := "delta-sharing-server",
  commonSettings,
  scalaStyleSettings,
  releaseSettings,
  dockerUsername := Some("deltaio"),
  scriptClasspath ++= Seq("../conf"),
  libraryDependencies ++= Seq(
    // Pin versions for jackson libraries as the new version of `jackson-module-scala` introduces a
    // breaking change making us not able to use `delta-standalone`.
    "com.fasterxml.jackson.core" % "jackson-core" % "2.6.7",
    "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7.3",
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.6.7.1",
    "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % "2.6.7",
    "org.json4s" %% "json4s-jackson" % "3.5.3" excludeAll(
      ExclusionRule("com.fasterxml.jackson.core"),
      ExclusionRule("com.fasterxml.jackson.module")
    ),
    "com.linecorp.armeria" %% "armeria-scalapb" % "1.6.0" excludeAll(
      ExclusionRule("com.fasterxml.jackson.core"),
      ExclusionRule("com.fasterxml.jackson.module"),
      ExclusionRule("org.json4s")
    ),
    "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf" excludeAll(
      ExclusionRule("com.fasterxml.jackson.core"),
      ExclusionRule("com.fasterxml.jackson.module"),
      ExclusionRule("org.json4s")
    ),
    "org.apache.hadoop" % "hadoop-aws" % "2.10.1" excludeAll(
      ExclusionRule("com.fasterxml.jackson.core"),
      ExclusionRule("com.fasterxml.jackson.module")
    ),
    "org.apache.hadoop" % "hadoop-azure" % "2.10.1" excludeAll(
      ExclusionRule("com.fasterxml.jackson.core"),
      ExclusionRule("com.fasterxml.jackson.module")
    ),
    "org.apache.hadoop" % "hadoop-common" % "2.10.1" excludeAll(
      ExclusionRule("com.fasterxml.jackson.core"),
      ExclusionRule("com.fasterxml.jackson.module")
    ),
    "org.apache.hadoop" % "hadoop-client" % "2.10.1" excludeAll(
      ExclusionRule("com.fasterxml.jackson.core"),
      ExclusionRule("com.fasterxml.jackson.module")
    ),
    "org.apache.parquet" % "parquet-hadoop" % "1.10.1" excludeAll(
      ExclusionRule("com.fasterxml.jackson.core"),
      ExclusionRule("com.fasterxml.jackson.module")
    ),
    "io.delta" %% "delta-standalone" % "0.2.0" excludeAll(
      ExclusionRule("com.fasterxml.jackson.core"),
      ExclusionRule("com.fasterxml.jackson.module")
    ),
    "org.apache.spark" %% "spark-sql" % "2.4.7" excludeAll(
      ExclusionRule("org.slf4j"),
      ExclusionRule("io.netty"),
      ExclusionRule("com.fasterxml.jackson.core"),
      ExclusionRule("com.fasterxml.jackson.module"),
      ExclusionRule("org.json4s")
    ),
    "org.slf4j" % "slf4j-api" % "1.6.1",
    "org.slf4j" % "slf4j-simple" % "1.6.1",
    "net.sourceforge.argparse4j" % "argparse4j" % "0.9.0",

    "org.scalatest" %% "scalatest" % "3.0.5" % "test"
  ),
  Compile / PB.targets := Seq(
    scalapb.gen() -> (Compile / sourceManaged).value / "scalapb"
  )
)

/*
 ***********************
 * ScalaStyle settings *
 ***********************
 */
ThisBuild / scalastyleConfig := baseDirectory.value / "scalastyle-config.xml"

lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")
lazy val testScalastyle = taskKey[Unit]("testScalastyle")

lazy val scalaStyleSettings = Seq(
  compileScalastyle := (Compile / scalastyle).toTask("").value,
  (Compile / compile) := ((Compile / compile) dependsOn compileScalastyle).value,
  testScalastyle := (Test / scalastyle).toTask("").value,
  (Test / test) := ((Test / test) dependsOn testScalastyle).value
)

/*
 ********************
 * Release settings *
 ********************
 */
import ReleaseTransformations._

lazy val releaseSettings = Seq(
  publishMavenStyle := true,

  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (isSnapshot.value) {
      Some("snapshots" at nexus + "content/repositories/snapshots")
    } else {
      Some("releases"  at nexus + "service/local/staging/deploy/maven2")
    }
  },

  releasePublishArtifactsAction := PgpKeys.publishSigned.value,

  releaseCrossBuild := true,

  releaseProcess := Seq[ReleaseStep](
    checkSnapshotDependencies,
    inquireVersions,
    runTest,
    setReleaseVersion,
    commitReleaseVersion,
    tagRelease,
    releaseStepCommandAndRemaining("+publishLocalSigned"),
    setNextVersion,
    commitNextVersion
  ),

  licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0")),

  pomExtra :=
    <url>https://github.com/delta-io/delta-sharing</url>
      <scm>
        <url>git@github.com:delta-io/delta-sharing.git</url>
        <connection>scm:git:git@github.com:delta-io/delta-sharing.git</connection>
      </scm>
      <developers>
        <developer>
          <id>marmbrus</id>
          <name>Michael Armbrust</name>
          <url>https://github.com/marmbrus</url>
        </developer>
        <developer>
          <id>jose-torres</id>
          <name>Jose Torres</name>
          <url>https://github.com/jose-torres</url>
        </developer>
        <developer>
          <id>ueshin</id>
          <name>Takuya UESHIN</name>
          <url>https://github.com/ueshin</url>
        </developer>
        <developer>
          <id>mateiz</id>
          <name>Matei Zaharia</name>
          <url>https://github.com/mateiz</url>
        </developer>
        <developer>
          <id>zsxwing</id>
          <name>Shixiong Zhu</name>
          <url>https://github.com/zsxwing</url>
        </developer>
      </developers>
)

// Looks like some of release settings should be set for the root project as well.
publishArtifact := false  // Don't release the root project
publish := {}
publishTo := Some("snapshots" at "https://oss.sonatype.org/content/repositories/snapshots")
releaseCrossBuild := false
