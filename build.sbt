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

val sparkVersion = "3.3.2"
val scala212 = "2.12.10"
val scala213 = "2.13.11"

lazy val commonSettings = Seq(
  organization := "io.delta",
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
    s"-Dazure.account.key=${sys.env.getOrElse("AZURE_TEST_ACCOUNT_KEY", "")}",
    "-Xmx1024m"
  )
)

lazy val root = (project in file(".")).aggregate(client, spark, server)

lazy val client = (project in file("client")) settings(
  name := "delta-sharing-client",
  crossScalaVersions := Seq(scala212, scala213),
  commonSettings,
  scalaStyleSettings,
  releaseSettings,
  libraryDependencies ++= Seq(
    "org.apache.httpcomponents" % "httpclient" % "4.5.14",
    "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-catalyst" % sparkVersion % "test" classifier "tests",
    "org.apache.spark" %% "spark-core" % sparkVersion % "test" classifier "tests",
    "org.apache.spark" %% "spark-sql" % sparkVersion % "test" classifier "tests",
    "org.scalatest" %% "scalatest" % "3.2.3" % "test",
    "org.scalatestplus" %% "mockito-4-11" % "3.2.18.0" % "test"
  ),
  Compile / sourceGenerators += Def.task {
    val file = (Compile / sourceManaged).value / "io" / "delta" / "sharing" / "client" / "package.scala"
    IO.write(file,
      s"""package io.delta.sharing
         |
         |package object client {
         |  val VERSION = "${version.value}"
         |}
         |""".stripMargin)
    Seq(file)
  }
)

lazy val spark = (project in file("spark")) dependsOn(client) settings(
  name := "delta-sharing-spark",
  crossScalaVersions := Seq(scala212, scala213),
  commonSettings,
  scalaStyleSettings,
  releaseSettings,
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-catalyst" % sparkVersion % "test" classifier "tests",
    "org.apache.spark" %% "spark-core" % sparkVersion % "test" classifier "tests",
    "org.apache.spark" %% "spark-sql" % sparkVersion % "test" classifier "tests",
    "org.scalatest" %% "scalatest" % "3.2.3" % "test"
  ),
  Compile / sourceGenerators += Def.task {
    val file = (Compile / sourceManaged).value / "io" / "delta" / "sharing" / "spark" / "package.scala"
    IO.write(file,
      s"""package io.delta.sharing
         |
         |package object spark {
         |  val VERSION = "${version.value}"
         |}
         |""".stripMargin)
    Seq(file)
  }
)

lazy val server = (project in file("server")) enablePlugins(JavaAppPackaging) settings(
  name := "delta-sharing-server",
  scalaVersion := scala212,
  commonSettings,
  scalaStyleSettings,
  releaseSettings,
  dockerUsername := Some("deltaio"),
  dockerBuildxPlatforms := Seq("linux/arm64", "linux/amd64"),
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
    "org.apache.hadoop" % "hadoop-aws" % "3.3.4" excludeAll(
      ExclusionRule("com.fasterxml.jackson.core"),
      ExclusionRule("com.fasterxml.jackson.module"),
      ExclusionRule("com.google.guava", "guava"),
      ExclusionRule("com.amazonaws", "aws-java-sdk-bundle")
    ),
    "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.189",
    "org.apache.hadoop" % "hadoop-azure" % "3.3.4" excludeAll(
      ExclusionRule("com.fasterxml.jackson.core"),
      ExclusionRule("com.fasterxml.jackson.module"),
      ExclusionRule("com.google.guava", "guava")
    ),
    "com.google.cloud" % "google-cloud-storage" % "2.2.2" excludeAll(
      ExclusionRule("com.fasterxml.jackson.core"),
      ExclusionRule("com.fasterxml.jackson.module")
    ),
    "com.google.cloud.bigdataoss" % "gcs-connector" % "hadoop2-2.2.4" excludeAll(
      ExclusionRule("com.fasterxml.jackson.core"),
      ExclusionRule("com.fasterxml.jackson.module")
    ),
    "org.apache.hadoop" % "hadoop-common" % "3.3.4" excludeAll(
      ExclusionRule("com.fasterxml.jackson.core"),
      ExclusionRule("com.fasterxml.jackson.module"),
      ExclusionRule("com.google.guava", "guava")
    ),
    "org.apache.hadoop" % "hadoop-client" % "3.3.4" excludeAll(
      ExclusionRule("com.fasterxml.jackson.core"),
      ExclusionRule("com.fasterxml.jackson.module"),
      ExclusionRule("com.google.guava", "guava")
    ),
    "org.apache.parquet" % "parquet-hadoop" % "1.12.3" excludeAll(
      ExclusionRule("com.fasterxml.jackson.core"),
      ExclusionRule("com.fasterxml.jackson.module"),
      ExclusionRule("com.google.guava", "guava")
    ),
    "io.delta" %% "delta-standalone" % "3.2.0" % "provided" excludeAll(
      ExclusionRule("com.fasterxml.jackson.core"),
      ExclusionRule("com.fasterxml.jackson.module"),
      ExclusionRule("com.google.guava", "guava")
    ),
    "io.delta" % "delta-kernel-api" % "3.2.0" excludeAll(
      ExclusionRule("com.fasterxml.jackson.core"),
      ExclusionRule("com.fasterxml.jackson.module"),
      ExclusionRule("com.google.guava", "guava")
    ),
    "io.delta" % "delta-kernel-defaults" % "3.2.0" excludeAll(
      ExclusionRule("com.fasterxml.jackson.core"),
      ExclusionRule("com.fasterxml.jackson.module"),
      ExclusionRule("com.google.guava", "guava")
    ),
    "org.apache.spark" %% "spark-sql" % "2.4.7" excludeAll(
      ExclusionRule("org.slf4j"),
      ExclusionRule("io.netty"),
      ExclusionRule("com.fasterxml.jackson.core"),
      ExclusionRule("com.fasterxml.jackson.module"),
      ExclusionRule("org.json4s"),
      ExclusionRule("com.google.guava", "guava")
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
  publishArtifact := true,
  Test / publishArtifact := false,

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
        <developer>
          <id>linzhou-db</id>
          <name>Lin Zhou</name>
          <url>https://github.com/linzhou-db</url>
        </developer>
        <developer>
          <id>chakankardb</id>
          <name>Abhijit Chakankar</name>
          <url>https://github.com/chakankardb</url>
        </developer>
        <developer>
          <id>charlenelyu-db</id>
          <name>Charlene Lyu</name>
          <url>https://github.com/charlenelyu-db</url>
        </developer>
        <developer>
          <id>zhuansunxt</id>
          <name>Xiaotong Sun</name>
          <url>https://github.com/zhuansunxt</url>
        </developer>
        <developer>
          <id>wchau</id>
          <name>William Chau</name>
          <url>https://github.com/wchau</url>
        </developer>
      </developers>
)

// Looks like some of release settings should be set for the root project as well.
publishArtifact := false  // Don't release the root project
publish := {}
publishTo := Some("snapshots" at "https://oss.sonatype.org/content/repositories/snapshots")
releaseCrossBuild := false
// crossScalaVersions must be set to Nil on the root project
crossScalaVersions := Nil
releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  releaseStepCommandAndRemaining("+publishSigned"),
  setNextVersion,
  commitNextVersion
)
