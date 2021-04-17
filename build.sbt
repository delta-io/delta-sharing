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

import ReleaseTransformations._
import sbt.ExclusionRule

parallelExecution in ThisBuild := false
crossScalaVersions in ThisBuild := Seq("2.12.8", "2.11.12")

lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")
lazy val testScalastyle = taskKey[Unit]("testScalastyle")

val sparkVersion = "3.0.2"
val hadoopVersion = "2.7.2"
val deltaVersion = "0.5.0"

lazy val commonSettings = Seq(
  organization := "io.delta",
  scalaVersion := "2.12.8",
  fork := true,
  javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
  scalacOptions += "-target:jvm-1.8",
  // Configurations to speed up tests and reduce memory footprint
  javaOptions in Test ++= Seq(
    "-Dspark.ui.enabled=false",
    "-Dspark.ui.showConsoleProgress=false",
    "-Dspark.databricks.delta.snapshotPartitions=2",
    "-Dspark.sql.shuffle.partitions=5",
    "-Ddelta.log.cacheSize=3",
    "-Dspark.sql.sources.parallelPartitionDiscovery.parallelism=5",
    "-Xmx1024m"
  )
)

lazy val releaseSettings = Seq(
  publishMavenStyle := true,
  releaseCrossBuild := true,
  licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0")),
  pomExtra :=
    <url>https://github.com/delta-io/delta-exchange</url>
      <scm>
        <url>git@github.com:delta-io/delta-exchange.git</url>
        <connection>scm:git:git@github.com:delta-io/delta-exchange.git</connection>
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
          <id>zsxwing</id>
          <name>Shixiong Zhu</name>
          <url>https://github.com/zsxwing</url>
        </developer>
      </developers>,
  bintrayOrganization := Some("delta-io"),
  bintrayRepository := "delta",
  releaseProcess := Seq[ReleaseStep](
    checkSnapshotDependencies,
    inquireVersions,
    runTest,
    setReleaseVersion,
    commitReleaseVersion,
    tagRelease,
    publishArtifacts,
    setNextVersion,
    commitNextVersion
  )
)

lazy val skipReleaseSettings = Seq(
  publishArtifact := false,
  publish := ()
)

// Don't release the root project
publishArtifact := false

publish := ()

// Looks some of release settings should be set for the root project as well.
releaseCrossBuild := true

releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  publishArtifacts,
  setNextVersion,
  commitNextVersion
)

lazy val root = (project in file("."))
  .aggregate(spark, server)

lazy val spark = (project in file("spark")) enablePlugins(Antlr4Plugin)  settings(
  name := "delta-exchange-spark",
  commonSettings,
  releaseSettings,
  antlr4Version in Antlr4 := "4.7",
  antlr4PackageName in Antlr4 := Some("io.delta.exchange.sql.parser"),
  antlr4GenListener in Antlr4 := true,
  antlr4GenVisitor in Antlr4 := true,
  libraryDependencies ++= Seq(
    "org.apache.parquet" % "parquet-hadoop" % "1.10.1" % "provided",
    "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
    "io.delta" %% "delta-core" % "0.8.0",
    "org.apache.spark" %% "spark-catalyst" % sparkVersion % "test" classifier "tests",
    "org.apache.spark" %% "spark-core" % sparkVersion % "test" classifier "tests",
    "org.apache.spark" %% "spark-sql" % sparkVersion % "test" classifier "tests",
    "org.scalatest" %% "scalatest" % "3.0.5" % "test",

    "org.apache.hadoop" % "hadoop-client" % "2.10.1",
    "org.apache.hadoop" % "hadoop-common" % "2.10.1",
    "org.apache.hadoop" % "hadoop-aws" % "2.10.1",
    "org.apache.hadoop" % "hadoop-azure" % "2.10.1",

    "com.google.cloud" % "google-cloud-storage" % "1.113.14",
    "com.google.cloud.bigdataoss" % "gcs-connector" % "hadoop2-2.2.0"
  )
)

lazy val server = (project in file("server")) settings(
  name := "delta-exchange-server",
  commonSettings,
  releaseSettings,
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-sql" % "2.4.7" excludeAll(
      ExclusionRule("org.slf4j"),
      ExclusionRule("io.netty")
    ),
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.6.7.1",
    "org.json4s" %% "json4s-jackson" % "3.5.3" excludeAll(
      ExclusionRule("com.fasterxml.jackson.core"),
      ExclusionRule("com.fasterxml.jackson.module")
    ),
    "com.fasterxml.jackson.core" % "jackson-core" % "2.6.7",
    "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7.3",
    "com.linecorp.armeria" %% "armeria-scalapb" % "1.6.0" excludeAll(
      ExclusionRule("com.fasterxml.jackson.core"),
      ExclusionRule("com.fasterxml.jackson.module")
    ),
    "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf" excludeAll(
      ExclusionRule("com.fasterxml.jackson.core"),
      ExclusionRule("com.fasterxml.jackson.module")
    ),
    "org.apache.hadoop" % "hadoop-aws" % "2.10.1" excludeAll(
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
    "org.slf4j" % "slf4j-api" % "1.6.1" excludeAll(
      ExclusionRule("com.fasterxml.jackson.core"),
      ExclusionRule("com.fasterxml.jackson.module")
    ),
    "org.slf4j" % "slf4j-simple" % "1.6.1" excludeAll(
      ExclusionRule("com.fasterxml.jackson.core"),
      ExclusionRule("com.fasterxml.jackson.module")
    ),
    "io.delta" %% "delta-standalone" % "0.2.0" excludeAll(
      ExclusionRule("com.fasterxml.jackson.core"),
      ExclusionRule("com.fasterxml.jackson.module")
    ),
    "com.fasterxml.jackson.core" % "jackson-core" % "2.6.7",
    "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7.3",
    "org.scalatest" %% "scalatest" % "3.0.5" % "test"
  ),
  Compile / PB.targets := Seq(
    scalapb.gen() -> (Compile / sourceManaged).value / "scalapb"
  )
)
