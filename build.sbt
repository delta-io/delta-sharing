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

val sparkVersion = "3.1.1"
val hadoopVersion = "2.7.2"

lazy val commonSettings = Seq(
  organization := "io.delta",
  scalaVersion := "2.12.10",
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
    "-Dspark.delta.sharing.client.sslTrustAll=true",
    "-Xmx1024m"
  )
)

lazy val releaseSettings = Seq(
  publishMavenStyle := true,
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

lazy val getInitialCommandsForConsole: Def.Initialize[String] = Def.settingDyn {
  val base = """ println("Welcome to\n" +
               |"      ____              __\n" +
               |"     / __/__  ___ _____/ /__\n" +
               |"    _\\ \\/ _ \\/ _ `/ __/  '_/\n" +
               |"   /___/ .__/\\_,_/_/ /_/\\_\\   version \"%s\"\n" +
               |"      /_/\n" +
               |"Using Scala \"%s\"\n")
               |
               |import org.apache.spark.SparkContext._
               |
               |val sc = {
               |  val conf = new org.apache.spark.SparkConf()
               |    .setMaster("local")
               |    .setAppName("Sbt console + Spark!")
               |    .set("spark.delta.sharing.client.sslTrustAll", "true")
               |  new org.apache.spark.SparkContext(conf)
               |}
               |sc.setLogLevel("WARN")
               |println("Created spark context as sc.")
    """.format(sparkVersion, scalaVersion.value).stripMargin
  if (libraryDependencies.value.map(_.name.contains("spark-sql")).reduce(_ || _)) {
    Def.setting {
      base +
        """val sqlContext = {
          |  val _sqlContext = new org.apache.spark.sql.SQLContext(sc)
          |  println("SQL context available as sqlContext.")
          |  _sqlContext
          |}
          |val spark = sqlContext.sparkSession
          |println("SparkSession available as spark.")
          |import sqlContext.implicits._
          |import sqlContext.sql
          |import org.apache.spark.sql.functions._
          """.stripMargin
    }
  } else {
    Def.setting(base)
  }
}

lazy val spark = (project in file("spark")) settings(
  name := "delta-sharing-spark",
  commonSettings,
  scalaStyleSettings,
  releaseSettings,
  initialCommands in console := getInitialCommandsForConsole.value,
  cleanupCommands in console := "sc.stop()",
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-catalyst" % sparkVersion % "test" classifier "tests",
    "org.apache.spark" %% "spark-core" % sparkVersion % "test" classifier "tests",
    "org.apache.spark" %% "spark-sql" % sparkVersion % "test" classifier "tests",
    "org.scalatest" %% "scalatest" % "3.2.3" % "test"
  )
)

lazy val server = (project in file("server")) settings(
  name := "delta-sharing-server",
  commonSettings,
  scalaStyleSettings,
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
    "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % "2.6.7",
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
    "org.scalatest" %% "scalatest" % "3.0.5" % "test",
    "net.sourceforge.argparse4j" % "argparse4j" % "0.9.0"
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
scalastyleConfig in ThisBuild := baseDirectory.value / "scalastyle-config.xml"

lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")
lazy val testScalastyle = taskKey[Unit]("testScalastyle")

lazy val scalaStyleSettings = Seq(
  compileScalastyle := scalastyle.in(Compile).toTask("").value,

  (compile in Compile) := ((compile in Compile) dependsOn compileScalastyle).value,

  testScalastyle := scalastyle.in(Test).toTask("").value,

  (test in Test) := ((test in Test) dependsOn testScalastyle).value
)
