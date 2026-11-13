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

package io.delta.sharing.client

import java.io.File
import java.nio.file.Files
import java.util.concurrent.{CountDownLatch, TimeUnit}

import scala.sys.process._
import scala.util.Try

import org.apache.commons.io.FileUtils

/**
 * Launches the isolated OpenSharing test server (`opensharingTestServer / runMain`) used by
 * client and spark integration tests.
 */
object TestDeltaSharingServerProcess {
  /** Must match the marker printed by opensharing-test-server TestDeltaSharingServer. */
  val SERVER_READY_MARKER = "DELTA_SHARING_TEST_SERVER_READY"

  private val START_TIMEOUT_SECONDS = 180L

  case class Handle(process: Process, pidFile: File, profileFile: File)

  def start(): Handle = {
    val pidFile = Files.createTempFile("delta-sharing-server", ".pid").toFile
    val profileFile = Files.createTempFile("delta-test", ".share").toFile
    val startLatch = new CountDownLatch(1)
    @volatile var process: Process = null
    @volatile var startError: Option[Throwable] = None

    new Thread("Run TestDeltaSharingServer") {
      setDaemon(true)

      override def run(): Unit = {
        try {
          val processLogger = ProcessLogger { stdout =>
            // scalastyle:off println
            println(stdout)
            // scalastyle:on println
            if (stdout.contains(SERVER_READY_MARKER)) {
              startLatch.countDown()
            }
          }
          val env = sys.env ++ Map(
            "AWS_REGION" -> sys.env.getOrElse("AWS_REGION",
              sys.env.getOrElse("AWS_DEFAULT_REGION", "us-west-2")),
            "AWS_DEFAULT_REGION" -> sys.env.getOrElse("AWS_DEFAULT_REGION",
              sys.env.getOrElse("AWS_REGION", "us-west-2"))
          )
          process =
            Process(
              Seq(
                "/bin/bash",
                "-c",
                s"cd .. && build/sbt 'opensharingTestServer / runMain " +
                  s"io.delta.sharing.server.TestDeltaSharingServer " +
                  s"${pidFile.getCanonicalPath} ${profileFile.getCanonicalPath}'"
              ),
              None,
              env.toSeq: _*
            ).run(processLogger)
          process.exitValue()
          process = null
          startLatch.countDown()
        } catch {
          case e: Throwable =>
            startError = Some(e)
            startLatch.countDown()
        }
      }
    }.start()

    try {
      if (!startLatch.await(START_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        throw new IllegalStateException(
          s"the server didn't start in $START_TIMEOUT_SECONDS seconds")
      }
      startError.foreach(throw _)
      if (process == null) {
        throw new IllegalStateException("the process exited with an error")
      }
      if (profileFile.length() <= 0) {
        throw new IllegalStateException(
          s"OpenSharing did not write a recipient profile to $profileFile")
      }
      Handle(process, pidFile, profileFile)
    } catch {
      case e: Throwable =>
        stop(process, pidFile, profileFile)
        throw e
    }
  }

  def stop(handle: Handle): Unit = {
    stop(handle.process, handle.pidFile, handle.profileFile)
  }

  private def stop(process: Process, pidFile: File, profileFile: File): Unit = {
    if (process != null) {
      process.destroy()
    }
    if (pidFile != null && pidFile.exists()) {
      val pidText = FileUtils.readFileToString(pidFile).trim
      if (pidText.nonEmpty && pidText.forall(_.isDigit)) {
        Try(pidText.toLong).foreach { pid =>
          // scalastyle:off println
          println(s"Killing $pid")
          // scalastyle:on println
          s"kill -9 $pid".!
        }
      }
      pidFile.delete()
    }
    if (profileFile != null) {
      profileFile.delete()
    }
  }
}
