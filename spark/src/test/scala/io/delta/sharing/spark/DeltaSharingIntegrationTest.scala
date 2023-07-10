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

package io.delta.sharing.spark

import java.io.File
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import java.util.concurrent.{CountDownLatch, TimeUnit}

import scala.sys.process._
import scala.util.Try

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkFunSuite
import org.scalatest.BeforeAndAfterAll

import io.delta.sharing.client.{DeltaSharingFileProfileProvider, DeltaSharingProfileProvider}

trait DeltaSharingIntegrationTest extends SparkFunSuite with BeforeAndAfterAll {

  def shouldRunIntegrationTest: Boolean = {
    sys.env.get("AWS_ACCESS_KEY_ID").exists(_.length > 0) &&
      sys.env.get("AZURE_TEST_ACCOUNT_KEY").exists(_.length > 0) &&
      sys.env.get("GOOGLE_APPLICATION_CREDENTIALS").exists(_.length > 0)
  }

  @volatile private var process: Process = _
  @volatile private var pidFile: File = _
  var testProfileFile: File = _

  val TEST_PORT = 12345

  override def beforeAll(): Unit = {
    super.beforeAll()
    if (shouldRunIntegrationTest) {
      pidFile = Files.createTempFile("delta-sharing-server", ".pid").toFile
      testProfileFile = Files.createTempFile("delta-test", ".share").toFile
      FileUtils.writeStringToFile(testProfileFile,
        s"""{
          |  "shareCredentialsVersion": 1,
          |  "endpoint": "https://localhost:$TEST_PORT/delta-sharing",
          |  "bearerToken": "dapi5e3574ec767ca1548ae5bbed1a2dc04d"
          |}""".stripMargin, UTF_8)

      val startLatch = new CountDownLatch(1)
      new Thread("Run TestDeltaSharingServer") {
        setDaemon(true)

        override def run(): Unit = {
          val processLogger = ProcessLogger { stdout =>
            // scalastyle:off println
            println(stdout)
            // scalastyle:on println
            if (stdout.contains(s"https://127.0.0.1:$TEST_PORT/")) {
              startLatch.countDown()
            }
          }
          process =
            Seq(
              "/bin/bash",
              "-c",
              s"cd .. && build/sbt 'server / Test / runMain " +
                s"io.delta.sharing.server.TestDeltaSharingServer ${pidFile.getCanonicalPath}'")
              .run(processLogger)
          process.exitValue()
          process = null
          startLatch.countDown()
        }
      }.start()
      try {
        assert(startLatch.await(120, TimeUnit.SECONDS), "the server didn't start in 120 seconds")
        if (process == null) {
          fail("the process exited with an error")
        }
      } catch {
        case e: Throwable =>
          if (process != null) {
            process.destroy()
            process = null
          }
          throw e
      }
    }
  }

  override def afterAll(): Unit = {
    if (shouldRunIntegrationTest) {
      try {
        org.apache.hadoop.fs.FileSystem.closeAll()
        if (process != null) {
          process.destroy()
          process = null
        }
        if (pidFile != null) {
          val pid = FileUtils.readFileToString(pidFile)
          Try(pid.toLong).foreach { pid =>
            // scalastyle:off println
            println(s"Killing $pid")
            // scalastyle:on println
            s"kill -9 $pid".!
          }
          pidFile.delete()
        }
        if (testProfileFile != null) {
          testProfileFile.delete()
        }
      } finally {
        super.afterAll()
      }
    }
  }

  def testProfileProvider: DeltaSharingProfileProvider = {
    new DeltaSharingFileProfileProvider(new Configuration, testProfileFile.getCanonicalPath)
  }

  def integrationTest(testName: String)(func: => Unit): Unit = {
    test(testName) {
      assume(shouldRunIntegrationTest)
      func
    }
  }
}
