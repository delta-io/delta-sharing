package io.delta.sharing.spark

import java.io.File
import java.nio.file.Files
import java.util.concurrent.{CountDownLatch, TimeUnit}

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkFunSuite
import org.scalatest.BeforeAndAfterAll

import scala.util.Try
import sys.process._

trait DeltaSharingIntegrationTest extends SparkFunSuite with BeforeAndAfterAll {

  @volatile private var process: Process = _
  @volatile private var pidFile: File = _
  var testProfileFile: File = _

  override def beforeAll(): Unit = {
    println("found AWS_ACCESS_KEY_ID: " + sys.env.get("AWS_ACCESS_KEY_ID").nonEmpty)
    if (sys.env.get("AWS_ACCESS_KEY_ID").nonEmpty) {
      println("found AWS_ACCESS_KEY_ID: " + sys.env.get("AWS_ACCESS_KEY_ID").get.length)
    }
    super.beforeAll()
    if (sys.env.get("AWS_ACCESS_KEY_ID").nonEmpty) {
      pidFile = Files.createTempFile("delta-sharing-server", ".pid").toFile
      testProfileFile = Files.createTempFile("delta-test", ".share").toFile
      FileUtils.writeStringToFile(testProfileFile,
        """{
          |  "version": 1,
          |  "endpoint": "https://localhost:443/delta-sharing",
          |  "bearerToken": "dapi5e3574ec767ca1548ae5bbed1a2dc04d"
          |}""".stripMargin, "UTF-8")

      val startLatch = new CountDownLatch(1)
      new Thread("Run TestDeltaSharingServer") {
        setDaemon(true)

        override def run(): Unit = {
          val processLogger = ProcessLogger { stdout =>
            println(stdout)
            if (stdout.contains("https://127.0.0.1:443/")) {
              startLatch.countDown()
            }
          }
          process =
            Seq(
              "/bin/bash",
              "-c",
              s"cd .. && build/sbt 'server/test:runMain io.delta.sharing.server.TestDeltaSharingServer ${pidFile.getCanonicalPath}'")
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
    if (sys.env.get("AWS_ACCESS_KEY_ID").nonEmpty) {
      try {
        if (process != null) {
          process.destroy()
          process = null
        }
        if (pidFile != null) {
          val pid = FileUtils.readFileToString(pidFile)
          Try(pid.toLong).foreach { pid =>
            println(s"Killing $pid")
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
      assume(sys.env.get("AWS_ACCESS_KEY_ID").nonEmpty)
      func
    }
  }
}
