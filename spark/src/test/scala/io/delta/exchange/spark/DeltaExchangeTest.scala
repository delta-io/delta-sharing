package io.delta.exchange.spark

import java.io.File
import java.nio.file.Files

import io.delta.exchange.client.{DeltaExchangeProfile, JsonUtils}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkFunSuite
import org.scalatest.BeforeAndAfterAll

trait DeltaExchangeTest extends SparkFunSuite with BeforeAndAfterAll {

  private var tempProfileFile: File = _

  protected def testProfilePath: String = {
    new Path(tempProfileFile.toURI).toString
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    tempProfileFile = Files.createTempFile("profile-file", ".share").toFile
    val profile =
      DeltaExchangeProfile("https://localhost:443/delta-exchange", "dapi5e3574ec767ca1548ae5bbed1a2dc04d")
    FileUtils.writeStringToFile(tempProfileFile, JsonUtils.toJson(profile), "UTF-8")
  }

  override def afterAll(): Unit = {
    try {
      if (tempProfileFile.exists()) {
        tempProfileFile.delete()
      }
    } finally {
      super.afterAll()
    }
  }
}
