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

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkFunSuite
import org.scalatest.BeforeAndAfterAll

trait DeltaSharingIntegrationTest extends SparkFunSuite with BeforeAndAfterAll {

  def shouldRunIntegrationTest: Boolean = {
    sys.env.get("AWS_ACCESS_KEY_ID").exists(_.length > 0) &&
      sys.env.get("AZURE_TEST_ACCOUNT_KEY").exists(_.length > 0) &&
      sys.env.get("GOOGLE_APPLICATION_CREDENTIALS").exists(_.length > 0)
  }

  @volatile private var serverHandle: TestDeltaSharingServerProcess.Handle = _
  var testProfileFile: File = _

  val TEST_PORT = 12345

  override def beforeAll(): Unit = {
    super.beforeAll()
    if (shouldRunIntegrationTest) {
      serverHandle = TestDeltaSharingServerProcess.start()
      testProfileFile = serverHandle.profileFile
    }
  }

  override def afterAll(): Unit = {
    if (shouldRunIntegrationTest) {
      try {
        org.apache.hadoop.fs.FileSystem.closeAll()
        if (serverHandle != null) {
          TestDeltaSharingServerProcess.stop(serverHandle)
          serverHandle = null
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
