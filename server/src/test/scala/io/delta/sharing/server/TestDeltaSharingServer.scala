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

package io.delta.sharing.server

import java.io.File
import java.lang.management.ManagementFactory

import org.apache.commons.io.FileUtils

import io.delta.sharing.server.config.ServerConfig

/**
 * This is a special test class for the client projects to test end-to-end experience. It will
 * generate configs for testing and start the sharing server.
 *
 * OpenSharing Server (io.opensharing:opensharing-server) is preferred for serving Delta Sharing
 * clients, with fallback to legacy DeltaSharingService.
 */
object TestDeltaSharingServer {
  private def startOpenSharingServer(serverConfig: ServerConfig): Option[AutoCloseable] = {
    try {
      val serverClass = Class.forName("io.opensharing.OpenSharingServer")
      val propertiesClass = Class.forName("io.opensharing.config.ServerProperties")
      val props = new java.util.Properties()
      props.setProperty("server.port", serverConfig.getPort.toString)
      props.setProperty("server.public-url", s"https://127.0.0.1:${serverConfig.getPort}")
      props.setProperty("protocol.base-path", serverConfig.endpoint)
      props.setProperty("catalog.type", "local")
      val serverProps =
        propertiesClass.getConstructor(classOf[java.util.Properties]).newInstance(props)
      val startMethod = serverClass.getMethod("start", propertiesClass)
      val server =
        startMethod.invoke(null, serverProps.asInstanceOf[AnyRef]).asInstanceOf[AutoCloseable]
      // scalastyle:off println
      println(s"Started OpenSharing server on port ${serverConfig.getPort}")
      // scalastyle:on println
      Some(server)
    } catch {
      case e: Throwable =>
        // scalastyle:off println
        println(s"OpenSharingServer not available or failed to start (${e.getMessage}); " +
          s"using DeltaSharingService fallback.")
        // scalastyle:on println
        None
    }
  }

  def main(args: Array[String]): Unit = {
    val pid = ManagementFactory.getRuntimeMXBean().getName().split("@")(0)
    val pidFile = new File(args(0))
    // scalastyle:off println
    println(s"Writing pid $pid to $pidFile")
    // scalastyle:off on
    FileUtils.writeStringToFile(pidFile, pid)
    if (sys.env.get("AWS_ACCESS_KEY_ID").exists(_.length > 0)) {
      val serverConfigPath = TestResource.setupTestTables().getCanonicalPath
      val serverConfig = ServerConfig.load(serverConfigPath)
      // scalastyle:off println
      println("serverConfigPath=" + serverConfigPath)
      println("serverConfig=" + serverConfig)
      // scalastyle:on println
      val openSharingServer = startOpenSharingServer(serverConfig)
      val server = if (openSharingServer.isEmpty) {
        Some(DeltaSharingService.start(serverConfig))
      } else {
        None
      }
      // scalastyle:off println
      println(s"Serving at https://127.0.0.1:${serverConfig.getPort}/")
      println(s"https://127.0.0.1:${serverConfig.getPort}/")
      // scalastyle:on println
      // Run at most 900 seconds and exit. This is to ensure we can exit even if the parent process
      // hits any error.
      Thread.sleep(900000)
      openSharingServer.foreach(_.close())
      server.foreach(_.stop())
    } else {
      throw new IllegalArgumentException("Cannot find AWS_ACCESS_KEY_ID in sys.env")
    }
  }
}
