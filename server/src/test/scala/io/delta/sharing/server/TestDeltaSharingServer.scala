package io.delta.sharing.server

import java.io.File
import java.lang.management.ManagementFactory

import io.delta.sharing.server.config.ServerConfig
import org.apache.commons.io.FileUtils

object TestDeltaSharingServer {
  def main(args: Array[String]): Unit = {
    val pid = ManagementFactory.getRuntimeMXBean().getName().split("@")(0)
    val pidFile = new File(args(0))
    println(s"Writing pid $pid to $pidFile")
    FileUtils.writeStringToFile(pidFile, pid)
    if (sys.env.get("AWS_ACCESS_KEY_ID").exists(_.length > 0)) {
      val serverConfigPath = TestResource.setupTestTables().getCanonicalPath
      val serverConfig = ServerConfig.load(serverConfigPath)
      val server = DeltaSharingService.start(serverConfig)
      // Run at most 60 seconds and exit. This is to ensure we can exit even if the parent process
      // hits any error.
      Thread.sleep(60000)
      server.stop()
    } else {
      throw new IllegalArgumentException("Cannot find AWS_ACCESS_KEY_ID in sys.env")
    }
  }
}
