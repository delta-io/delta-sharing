package io.delta.sharing.spark

import io.delta.sharing.spark.util.JsonUtils
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import java.nio.charset.StandardCharsets.UTF_8

case class DeltaSharingProfile(
    version: Option[Int] = Some(DeltaSharingProfile.CURRENT),
    endpoint: String = null,
    bearerToken: String = null)

object DeltaSharingProfile {
  val CURRENT = 1
}

trait DeltaSharingProfileProvider {
  def getProfile: DeltaSharingProfile
}

/**
 * Load [[DeltaSharingProfile]] from a file. `conf` should be provided to load the file from remote
 * file systems.
 */
class DeltaSharingFileProfileProvider(
    conf: Configuration,
    file: String) extends DeltaSharingProfileProvider {

  val profile = {
    val input = new Path(file).getFileSystem(conf).open(new Path(file))
    val profile = try {
      JsonUtils.fromJson[DeltaSharingProfile](IOUtils.toString(input, UTF_8))
    } finally {
      input.close()
    }
    if (profile.version.isEmpty) {
      throw new IllegalArgumentException("Cannot find the 'version' field in the profile file")
    }
    if (profile.version.get > DeltaSharingProfile.CURRENT) {
      throw new IllegalArgumentException(s"The 'version' (${profile.version.get}) in the profile is " +
        s"too new. The current release supports version ${DeltaSharingProfile.CURRENT} and below. " +
        s"Please upgrade to a newer release.")
    }
    if (profile.endpoint == null) {
      throw new IllegalArgumentException("Cannot find the 'endpoint' field in the profile file")
    }
    if (profile.bearerToken == null) {
      throw new IllegalArgumentException("Cannot find the 'bearerToken' field in the profile file")
    }
    profile
  }

  override def getProfile: DeltaSharingProfile = profile
}
