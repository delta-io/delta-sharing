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
      throw new IllegalArgumentException(s"The 'version' (${profile.version.get}) in the profile " +
        s"is too new. The current release supports version ${DeltaSharingProfile.CURRENT} and " +
        s"below. Please upgrade to a newer release.")
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
