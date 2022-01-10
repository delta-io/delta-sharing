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

import java.nio.charset.StandardCharsets.UTF_8

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import io.delta.sharing.spark.util.JsonUtils

case class DeltaSharingProfile(
    shareCredentialsVersion: Option[Int] = Some(DeltaSharingProfile.CURRENT),
    endpoint: String = null,
    bearerToken: String = null,
    expirationTime: String = null)

object DeltaSharingProfile {
  val CURRENT = 1
}

/**
 * A provider that provides Delta Sharing profile for data recipient to access the shared data.
 * https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md#profile-file-format.
 */
trait DeltaSharingProfileProvider {
  def getProfile: DeltaSharingProfile
}

/**
 * Load [[DeltaSharingProfile]] from a file. `conf` should be provided to load the file from remote
 * file systems.
 */
private[sharing] class DeltaSharingFileProfileProvider(
    conf: Configuration,
    file: String) extends DeltaSharingProfileProvider {

  val profile = {
    val input = new Path(file).getFileSystem(conf).open(new Path(file))
    val profile = try {
      JsonUtils.fromJson[DeltaSharingProfile](IOUtils.toString(input, UTF_8))
    } finally {
      input.close()
    }
    if (profile.shareCredentialsVersion.isEmpty) {
      throw new IllegalArgumentException(
        "Cannot find the 'shareCredentialsVersion' field in the profile file")
    }

    if (profile.shareCredentialsVersion.get > DeltaSharingProfile.CURRENT) {
      throw new IllegalArgumentException(
        s"'shareCredentialsVersion' in the profile is " +
          s"${profile.shareCredentialsVersion.get} which is too new. The current release " +
          s"supports version ${DeltaSharingProfile.CURRENT} and below. Please upgrade to a newer " +
          s"release.")
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
