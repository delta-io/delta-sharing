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

package io.delta.sharing.server.util

import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest

object AuthUtils {
  def compareBearerTokens(t1: String, t2: String): Boolean = {
    // Use `MessageDigest.isEqual` to do a time-constant comparison to avoid
    // timing attacks
    if (t1 == null || t2 == null) return false

    MessageDigest.isEqual(
      t1.getBytes(UTF_8),
      t2.getBytes(UTF_8)
    )
  }
}
