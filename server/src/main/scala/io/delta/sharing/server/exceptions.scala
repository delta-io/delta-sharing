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

/**
 * A special exception for invalid requests happening in Delta Sharing Server. We define a special
 * class rather than reusing `IllegalArgumentException` so that we can ensure that the message in
 * `IllegalArgumentException` thrown from other libraries won't be returned to users.
 *
 * @note `message` will be in the response. Please make sure it doesn't contain any sensitive info.
 */
class DeltaSharingIllegalArgumentException(message: String)
  extends IllegalArgumentException(message)

/**
 * A special exception for resource not found in Delta Sharing Server. We define a special
 * class rather than reusing `NoSuchElementException` so that we can ensure that the message in
 * `NoSuchElementException` thrown from other libraries won't be returned to users.
 *
 * @note `message` will be in the response. Please make sure it doesn't contain any sensitive info.
 */
class DeltaSharingNoSuchElementException(message: String)
  extends NoSuchElementException(message)

/**
 * A special exception for invalid requests happening in Delta Sharing Server. We define a special
 * class rather than reusing `UnsupportedOperationException` so that we can ensure that the message
 * in `UnsupportedOperationException` thrown from other libraries won't be returned to users.
 *
 * @note `message` will be in the response. Please make sure it doesn't contain any sensitive info.
 */
class DeltaSharingUnsupportedOperationException(message: String)
  extends UnsupportedOperationException(message)

/**
 * A special exception that wraps an unhandled exception when processing a request.
 * `DeltaInternalException` should never be exposed to users as an unhandled exception may contain
 * sensitive information.
 */
class DeltaInternalException(e: Throwable) extends RuntimeException(e)

object ErrorStrings {
  def multipleParametersSetErrorMsg(params: Seq[String]): String = {
    s"Please only provide one of: ${params.mkString(",")}"
  }
}

object CausedBy {
  def unapply(e: Throwable): Option[Throwable] = Option(e.getCause)
}
