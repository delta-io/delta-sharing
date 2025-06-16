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

import org.apache.http.impl.client.HttpClients
import org.apache.spark.SparkFunSuite

class DeltaSharingFileSystemHttpClientSuite extends SparkFunSuite {
  /**
   * Helper method to create a test DeltaSharingFileSystemHttpClient with mock dependencies
   */
  private def createTestHttpClient(noProxyHosts: Seq[String] = Seq.empty): DeltaSharingFileSystemHttpClient = {
    val mockHttpClient = HttpClients.createDefault()
    DeltaSharingFileSystemHttpClient(
      noProxyHttpClient = mockHttpClient,
      proxyHttpClient = mockHttpClient,
      useProxy = true,
      noProxyHosts = noProxyHosts,
      disableHttps = false
    )
  }

  test("hasNoProxyHostsMatch - exact host matches") {
    val client = createTestHttpClient(noProxyHosts = Seq("example.com", "localhost", "127.0.0.1"))
    
    assert(client.hasNoProxyHostsMatch("example.com"))
    assert(client.hasNoProxyHostsMatch("localhost"))
    assert(client.hasNoProxyHostsMatch("127.0.0.1"))
    assert(!client.hasNoProxyHostsMatch("different.com"))
    assert(!client.hasNoProxyHostsMatch("sub.example.com"))
  }

  test("hasNoProxyHostsMatch - wildcard DNS matches") {
    val client = createTestHttpClient(noProxyHosts = Seq("*.example.com", "*.internal"))
    
    // Should match wildcard patterns
    assert(client.hasNoProxyHostsMatch("api.example.com"))
    assert(client.hasNoProxyHostsMatch("sub.example.com"))
    assert(client.hasNoProxyHostsMatch("deep.nested.example.com"))
    assert(client.hasNoProxyHostsMatch("service.internal"))
    
    // Should NOT match the wildcard domain itself
    assert(!client.hasNoProxyHostsMatch("example.com"))
    assert(!client.hasNoProxyHostsMatch("internal"))
    
    // Should NOT match different domains
    assert(!client.hasNoProxyHostsMatch("example.org"))
    assert(!client.hasNoProxyHostsMatch("notexample.com"))
    assert(!client.hasNoProxyHostsMatch("external"))
  }

  test("hasNoProxyHostsMatch - empty noProxyHosts list") {
    val client = createTestHttpClient(noProxyHosts = Seq.empty)
    
    assert(!client.hasNoProxyHostsMatch("example.com"))
    assert(!client.hasNoProxyHostsMatch("localhost"))
    assert(!client.hasNoProxyHostsMatch("127.0.0.1"))
  }

  test("hasNoProxyHostsMatch - edge cases") {
    // These edge cases may not be correct, but for simplicity, we will allow some of these behaviors.
    val client = createTestHttpClient(noProxyHosts = Seq("*.", "*.168.1.1"))
    
    assert(!client.hasNoProxyHostsMatch("example.com"))
    assert(client.hasNoProxyHostsMatch("."))
    assert(client.hasNoProxyHostsMatch("192.168.1.1")) 
  }
}