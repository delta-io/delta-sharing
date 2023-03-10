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

import java.util.{Arrays, Collections}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.scalatest.FunSuite

import io.delta.sharing.server.config.{SchemaConfig, ServerConfig, ShareConfig, TableConfig}
import io.delta.sharing.server.protocol.{Schema, Share, Table}

class SimpleProtocolManagerSuite extends FunSuite {

  test("list shares") {
    val serverConfig = new ServerConfig()
    serverConfig.shares = Arrays.asList(
      ShareConfig("share1", Collections.emptyList()),
      ShareConfig("share2", Collections.emptyList()),
      ShareConfig("share3", Collections.emptyList()),
      ShareConfig("share4", Collections.emptyList()),
      ShareConfig("share5", Collections.emptyList())
    )
    val protocolManager = new SimpleProtocolManager(serverConfig)

    def checkMaxResults(maxResults: Int): Unit = {
      val results = ArrayBuffer[Share]()
      var response =
        protocolManager.listShares(nextPageToken = None, maxResults = Some(maxResults))
      var partial = response._1
      var nextPageToken: Option[String] = response._2
      results ++= partial
      while (nextPageToken.nonEmpty) {
        response = protocolManager.listShares(
          nextPageToken = nextPageToken,
          maxResults = Some(maxResults))
        partial = response._1
        nextPageToken = response._2
        results ++= partial
      }
      assert(results.map(_.getName) == serverConfig.shares.asScala.map(_.getName))
    }

    for (maxResults <- 1 to 6) {
      checkMaxResults(maxResults)
    }

    var response = protocolManager.listShares(nextPageToken = None, maxResults = Some(0))
    assert(response._1.isEmpty) // shares
    assert(response._2.nonEmpty) // nextPageToken

    response = protocolManager.listShares(nextPageToken = None, maxResults = None)
    assert(response._1.map(_.getName) == serverConfig.shares.asScala.map(_.getName)) // shares
    assert(response._2.isEmpty) // nextPageToken
  }

  test("get share") {
    val serverConfig = new ServerConfig()
    serverConfig.shares = Arrays.asList(
      ShareConfig("share1", Collections.emptyList())
    )
    val protocolManager = new SimpleProtocolManager(serverConfig)
    val response = protocolManager.getShare("share1")
    assert(response.getName == "share1")

    assert(intercept[DeltaSharingNoSuchElementException] {
      protocolManager.getShare("share2")
    }.getMessage.contains("share 'share2' not found"))
  }

  test("list schemas") {
    val serverConfig = new ServerConfig()
    serverConfig.shares = Arrays.asList(
      ShareConfig(
        "share1",
        Arrays.asList(
          SchemaConfig("schema1", Collections.emptyList()),
          SchemaConfig("schema2", Collections.emptyList()),
          SchemaConfig("schema3", Collections.emptyList())
        )
      )
    )
    val protocolManager = new SimpleProtocolManager(serverConfig)
    val (schemas, _) = protocolManager.listSchemas("share1")
    assert(schemas == Seq(
      Schema().withName("schema1").withShare("share1"),
      Schema().withName("schema2").withShare("share1"),
      Schema().withName("schema3").withShare("share1")
    ))

    assert(intercept[DeltaSharingNoSuchElementException] {
      protocolManager.listSchemas("share2")
    }.getMessage.contains("share 'share2' not found"))
  }

  test("list tables") {
    val serverConfig = new ServerConfig()
    serverConfig.shares = Arrays.asList(
      ShareConfig(
        "share1",
        Arrays.asList(
          SchemaConfig(
            "schema1",
            Arrays.asList(
              TableConfig("table1", "location1", "00000000-0000-0000-0000-000000000001"),
              TableConfig("table2", "location1", "00000000-0000-0000-0000-000000000002"),
              TableConfig("table3", "location2", "00000000-0000-0000-0000-000000000003")
            )
          )
        )
      )
    )
    val protocolManager = new SimpleProtocolManager(serverConfig)
    val (tables, _) = protocolManager.listTables("share1", "schema1")
    assert(tables == Seq(
      Table(
        name = Some("table1"),
        schema = Some("schema1"),
        share = Some("share1"),
        id = Some("00000000-0000-0000-0000-000000000001")
      ),
      Table(
        name = Some("table2"),
        schema = Some("schema1"),
        share = Some("share1"),
        id = Some("00000000-0000-0000-0000-000000000002")
      ),
      Table(
        name = Some("table3"),
        schema = Some("schema1"),
        share = Some("share1"),
        id = Some("00000000-0000-0000-0000-000000000003")
      )
    ))

    assert(intercept[DeltaSharingNoSuchElementException] {
      protocolManager.listTables("share2", "schema1")
    }.getMessage.contains("share 'share2' not found"))
    assert(intercept[DeltaSharingNoSuchElementException] {
      protocolManager.listTables("share1", "schema2")
    }.getMessage.contains("schema 'schema2' not found"))
  }

  test("list all tables") {
    val serverConfig = new ServerConfig()
    serverConfig.shares = Arrays.asList(
      ShareConfig(
        "share1",
        Arrays.asList(
          SchemaConfig(
            "schema1",
            Arrays.asList(
              TableConfig("table1", "location1", "00000000-0000-0000-0000-000000000001"),
              TableConfig("table2", "location1", "00000000-0000-0000-0000-000000000002")
            )
          ),
          SchemaConfig(
            "schema2",
            Arrays.asList(
              TableConfig("table3", "location1", "00000000-0000-0000-0000-000000000003")
            )
          )
        )
      )
    )
    val protocolManager = new SimpleProtocolManager(serverConfig)
    val (tables, _) = protocolManager.listAllTables("share1")
    assert(tables == Seq(
      Table(
        name = Some("table1"),
        schema = Some("schema1"),
        share = Some("share1"),
        id = Some("00000000-0000-0000-0000-000000000001")
      ),
      Table(
        name = Some("table2"),
        schema = Some("schema1"),
        share = Some("share1"),
        id = Some("00000000-0000-0000-0000-000000000002")
      ),
      Table(
        name = Some("table3"),
        schema = Some("schema2"),
        share = Some("share1"),
        id = Some("00000000-0000-0000-0000-000000000003")
      )
    ))

    assert(intercept[DeltaSharingNoSuchElementException] {
      protocolManager.listAllTables("share2")
    }.getMessage.contains("share 'share2' not found"))
  }

  test("getTable") {
    val serverConfig = new ServerConfig()
    serverConfig.shares = Arrays.asList(
      ShareConfig(
        "share1",
        Arrays.asList(
          SchemaConfig(
            "schema1",
            Arrays.asList(
              TableConfig("table1", "location1", "00000000-0000-0000-0000-000000000001"),
              TableConfig(
                "table0",
                "location0",
                "00000000-0000-0000-0000-000000000000",
                cdfEnabled = true
              )
            )
          )
        )
      )
    )
    val protocolManager = new SimpleProtocolManager(serverConfig)
    var table = protocolManager.getTable("share1", "schema1", "table1")
    assert(table == TableConfig("table1", "location1", "00000000-0000-0000-0000-000000000001"))

    table = protocolManager.getTable("share1", "schema1", "table0")
    assert(table ==
      TableConfig(
        "table0",
        "location0",
        "00000000-0000-0000-0000-000000000000",
        cdfEnabled = true
      )
    )

    assert(intercept[DeltaSharingNoSuchElementException] {
      protocolManager.getTable("share2", "schema1", "table1")
    }.getMessage.contains("share2/schema1/table1' does not exist, " +
    "please contact your share provider for further information."))
    assert(intercept[DeltaSharingNoSuchElementException] {
      protocolManager.getTable("share1", "schema2", "table1")
    }.getMessage.contains("share1/schema2/table1' does not exist, " +
      "please contact your share provider for further information."))
    assert(intercept[DeltaSharingNoSuchElementException] {
      protocolManager.getTable("share1", "schema1", "table2")
    }.getMessage.contains("share1/schema1/table2' does not exist, " +
      "please contact your share provider for further information."))
  }

  test("empty share list") {
    val protocolManager = new SimpleProtocolManager(new ServerConfig())
    val (shares, nextPageToken) = protocolManager.listShares(None, maxResults = None)
    assert(shares.isEmpty)
    assert(nextPageToken.isEmpty)
  }

  test("invalid maxResults") {
    val protocolManager = new SimpleProtocolManager(new ServerConfig())
    assert(intercept[DeltaSharingIllegalArgumentException] {
      protocolManager.listShares(nextPageToken = None, maxResults = Some(-1))
    }.getMessage.contains("maxResults"))
    assert(intercept[DeltaSharingIllegalArgumentException] {
      protocolManager.listShares(nextPageToken = None, maxResults = Some(501))
    }.getMessage.contains("maxResults"))
  }

  test("incorrect page token") {
    val serverConfig = new ServerConfig()
    serverConfig.shares = Arrays.asList(
      ShareConfig(
        "share1",
        Arrays.asList(
          SchemaConfig(
            "schema1",
            Arrays.asList(
              TableConfig("table1", "location1")
            )
          )
        )
      )
    )
    val protocolManager = new SimpleProtocolManager(serverConfig)
    // invalid base64
    assert(intercept[DeltaSharingIllegalArgumentException] {
      protocolManager.listShares(nextPageToken = Some(":"))
    }.getMessage.contains("invalid 'nextPageToken'"))

    // valid base64 but invalid protobuf
    assert(intercept[DeltaSharingIllegalArgumentException] {
      protocolManager.listShares(nextPageToken = Some("a1b"))
    }.getMessage.contains("invalid 'nextPageToken'"))

    val (_, nextPageToken) =
      protocolManager.listShares(nextPageToken = None, maxResults = Some(0))

    // Send token to a wrong API
    assert(intercept[DeltaSharingIllegalArgumentException] {
      protocolManager.listSchemas("share1", nextPageToken = nextPageToken, maxResults = Some(0))
    }.getMessage.contains("invalid 'nextPageToken'"))
    assert(intercept[DeltaSharingIllegalArgumentException] {
      protocolManager.listTables(
        "share1", "schema1", nextPageToken = nextPageToken, maxResults = Some(0))
    }.getMessage.contains("invalid 'nextPageToken'"))
    assert(intercept[DeltaSharingIllegalArgumentException] {
      protocolManager.listAllTables(
        "share1", nextPageToken = nextPageToken, maxResults = Some(0))
    }.getMessage.contains("invalid 'nextPageToken'"))
  }
}
