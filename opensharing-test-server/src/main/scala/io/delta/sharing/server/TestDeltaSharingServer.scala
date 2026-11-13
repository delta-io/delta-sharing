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
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, StandardOpenOption}
import java.util.{Collections, Properties}
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import com.fasterxml.jackson.databind.ObjectMapper
import com.linecorp.armeria.client.WebClient
import com.linecorp.armeria.common.{
  AggregatedHttpRequest,
  HttpData,
  HttpMethod,
  HttpResponse,
  HttpStatus,
  MediaType,
  ResponseHeaders
}
import com.linecorp.armeria.server.{HttpService, Server, ServiceRequestContext}
import io.opensharing.OpenSharingServer
import io.opensharing.auth.UserContext
import io.opensharing.catalog.AssetType
import io.opensharing.config.ServerProperties
import io.opensharing.recipient.{AuthenticationType, CreateRecipientRequest, ProfileFile}
import io.opensharing.share.{
  CreateShareRequest,
  SharePrivilege,
  UpdateSharePermissionsRequest,
  UpdateShareRequest
}

/**
 * Starts [[OpenSharingServer]] for client / spark / python integration tests.
 *
 * This project depends only on the OpenSharing fat jar so its Jackson / Armeria versions are not
 * mixed with the deprecated reference server's pinned Jackson 2.6.7 classpath.
 *
 * Args: &lt;pidFile&gt; [&lt;profileFile&gt;]
 *
 * OpenSharing listens on an internal plain-HTTP port. A front server on [[TEST_PORT]] serves the
 * protocol over self-signed HTTPS (the suite profiles use https://localhost, and the test JVMs
 * trust all certificates), rewrites the deprecated reference server's well-known bearer token,
 * reproduces the reference server's async-query test behaviour for a few dedicated tables, and
 * proxies everything else verbatim to OpenSharing.
 *
 * Catalog: classpath resource `integration-tables.json` (from `dev/`), shared with
 * [[io.delta.sharing.server.TestResource]].
 */
object TestDeltaSharingServer {
  /** Printed once the server is ready; client/spark/python harnesses wait for this exact line. */
  val SERVER_READY_MARKER = "DELTA_SHARING_TEST_SERVER_READY"

  private val TEST_PORT = 12345
  private val INTERNAL_PORT = 12346
  private val UPSTREAM_TIMEOUT_MILLIS = 300000L
  private val PROTOCOL_BASE_PATH = "/delta-sharing"
  private val PROVIDER_TOKEN = "alice-token"
  private val PROVIDER_USER_ID = "catalog-alice-id"
  private val PROVIDER_USER_NAME = "alice"
  private val RECIPIENT_NAME = "integration-tests"

  /**
   * Tables whose async queries the front server answers like the deprecated reference server did:
   * an initial query returns a pending query status, the first three polls stay pending, and the
   * fourth poll either fails or returns the real result. The reference server hooked `table_wasb`
   * for the plain case; OpenSharing serves Azure tables over abfs only, so `table_abfs` stands in
   * for it, and the three tables named after their hooks are abfs tables as well.
   */
  private val ASYNC_HOOK_TABLES = Set(
    "tableWithAsyncQueryError",
    "table_abfs",
    "table_wasb_change_query_id",
    "table_wasb_change_query_id_to_be_null",
    "table_wasb_bad_table")

  /** The capabilities header value that marks a query as async. */
  private val ASYNC_CAPABILITY = "asyncquery"

  private val DeltaSharingCapabilitiesHeader = "delta-sharing-capabilities"

  private val NDJSON = MediaType.parse("application/x-ndjson; charset=utf-8")

  private val mapper = new ObjectMapper()

  /** An async query the front server is tracking, with its poll count. */
  private final class AsyncQuery(val table: String) {
    val polls = new AtomicInteger()
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      throw new IllegalArgumentException(
        "Usage: TestDeltaSharingServer <pidFile> [profileFile]")
    }
    if (!sys.env.get("AWS_ACCESS_KEY_ID").exists(_.length > 0)) {
      throw new IllegalArgumentException("Cannot find AWS_ACCESS_KEY_ID in sys.env")
    }
    // OpenSharing's AWS SDK v2 credential helper reads AWS_REGION / AWS_DEFAULT_REGION.
    if (!sys.env.get("AWS_REGION").exists(_.nonEmpty) &&
        !sys.env.get("AWS_DEFAULT_REGION").exists(_.nonEmpty)) {
      sys.props.getOrElseUpdate("aws.region", "us-west-2")
      // scalastyle:off println
      println("AWS_REGION not set; defaulting aws.region system property to us-west-2")
      // scalastyle:on println
    }

    val pid = ManagementFactory.getRuntimeMXBean.getName.split("@")(0)
    val pidFile = new File(args(0))
    val profileFile =
      if (args.length >= 2) Some(new File(args(1))) else None
    // scalastyle:off println
    println(s"Writing pid $pid to $pidFile")
    // scalastyle:on println
    writeString(pidFile, pid)

    maybeSetupGoogleServiceAccountCredentials()

    val shares = loadShares()
    val catalogFile = writeLocalCatalog(shares)
    // OpenSharing builds the recipient profile's endpoint and the activation links from the
    // public URL, so it must be the front server's.
    val internalProps = new Properties()
    internalProps.setProperty("server.port", INTERNAL_PORT.toString)
    internalProps.setProperty("server.public-url", s"https://localhost:$TEST_PORT")
    internalProps.setProperty("protocol.base-path", PROTOCOL_BASE_PATH)
    internalProps.setProperty("catalog.type", "local")
    internalProps.setProperty("catalog.local.file", catalogFile.getAbsolutePath)

    val server = try {
      OpenSharingServer.start(new ServerProperties(internalProps))
    } catch {
      case e: Throwable =>
        throw new IllegalStateException("Failed to start OpenSharingServer", e)
    }

    try {
      val profile = bootstrap(server, shares)
      profileFile.foreach { file =>
        writeProfile(file, profile)
        // scalastyle:off println
        println(s"Wrote recipient profile to ${file.getCanonicalPath}")
        // scalastyle:on println
      }
      startFrontServer()
      // scalastyle:off println
      println(SERVER_READY_MARKER)
      // scalastyle:on println
      // Stay alive until the parent test process kills us via the pid file.
      new CountDownLatch(1).await()
    } finally {
      server.close()
    }
  }

  /**
   * The recipient-facing front server: self-signed HTTPS on [[TEST_PORT]], an Authorization
   * rewrite for the reference server's fixed token, async-query hooks for [[ASYNC_HOOK_TABLES]],
   * and a verbatim proxy to the internal OpenSharing port for everything else.
   */
  private def startFrontServer(): Server = {
    // Armeria's default 10 s server and 15 s client response timeouts would answer a slow
    // upstream request with a 503 that the real server never sends, so the front server waits for
    // the upstream answer instead.
    val proxy = WebClient.builder(s"http://127.0.0.1:$INTERNAL_PORT")
      .responseTimeoutMillis(UPSTREAM_TIMEOUT_MILLIS)
      .build()
    val asyncQueries = new ConcurrentHashMap[String, AsyncQuery]()

    val service = new HttpService {
      override def serve(ctx: ServiceRequestContext, req: com.linecorp.armeria.common.HttpRequest)
        : HttpResponse = {
        HttpResponse.from(
          req.aggregate().thenApplyAsync[HttpResponse](
            new java.util.function.Function[AggregatedHttpRequest, HttpResponse] {
              override def apply(aggregated: AggregatedHttpRequest): HttpResponse =
                handle(aggregated)
            },
            ctx.blockingTaskExecutor()))
      }

      private def handle(request: AggregatedHttpRequest): HttpResponse = {
        val capabilities =
          Option(request.headers().get(DeltaSharingCapabilitiesHeader)).getOrElse("")
        val forwarded = AggregatedHttpRequest.of(request.headers(), request.content())
        val path = forwarded.path()

        hook(path, forwarded, capabilities).getOrElse {
          val response = proxy.execute(forwarded).aggregate().join()
          HttpResponse.of(response.headers(), response.content())
        }
      }

      /**
       * The front server's async-query hook: a pending status for the initial query of a hooked
       * table, and the reference server's poll behaviour for its polls.
       */
      private def hook(
          path: String,
          request: AggregatedHttpRequest,
          capabilities: String): Option[HttpResponse] = {
        val prefix = s"$PROTOCOL_BASE_PATH/shares/"
        if (!path.startsWith(prefix) || !path.contains("/tables/")) {
          return None
        }
        val afterTable = path.substring(path.lastIndexOf("/tables/") + "/tables/".length)
        val slash = afterTable.indexOf('/')
        if (slash <= 0) {
          return None
        }
        val table = afterTable.substring(0, slash)
        val suffix = afterTable.substring(slash + 1)
        if (request.method() != HttpMethod.POST || !ASYNC_HOOK_TABLES.contains(table)) {
          return None
        }
        if (suffix == "query") {
          if (!capabilities.contains(ASYNC_CAPABILITY)) {
            return None
          }
          val parts = path.stripPrefix(prefix).split("/")
          val queryId = s"${parts(0)}_${parts(2)}_${parts(4)}_${System.currentTimeMillis()}_" +
            java.util.UUID.randomUUID().toString
          asyncQueries.put(queryId, new AsyncQuery(table))
          Some(queryStatusResponse(queryId))
        } else if (suffix.startsWith("queries/")) {
          Some(poll(suffix.stripPrefix("queries/"), path, request))
        } else {
          None
        }
      }

      /**
       * Answers one poll: pending for the first three, then the reference server's per-table
       * error behaviour, or the real query result proxied from OpenSharing.
       */
      private def poll(
          queryId: String,
          path: String,
          request: AggregatedHttpRequest): HttpResponse = {
        val query = asyncQueries.get(queryId)
        if (query == null) {
          return errorResponse(
            HttpStatus.NOT_FOUND,
            "RESOURCE_DOES_NOT_EXIST",
            s"query '$queryId' does not exist")
        }
        if (query.table == "tableWithAsyncQueryError") {
          return errorResponse(
            HttpStatus.BAD_REQUEST,
            "INVALID_PARAMETER_VALUE",
            "expected error")
        }
        val count = query.polls.incrementAndGet()
        if (count <= 3) {
          val returnedQueryId =
            if (query.table == "table_wasb_change_query_id_to_be_null") {
              null
            } else if (query.table == "table_wasb_change_query_id") {
              s"${queryId}_modified"
            } else {
              queryId
            }
          return queryStatusResponse(returnedQueryId)
        }
        if (query.table == "table_wasb_bad_table") {
          return errorResponse(
            HttpStatus.NOT_FOUND,
            "RESOURCE_DOES_NOT_EXIST",
            "table 'default.nonexistent_bad_table' does not exist in share 'share_azure'")
        }

        // Return the real query result: re-run the query against OpenSharing with the poll
        // request's maxFiles / pageToken, like the deprecated reference server's getQueryStatus.
        val node =
          if (request.content().isEmpty) mapper.createObjectNode()
          else mapper.readTree(request.contentUtf8())
        val body = mapper.createObjectNode()
        if (node.hasNonNull("maxFiles")) body.put("maxFiles", node.get("maxFiles").asInt())
        if (node.hasNonNull("pageToken")) body.put("pageToken", node.get("pageToken").asText())
        val queryPath = path.substring(0, path.length - (queryId.length + "/queries/".length)) +
          "/query"
        val queryRequest = AggregatedHttpRequest.of(
          request.headers().toBuilder
            .method(HttpMethod.POST)
            .path(queryPath)
            .contentType(MediaType.JSON)
            .build(),
          HttpData.ofUtf8(mapper.writeValueAsString(body)))
        val response = proxy.execute(queryRequest).aggregate().join()
        HttpResponse.of(response.headers(), response.content())
      }

      /** A pending query status, like the deprecated reference server's async initial reply. */
      private def queryStatusResponse(queryId: String): HttpResponse = {
        val line =
          if (queryId == null) """{"queryStatus":{"queryId":null}}"""
          else s"""{"queryStatus":{"queryId":"$queryId"}}"""
        HttpResponse.of(
          ResponseHeaders.builder(HttpStatus.OK)
            .contentType(NDJSON)
            .add("Delta-Table-Version", "0")
            .build(),
          HttpData.ofUtf8(line))
      }

      private def errorResponse(
          status: HttpStatus,
          errorCode: String,
          message: String): HttpResponse = {
        val body = mapper.createObjectNode()
        body.put("errorCode", errorCode)
        body.put("message", message)
        HttpResponse.of(
          ResponseHeaders.builder(status).contentType(MediaType.JSON).build(),
          HttpData.ofUtf8(mapper.writeValueAsString(body)))
      }
    }

    val front = Server.builder()
      .https(TEST_PORT)
      .tlsSelfSigned()
      .requestTimeoutMillis(UPSTREAM_TIMEOUT_MILLIS)
      .serviceUnder("/", service)
      .build()
    front.start().join()
    front
  }

  private def writeProfile(file: File, profile: ProfileFile): Unit = {
    val node = mapper.createObjectNode()
    node.put("shareCredentialsVersion", profile.shareCredentialsVersion)
    node.put("endpoint", profile.endpoint)
    node.put("bearerToken", profile.bearerToken)
    node.put("expirationTime", profile.expirationTime)
    writeString(file, mapper.writerWithDefaultPrettyPrinter.writeValueAsString(node))
  }

  private def writeString(file: File, content: String): Unit = {
    Files.write(
      file.toPath,
      content.getBytes(UTF_8),
      StandardOpenOption.CREATE,
      StandardOpenOption.TRUNCATE_EXISTING,
      StandardOpenOption.WRITE
    )
  }

  private def bootstrap(server: OpenSharingServer, shares: Seq[ShareSpec]): ProfileFile = {
    val openSharing = server.openSharing()
    val alice = new UserContext(PROVIDER_USER_ID, PROVIDER_TOKEN, PROVIDER_USER_NAME)

    shares.foreach { share =>
      openSharing.shares.create(alice, new CreateShareRequest(share.name, null, null, null))
      val updates = new ArrayBuffer[UpdateShareRequest.Update]()
      share.schemas.foreach { schema =>
        schema.tables.foreach { table =>
          val assetId = assetIdentifier(share.name, schema.name, table.name)
          updates += new UpdateShareRequest.Update(
            UpdateShareRequest.Action.ADD,
            new UpdateShareRequest.DataObject(
              assetId,
              AssetType.TABLE,
              s"${schema.name}.${table.name}"
            )
          )
        }
      }
      if (updates.nonEmpty) {
        openSharing.shares.update(
          alice,
          share.name,
          new UpdateShareRequest(null, null, null, updates.toList.asJava, null)
        )
      }
    }

    val recipient = openSharing.recipients.create(
      alice,
      new CreateRecipientRequest(RECIPIENT_NAME, null, AuthenticationType.TOKEN, Long.box(3650L))
    )
    val activationUrl = recipient.activationUrl
    if (activationUrl == null || activationUrl.isEmpty) {
      throw new IllegalStateException("recipient activation URL was empty")
    }
    val activationCode = activationUrl.substring(activationUrl.lastIndexOf('/') + 1)

    shares.foreach { share =>
      openSharing.shares.updatePermissions(
        alice,
        share.name,
        new UpdateSharePermissionsRequest(
          Collections.singletonList(
            new UpdateSharePermissionsRequest.Change(
              RECIPIENT_NAME,
              Collections.singletonList(SharePrivilege.SELECT),
              Collections.emptyList()
            )
          )
        )
      )
    }

    openSharing.recipients.activate(activationCode)
  }

  private def writeLocalCatalog(shares: Seq[ShareSpec]): File = {
    val file = Files.createTempFile("opensharing-local-catalog", ".yml").toFile
    file.deleteOnExit()
    val sb = new StringBuilder
    sb.append("# Generated for delta-sharing client integration tests.\n")
    sb.append("credentials:\n")
    sb.append("  mode: ENV\n")
    sb.append("  ttlSeconds: 3600\n\n")
    sb.append("principals:\n")
    sb.append(s"  - bearerToken: $PROVIDER_TOKEN\n")
    sb.append(s"    userId: $PROVIDER_USER_ID\n")
    sb.append(s"    userName: $PROVIDER_USER_NAME\n")
    sb.append(
      "    privileges: [CREATE_SHARE, CREATE_RECIPIENT, USE_SHARE, USE_RECIPIENT]\n\n")
    sb.append("assets:\n")
    shares.foreach { share =>
      share.schemas.foreach { schema =>
        schema.tables.foreach { table =>
          val id = assetIdentifier(share.name, schema.name, table.name)
          sb.append(s"  - identifier: $id\n")
          sb.append("    type: TABLE\n")
          sb.append(s"    storageLocation: ${table.location}\n")
          sb.append("    format: delta\n")
        }
      }
    }
    writeString(file, sb.toString)
    file
  }

  private def assetIdentifier(share: String, schema: String, table: String): String = {
    s"delta.$share.$schema.$table"
  }

  private def loadShares(): Seq[ShareSpec] = {
    val stream = getClass.getClassLoader.getResourceAsStream("integration-tables.json")
    if (stream == null) {
      throw new IllegalStateException("missing classpath resource integration-tables.json")
    }
    try {
      val root = mapper.readTree(stream)
      root.get("shares").elements().asScala.map { shareNode =>
        val schemas = shareNode.get("schemas").elements().asScala.map { schemaNode =>
          val tables = schemaNode.get("tables").elements().asScala.map { tableNode =>
            TableSpec(
              tableNode.get("name").asText(),
              tableNode.get("location").asText()
            )
          }.toSeq
          SchemaSpec(schemaNode.get("name").asText(), tables)
        }.toSeq
        ShareSpec(shareNode.get("name").asText(), schemas)
      }.toSeq
    } finally {
      stream.close()
    }
  }

  private def maybeSetupGoogleServiceAccountCredentials(): Unit = {
    if (sys.env.get("GOOGLE_SERVICE_ACCOUNT_KEY").exists(_.length > 0) &&
        sys.env.get("GOOGLE_APPLICATION_CREDENTIALS").exists(_.length > 0)) {
      val serviceAccountKey = sys.env("GOOGLE_SERVICE_ACCOUNT_KEY")
      val credFilePath = new File(sys.env("GOOGLE_APPLICATION_CREDENTIALS"))
      credFilePath.deleteOnExit()
      writeString(credFilePath, serviceAccountKey)
    }
  }

  private case class TableSpec(name: String, location: String)
  private case class SchemaSpec(name: String, tables: Seq[TableSpec])
  private case class ShareSpec(name: String, schemas: Seq[SchemaSpec])
}
