package io.delta.sharing.spark

import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import scala.util.Try

import org.sparkproject.jetty.client.HttpClient
import org.sparkproject.jetty.http.HttpMethod
import org.sparkproject.jetty.server.{Request, Server}
import org.sparkproject.jetty.server.handler.AbstractHandler
import org.sparkproject.jetty.util.ssl.SslContextFactory


/**
 * A simple proxy server that forwards storage access while upgrading the connection to https.
 * This is used to test the behavior of the DeltaSharingFileSystem when
 * "spark.delta.sharing.never.use.https" is set to true.
 */
class TestStorageProxyServer(port: Int) {
  private val server = new Server(port)
  val sslContextFactory = new SslContextFactory.Client()
  private val httpClient = new HttpClient(sslContextFactory)
  server.setHandler(new ProxyHandler)

  def initialize(): Unit = {
    new Thread(() => {
      Try(httpClient.start())
      Try(server.start())
    }).start()

    do {
      Thread.sleep(100)
    } while (!server.isStarted())
  }

  def stop(): Unit = {
    Try(server.stop())
    Try(httpClient.stop())
  }

  def getPort(): Int = {
    server.getURI().getPort()
  }

  def getHost(): String = {
    server.getURI().getHost
  }

  private class ProxyHandler extends AbstractHandler {
    override def handle(target: String,
                        baseRequest: Request,
                        request: HttpServletRequest,
                        response: HttpServletResponse): Unit = {

      Option(request.getHeader("Host")) match {
        case Some(host) =>
          // upgrade bucket access call from http -> https
          val uri = "https://" + host + request.getRequestURI.replace("null", "") +
            "?" + request.getQueryString

          val res = httpClient.newRequest(uri)
            .method(HttpMethod.GET)
            .header("Range", request.getHeader("Range"))
            .send()

          response.setStatus(res.getStatus)
          res.getHeaders.forEach { header =>
            response.setHeader(header.getName, header.getValue)
          }
          val out = response.getOutputStream
          out.write(res.getContent, 0, res.getContent.length)
          out.flush()
          out.close()

          baseRequest.setHandled(true)

        case None =>
          response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No forwarding URL provided")
      }
    }
  }
}
