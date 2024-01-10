package io.delta.sharing.client.util

import java.net.URI
import java.util.Collections
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import scala.util.Try

import org.sparkproject.jetty.client.HttpClient
import org.sparkproject.jetty.http.HttpMethod
import org.sparkproject.jetty.server.{Request, Server}
import org.sparkproject.jetty.server.handler.AbstractHandler

class ProxyServer(port: Int) {
  private val server = new Server(port)
  private val httpClient = new HttpClient()
  private val capturedRequests = Collections
    .synchronizedList(new java.util.ArrayList[HttpServletRequest]())

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

  def getCapturedRequests(): Seq[HttpServletRequest] = {
    capturedRequests.toArray(Array[HttpServletRequest]()).toSeq
  }

  private class ProxyHandler extends AbstractHandler {
    override def handle(target: String,
                        baseRequest: Request,
                        request: HttpServletRequest,
                        response: HttpServletResponse): Unit = {

      capturedRequests.add(request)
      Option(request.getHeader("Host")) match {
        case Some(host) =>
          Try {
            val uri = request.getScheme + "://" + host + request.getRequestURI
            val res = httpClient.newRequest(uri)
              .method(HttpMethod.GET)
              .send()

            response.setContentType(res.getMediaType)
            response.setStatus(res.getStatus)
            // scalastyle:off
            response.getWriter.println(res.getContentAsString)
            // scalastyle:on
          }.recover {
            case e: Exception =>
              e.printStackTrace()
              // scalastyle:off
              response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Error in Proxy Server")
            // scalastyle:on
          }

          baseRequest.setHandled(true)

        case None =>
          response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No forwarding URL provided")
      }
    }
  }
}
