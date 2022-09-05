// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.testing

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}

import java.net.InetSocketAddress

/** Helper to create a HTTP server that serves a constant response on the "/result" URL */
object SimpleHttpServer {
  def start(response: String): HttpServer = {
    val server = HttpServer.create(new InetSocketAddress(0), 0)
    server.createContext("/result", new HttpResultHandler(response))
    server.setExecutor(null)
    server.start()
    server
  }

  def responseUrl(server: HttpServer) =
    s"http://localhost:${server.getAddress.getPort}/result"

  def stop(server: HttpServer): Unit =
    server.stop(0)

  private[this] class HttpResultHandler(response: String) extends HttpHandler {
    def handle(t: HttpExchange): Unit = {
      t.sendResponseHeaders(200, response.getBytes().length.toLong)
      val os = t.getResponseBody
      os.write(response.getBytes)
      os.close()
    }
  }

}
