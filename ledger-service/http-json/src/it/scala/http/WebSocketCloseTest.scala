// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import akka.http.scaladsl.model.Uri
import com.daml.http.HttpServiceTestFixture.UseTls
import com.daml.http.dbbackend.JdbcConfig
import java.net.http.{HttpClient, WebSocket}
import org.scalatest.Inside
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import scala.jdk.FutureConverters._

class WebSocketCloseTest
    extends AsyncFreeSpec
    with Matchers
    with Inside
    with AbstractHttpServiceIntegrationTestFunsCustomToken {
  override def jdbcConfig: Option[JdbcConfig] = None
  override def staticContentConfig: Option[StaticContentConfig] = None
  override def useTls: UseTls = UseTls.NoTls
  override def wsConfig: Option[WebsocketConfig] = None

  "sending close from the client should result in receiving close from the server" in withHttpService {
    (uri, _, _, _) =>
      import WebsocketTestFixture.validSubprotocols

      val serverCloseReceived = scala.concurrent.Promise[Unit]()
      val listener = new WebSocket.Listener() {
        override def onClose(ws: WebSocket, statusCode: Int, reason: String) = {
          val _ = serverCloseReceived.success(())
          super.onClose(ws, statusCode, reason)
        }
      }

      val wsUri = java.net.URI.create(
        uri.copy(scheme = "ws").withPath(Uri.Path(s"/v1/stream/query")).toString
      )

      for {
        subprotocols <- jwt(uri).map(validSubprotocols)
        ws <- HttpClient
          .newHttpClient()
          .newWebSocketBuilder()
          .subprotocols(subprotocols.head, subprotocols.tail: _*)
          .buildAsync(wsUri, listener)
          .asScala
        _ <- ws.sendText("""{"templateIds": ["Iou:Iou"]}""", true).asScala
        _ <- ws.sendClose(WebSocket.NORMAL_CLOSURE, "ok").asScala
        _ <- serverCloseReceived.future
      } yield {
        ws.isInputClosed shouldBe true
      }
  }
}
