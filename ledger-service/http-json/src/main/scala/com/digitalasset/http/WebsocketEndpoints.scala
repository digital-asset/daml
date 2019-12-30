// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.ws.{Message, UpgradeToWebSocket}
import akka.stream.Materializer
import akka.stream.scaladsl.Flow
import com.digitalasset.jwt.domain.Jwt
import com.digitalasset.ledger.api.refinements.{ApiTypes => lar}
import com.typesafe.scalalogging.StrictLogging
import scalaz.syntax.std.boolean._
import scalaz.syntax.std.option._
import scalaz.\/
import scala.concurrent.{ExecutionContext, Future}
import EndpointsCompanion._

object WebsocketEndpoints {
  private[http] val tokenPrefix: String = "jwt.token."
  private[http] val wsProtocol: String = "daml.ws.auth"
  type WebSocketHandler =
    (Jwt, domain.JwtPayload, UpgradeToWebSocket, Option[String]) => HttpResponse

  private def findJwtFromSubProtocol(
      upgradeToWebSocket: UpgradeToWebSocket): Unauthorized \/ Jwt = {
    upgradeToWebSocket.requestedProtocols
      .collectFirst {
        case p if p startsWith tokenPrefix => Jwt(p drop tokenPrefix.length)
      }
      .toRightDisjunction(Unauthorized(s"Missing required $tokenPrefix.[token] in subprotocol"))
  }

  private def preconnect(
      decodeJwt: ValidateJwt,
      req: UpgradeToWebSocket,
      subprotocol: Option[String]) =
    for {
      _ <- subprotocol.exists(req.requestedProtocols.contains(_)) either (()) or Unauthorized(
        s"Missing required $tokenPrefix.[token] or $wsProtocol subprotocol")
      jwt0 <- findJwtFromSubProtocol(req)
      payload <- decodeAndParsePayload(jwt0, decodeJwt)
    } yield payload
}

class WebsocketEndpoints(
    ledgerId: lar.LedgerId,
    decodeJwt: ValidateJwt,
    webSocketService: WebSocketService)(implicit mat: Materializer, ec: ExecutionContext)
    extends StrictLogging {

  import WebsocketEndpoints._

  lazy val transactionWebSocket: PartialFunction[HttpRequest, Future[HttpResponse]] = {
    case req @ HttpRequest(GET, Uri.Path("/transactions"), _, _, _) =>
      Future.successful(
        (for {
          upgradeReq <- req.header[UpgradeToWebSocket] \/> InvalidUserInput(
            s"Cannot upgrade client's connection to websocket")
          _ = logger.info(s"GOT $wsProtocol")
          subprotocol = Some(wsProtocol)
          payload <- preconnect(decodeJwt, upgradeReq, subprotocol)
          (jwt, jwtPayload) = payload
        } yield handleWebsocketRequest(jwt, jwtPayload, upgradeReq, subprotocol))
          .leftMap(httpResponseError)
          .merge)
  }

  private def handleWebsocketRequest(
      jwt: Jwt,
      jwtPayload: domain.JwtPayload,
      req: UpgradeToWebSocket,
      protocol: Option[String]): HttpResponse = {
    val handler: Flow[Message, Message, _] =
      webSocketService.transactionMessageHandler(jwt, jwtPayload)
    req.handleMessages(handler, protocol)
  }

}
