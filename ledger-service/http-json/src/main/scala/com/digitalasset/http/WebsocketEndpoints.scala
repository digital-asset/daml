// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.ws.{Message, UpgradeToWebSocket}
import akka.stream.Materializer
import akka.stream.scaladsl.Flow
import com.daml.jwt.domain.Jwt
import com.daml.ledger.api.refinements.{ApiTypes => lar}
import com.typesafe.scalalogging.StrictLogging
import scalaz.syntax.std.boolean._
import scalaz.syntax.std.option._
import scalaz.\/

import scala.concurrent.{ExecutionContext, Future}
import EndpointsCompanion._

object WebsocketEndpoints {
  private[http] val tokenPrefix: String = "jwt.token."
  private[http] val wsProtocol: String = "daml.ws.auth"

  private def findJwtFromSubProtocol(
      upgradeToWebSocket: UpgradeToWebSocket,
  ): Unauthorized \/ Jwt = {
    upgradeToWebSocket.requestedProtocols
      .collectFirst {
        case p if p startsWith tokenPrefix => Jwt(p drop tokenPrefix.length)
      }
      .toRightDisjunction(Unauthorized(s"Missing required $tokenPrefix.[token] in subprotocol"))
  }

  private def preconnect(
      decodeJwt: ValidateJwt,
      req: UpgradeToWebSocket,
      subprotocol: String,
  ) =
    for {
      _ <- req.requestedProtocols.contains(subprotocol) either (()) or Unauthorized(
        s"Missing required $tokenPrefix.[token] or $wsProtocol subprotocol",
      )
      jwt0 <- findJwtFromSubProtocol(req)
      payload <- decodeAndParsePayload(jwt0, decodeJwt)
    } yield payload
}

class WebsocketEndpoints(
    ledgerId: lar.LedgerId,
    decodeJwt: ValidateJwt,
    webSocketService: WebSocketService,
)(implicit mat: Materializer, ec: ExecutionContext)
    extends StrictLogging {

  import WebsocketEndpoints._

  lazy val transactionWebSocket: PartialFunction[HttpRequest, Future[HttpResponse]] = {
    case req @ HttpRequest(GET, Uri.Path("/v1/stream/query"), _, _, _) =>
      Future.successful(
        (for {
          upgradeReq <- req.header[UpgradeToWebSocket] \/> InvalidUserInput(
            s"Cannot upgrade client's connection to websocket",
          )
          _ = logger.info(s"GOT $wsProtocol")

          payload <- preconnect(decodeJwt, upgradeReq, wsProtocol)
          (jwt, jwtPayload) = payload
        } yield
          handleWebsocketRequest[domain.SearchForeverRequest](
            jwt,
            jwtPayload,
            upgradeReq,
            wsProtocol))
          .valueOr(httpResponseError),
      )

    case req @ HttpRequest(GET, Uri.Path("/v1/stream/fetch"), _, _, _) =>
      Future.successful(
        (for {
          upgradeReq <- req.header[UpgradeToWebSocket] \/> InvalidUserInput(
            s"Cannot upgrade client's connection to websocket",
          )
          payload <- preconnect(decodeJwt, upgradeReq, wsProtocol)
          (jwt, jwtPayload) = payload
        } yield
          handleWebsocketRequest[domain.ContractKeyStreamRequest[_, _]](
            jwt,
            jwtPayload,
            upgradeReq,
            wsProtocol))
          .valueOr(httpResponseError),
      )
  }

  def handleWebsocketRequest[A: WebSocketService.StreamQueryReader](
      jwt: Jwt,
      jwtPayload: domain.JwtPayload,
      req: UpgradeToWebSocket,
      protocol: String,
  ): HttpResponse = {
    val handler: Flow[Message, Message, _] =
      webSocketService.transactionMessageHandler[A](jwt, jwtPayload)
    req.handleMessages(handler, Some(protocol))
  }
}
