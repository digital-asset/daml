// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.ws.{Message, WebSocketUpgrade}
import akka.stream.scaladsl.Flow
import com.daml.jwt.domain.Jwt
import scalaz.syntax.std.boolean._
import scalaz.syntax.std.option._
import scalaz.\/

import scala.concurrent.{ExecutionContext, Future}
import EndpointsCompanion._
import akka.http.scaladsl.server.{Rejection, RequestContext, Route, RouteResult}
import akka.http.scaladsl.server.RouteResult.{Complete, Rejected}
import com.daml.http.domain.JwtPayload
import com.daml.http.util.Logging.{InstanceUUID, RequestID, extendWithRequestIdLogCtx}
import com.daml.logging.{ContextualizedLogger, LoggingContextOf}
import com.daml.metrics.Metrics

import scala.collection.immutable.Seq

object WebsocketEndpoints {
  private[http] val tokenPrefix: String = "jwt.token."
  private[http] val wsProtocol: String = "daml.ws.auth"

  private def findJwtFromSubProtocol[Err >: Unauthorized](
      upgradeToWebSocket: WebSocketUpgrade
  ): Err \/ Jwt =
    upgradeToWebSocket.requestedProtocols
      .collectFirst {
        case p if p startsWith tokenPrefix => Jwt(p drop tokenPrefix.length)
      }
      .toRightDisjunction(Unauthorized(s"Missing required $tokenPrefix.[token] in subprotocol"))

  private def preconnect[Err >: Unauthorized](
      decodeJwt: ValidateJwt,
      req: WebSocketUpgrade,
      subprotocol: String,
  ): Err \/ (Jwt, JwtPayload) =
    for {
      _ <- req.requestedProtocols.contains(subprotocol) either (()) or (Unauthorized(
        s"Missing required $tokenPrefix.[token] or $wsProtocol subprotocol"
      ): Err)
      jwt0 <- findJwtFromSubProtocol[Err](req)
      payload <- decodeAndParsePayload[JwtPayload](jwt0, decodeJwt)
    } yield payload
}

class WebsocketEndpoints(
    decodeJwt: ValidateJwt,
    webSocketService: WebSocketService,
)(implicit ec: ExecutionContext) {

  import WebsocketEndpoints._

  private[this] val logger = ContextualizedLogger.get(getClass)

  def transactionWebSocket(implicit
      lc: LoggingContextOf[InstanceUUID],
      metrics: Metrics,
  ): Route = { (ctx: RequestContext) =>
    val dispatch: PartialFunction[HttpRequest, LoggingContextOf[
      InstanceUUID with RequestID
    ] => Future[HttpResponse]] = {
      case req @ HttpRequest(GET, Uri.Path("/v1/stream/query"), _, _, _) =>
        (
            implicit lc =>
              Future.successful(
                (for {
                  upgradeReq <- req.attribute(AttributeKeys.webSocketUpgrade) \/> (InvalidUserInput(
                    "Cannot upgrade client's connection to websocket"
                  ): Error)
                  _ = logger.info(s"GOT $wsProtocol")

                  payload <- preconnect[Error](decodeJwt, upgradeReq, wsProtocol)
                  (jwt, jwtPayload) = payload
                } yield handleWebsocketRequest[domain.SearchForeverRequest](
                  jwt,
                  jwtPayload,
                  upgradeReq,
                  wsProtocol,
                ))
                  .valueOr(httpResponseError)
              )
        )

      case req @ HttpRequest(GET, Uri.Path("/v1/stream/fetch"), _, _, _) =>
        (
            implicit lc =>
              Future.successful(
                (for {
                  upgradeReq <- req.attribute(AttributeKeys.webSocketUpgrade) \/> (InvalidUserInput(
                    s"Cannot upgrade client's connection to websocket"
                  ): Error)
                  payload <- preconnect[Error](decodeJwt, upgradeReq, wsProtocol)
                  (jwt, jwtPayload) = payload
                } yield handleWebsocketRequest[domain.ContractKeyStreamRequest[_, _]](
                  jwt,
                  jwtPayload,
                  upgradeReq,
                  wsProtocol,
                ))
                  .valueOr(httpResponseError)
              )
        )
    }
    import scalaz.std.partialFunction._, scalaz.syntax.arrow._
    dispatch
      .&&& { case r => r }
      .andThen { case (lcFhr, req) =>
        extendWithRequestIdLogCtx(implicit lc => {
          logger.trace(s"Incoming request on ${req.uri}")
          lcFhr(lc) map Complete
        })
      }
      .applyOrElse[HttpRequest, Future[RouteResult]](
        ctx.request,
        _ => Future(Rejected(Seq.empty[Rejection])),
      )
  }

  def handleWebsocketRequest[A: WebSocketService.StreamQueryReader](
      jwt: Jwt,
      jwtPayload: domain.JwtPayload,
      req: WebSocketUpgrade,
      protocol: String,
  )(implicit lc: LoggingContextOf[InstanceUUID with RequestID], metrics: Metrics): HttpResponse = {
    val handler: Flow[Message, Message, _] =
      webSocketService.transactionMessageHandler[A](jwt, jwtPayload)
    req.handleMessages(handler, Some(protocol))
  }
}
