// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.ws.{Message, WebSocketUpgrade}
import akka.stream.scaladsl.Flow
import com.daml.jwt.domain.Jwt
import scalaz.syntax.std.boolean._
import scalaz.syntax.std.option._
import scalaz.{EitherT, \/}

import scala.concurrent.{ExecutionContext, Future}
import EndpointsCompanion._
import akka.http.scaladsl.server.{Rejection, RequestContext, Route, RouteResult}
import akka.http.scaladsl.server.RouteResult.{Complete, Rejected}
import com.daml.http.domain.JwtPayload
import com.daml.http.util.Logging.{InstanceUUID, RequestID, extendWithRequestIdLogCtx}
import com.daml.ledger.client.services.admin.UserManagementClient
import com.daml.ledger.client.services.identity.LedgerIdentityClient
import com.daml.logging.{ContextualizedLogger, LoggingContextOf}
import com.daml.metrics.Metrics
import com.daml.metrics.api.MetricsContext
import com.daml.metrics.akkahttp.WebSocketMetrics

import scala.collection.immutable.Seq
import scalaz.std.scalaFuture._

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

  private def preconnect(
      decodeJwt: ValidateJwt,
      req: WebSocketUpgrade,
      subprotocol: String,
      userManagementClient: UserManagementClient,
      ledgerIdentityClient: LedgerIdentityClient,
  )(implicit ec: ExecutionContext): EitherT[Future, Error, (Jwt, JwtPayload)] =
    for {
      _ <- EitherT.either(
        req.requestedProtocols.contains(subprotocol) either (()) or (Unauthorized(
          s"Missing required $tokenPrefix.[token] or $wsProtocol subprotocol"
        ): Error)
      )
      jwt0 <- EitherT.either(findJwtFromSubProtocol[Error](req))
      payload <- decodeAndParsePayload[JwtPayload](
        jwt0,
        decodeJwt,
        userManagementClient,
        ledgerIdentityClient,
      ).leftMap(it => it: Error)
    } yield payload
}

class WebsocketEndpoints(
    decodeJwt: ValidateJwt,
    webSocketService: WebSocketService,
    userManagementClient: UserManagementClient,
    ledgerIdentityClient: LedgerIdentityClient,
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
              (for {
                upgradeReq <- EitherT.either(
                  req.attribute(AttributeKeys.webSocketUpgrade) \/> (InvalidUserInput(
                    "Cannot upgrade client's connection to websocket"
                  ): Error)
                )
                _ = logger.info(s"GOT $wsProtocol")

                payload <- preconnect(
                  decodeJwt,
                  upgradeReq,
                  wsProtocol,
                  userManagementClient,
                  ledgerIdentityClient,
                )
                (jwt, jwtPayload) = payload
              } yield handleWebsocketRequest[domain.SearchForeverRequest](
                jwt,
                jwtPayload,
                upgradeReq,
                wsProtocol,
              ))
                .valueOr(httpResponseError)
        )

      case req @ HttpRequest(GET, Uri.Path("/v1/stream/fetch"), _, _, _) =>
        (
            implicit lc =>
              (for {
                upgradeReq <- EitherT.either(
                  req.attribute(AttributeKeys.webSocketUpgrade) \/> (InvalidUserInput(
                    s"Cannot upgrade client's connection to websocket"
                  ): Error)
                )
                payload <- preconnect(
                  decodeJwt,
                  upgradeReq,
                  wsProtocol,
                  userManagementClient,
                  ledgerIdentityClient,
                )
                (jwt, jwtPayload) = payload
              } yield handleWebsocketRequest[domain.ContractKeyStreamRequest[_, _]](
                jwt,
                jwtPayload,
                upgradeReq,
                wsProtocol,
              ))
                .valueOr(httpResponseError)
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

  def handleWebsocketRequest[A: WebSocketService.StreamRequestParser](
      jwt: Jwt,
      jwtPayload: domain.JwtPayload,
      req: WebSocketUpgrade,
      protocol: String,
  )(implicit lc: LoggingContextOf[InstanceUUID with RequestID], metrics: Metrics): HttpResponse = {
    val handler: Flow[Message, Message, _] =
      WebSocketMetrics.withGoldenSignalsMetrics(
        metrics.daml.HttpJsonApi.websocketReceivedTotal,
        metrics.daml.HttpJsonApi.websocketReceivedBytesTotal,
        metrics.daml.HttpJsonApi.websocketSentTotal,
        metrics.daml.HttpJsonApi.websocketSentBytesTotal,
        webSocketService.transactionMessageHandler[A](jwt, jwtPayload),
      )(MetricsContext.Empty)
    req.handleMessages(handler, Some(protocol))
  }
}
