// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http

import com.daml.jwt.Jwt
import com.daml.logging.LoggingContextOf
import com.daml.metrics.api.MetricsContext
import com.daml.metrics.pekkohttp.{MetricLabelsExtractor, WebSocketMetricsInterceptor}
import com.digitalasset.canton.http.EndpointsCompanion.*
import com.digitalasset.canton.http.metrics.HttpApiMetrics
import com.digitalasset.canton.http.util.Logging.{
  InstanceUUID,
  RequestID,
  extendWithRequestIdLogCtx,
}
import com.digitalasset.canton.http.{ContractKeyStreamRequest, JwtPayload, SearchForeverRequest}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.NoTracing
import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.http.scaladsl.model.HttpMethods.*
import org.apache.pekko.http.scaladsl.model.ws.{Message, WebSocketUpgrade}
import org.apache.pekko.http.scaladsl.server.RouteResult.{Complete, Rejected}
import org.apache.pekko.http.scaladsl.server.{Rejection, RequestContext, Route, RouteResult}
import org.apache.pekko.stream.scaladsl.Flow
import scalaz.std.scalaFuture.*
import scalaz.syntax.std.boolean.*
import scalaz.syntax.std.option.*
import scalaz.{EitherT, \/}

import scala.concurrent.{ExecutionContext, Future}

object WebsocketEndpoints {
  val tokenPrefix: String = "jwt.token."
  val wsProtocol: String = "daml.ws.auth"

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
      resolveUser: ResolveUser,
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
        resolveUser,
      ).leftMap(it => it: Error)
    } yield payload
}

class WebsocketEndpoints(
    decodeJwt: ValidateJwt,
    webSocketService: WebSocketService,
    resolveUser: ResolveUser,
    val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging
    with NoTracing {

  import WebsocketEndpoints.*

  def transactionWebSocket(implicit
      lc: LoggingContextOf[InstanceUUID],
      metrics: HttpApiMetrics,
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
                _ = logger.info(s"GOT $wsProtocol ${lc.makeString}")

                payload <- preconnect(
                  decodeJwt,
                  upgradeReq,
                  wsProtocol,
                  resolveUser,
                )
                (jwt, jwtPayload) = payload
              } yield {
                MetricsContext.withMetricLabels(MetricLabelsExtractor.labelsFromRequest(req)*) {
                  implicit mc: MetricsContext =>
                    handleWebsocketRequest[SearchForeverRequest](
                      jwt,
                      jwtPayload,
                      upgradeReq,
                      wsProtocol,
                    )
                }
              })
                .valueOr(httpResponseError(_, logger))
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
                  resolveUser,
                )
                (jwt, jwtPayload) = payload
              } yield {
                MetricsContext.withMetricLabels(MetricLabelsExtractor.labelsFromRequest(req)*) {
                  implicit mc: MetricsContext =>
                    handleWebsocketRequest[ContractKeyStreamRequest[_, _]](
                      jwt,
                      jwtPayload,
                      upgradeReq,
                      wsProtocol,
                    )
                }
              })
                .valueOr(httpResponseError(_, logger))
        )
    }
    import scalaz.std.partialFunction.*
    import scalaz.syntax.arrow.*
    dispatch
      .&&& { case r => r }
      .andThen { case (lcFhr, req) =>
        extendWithRequestIdLogCtx { implicit lc =>
          logger.trace(s"Incoming request on ${req.uri}, ${lc.makeString}")
          lcFhr(lc) map Complete.apply
        }
      }
      .applyOrElse[HttpRequest, Future[RouteResult]](
        ctx.request,
        _ => Future(Rejected(Seq.empty[Rejection])),
      )
  }

  def handleWebsocketRequest[A: WebSocketService.StreamRequestParser](
      jwt: Jwt,
      jwtPayload: JwtPayload,
      req: WebSocketUpgrade,
      protocol: String,
  )(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      metrics: HttpApiMetrics,
      mc: MetricsContext,
  ): HttpResponse = {
    val handler: Flow[Message, Message, _] =
      WebSocketMetricsInterceptor.withRateSizeMetrics(
        metrics.websocket,
        webSocketService.transactionMessageHandler[A](jwt, jwtPayload),
      )
    req.handleMessages(handler, Some(protocol))
  }
}
