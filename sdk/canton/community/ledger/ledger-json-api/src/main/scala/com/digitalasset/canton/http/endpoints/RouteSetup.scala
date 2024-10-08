// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.endpoints

import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.http.scaladsl.model.headers.{
  Authorization,
  ModeledCustomHeader,
  ModeledCustomHeaderCompanion,
  OAuth2BearerToken,
  `X-Forwarded-Proto`,
}
import org.apache.pekko.stream.Materializer
import com.digitalasset.canton.http.Endpoints.ET
import com.digitalasset.canton.http.EndpointsCompanion.*
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString
import com.daml.logging.LoggingContextOf.withEnrichedLoggingContext
import com.daml.scalautil.Statement.discard
import com.digitalasset.canton.http.domain.{JwtPayloadG, JwtPayloadTag, JwtWritePayload}
import com.digitalasset.canton.http.json.*
import com.digitalasset.canton.http.Endpoints
import com.digitalasset.canton.http.util.FutureUtil.{either, eitherT}
import com.digitalasset.canton.http.util.Logging.{InstanceUUID, RequestID}
import com.daml.jwt.Jwt
import com.daml.ledger.api.v2 as lav2
import lav2.value.Value as ApiValue
import scalaz.std.scalaFuture.*
import scalaz.syntax.std.option.*
import scalaz.{-\/, EitherT, Traverse, \/, \/-}
import spray.json.*

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try
import com.digitalasset.canton.ledger.client.services.admin.UserManagementClient
import com.daml.logging.LoggingContextOf
import com.daml.metrics.api.MetricHandle.Timer.TimerHandle
import com.digitalasset.canton.http.{EndpointsCompanion, domain}
import com.digitalasset.canton.http.metrics.HttpApiMetrics
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.{NoTracing, TraceContext, W3CTraceContext}

private[http] final class RouteSetup(
    allowNonHttps: Boolean,
    decodeJwt: EndpointsCompanion.ValidateJwt,
    encoder: DomainJsonEncoder,
    userManagementClient: UserManagementClient,
    maxTimeToCollectRequest: FiniteDuration,
    val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext, mat: Materializer)
    extends NamedLogging
    with NoTracing {
  import RouteSetup.*
  import encoder.implicits.*
  import com.digitalasset.canton.http.util.ErrorOps.*

  private[endpoints] def handleCommand[T[_]](req: HttpRequest)(
      fn: (
          Jwt,
          JwtWritePayload,
          JsValue,
          TimerHandle,
      ) => TraceContext=> LoggingContextOf[JwtPayloadTag with InstanceUUID with RequestID] => ET[
        T[ApiValue]
      ]
  )(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      ev1: JsonWriter[T[JsValue]],
      ev2: Traverse[T],
      metrics: HttpApiMetrics,
  ): ET[domain.SyncResponse[JsValue]] = {
    val traceContextOption =
      W3CTraceContext.fromHeaders(req.headers.map(header => (header.name(), header.value())).toMap)
    implicit val traceContext = traceContextOption.map(_.toTraceContext).getOrElse(TraceContext.empty)
    for {
      parseAndDecodeTimerCtx <- getParseAndDecodeTimerCtx()
      t3 <- inputJsValAndJwtPayload(req): ET[(Jwt, JwtWritePayload, JsValue)]
      (jwt, jwtPayload, reqBody) = t3
      resp <- withJwtPayloadLoggingContext(jwtPayload)(
        fn(jwt, jwtPayload, reqBody, parseAndDecodeTimerCtx)
      )
      jsVal <- either(SprayJson.encode1(resp).liftErr(ServerError.fromMsg)): ET[JsValue]
    } yield domain.OkResponse(jsVal)
  }

  def inputJsValAndJwtPayload[P](req: HttpRequest)(implicit
      createFromUserToken: CreateFromUserToken[P],
      lc: LoggingContextOf[InstanceUUID with RequestID],
  ): EitherT[Future, Error, (Jwt, P, JsValue)] =
    inputJsVal(req).flatMap(x => withJwtPayload[JsValue, P](x).leftMap(it => it: Error))

  def withJwtPayload[A, P](fa: (Jwt, A))(implicit
      createFromUserToken: CreateFromUserToken[P]
  ): EitherT[Future, Error, (Jwt, P, A)] =
    decodeAndParsePayload[P](fa._1, decodeJwt, userManagementClient).map(t2 =>
      (t2._1, t2._2, fa._2)
    )

  def inputAndJwtPayload[P](
      req: HttpRequest
  )(implicit
      createFromUserToken: CreateFromUserToken[P],
      lc: LoggingContextOf[InstanceUUID with RequestID],
  ): EitherT[Future, Error, (Jwt, P, String)] =
    eitherT(input(req)).flatMap(it => withJwtPayload[String, P](it))

  def getParseAndDecodeTimerCtx()(implicit
      metrics: HttpApiMetrics
  ): ET[TimerHandle] =
    EitherT.pure(metrics.incomingJsonParsingAndValidationTimer.startAsync())

  def input(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): Future[Error \/ (Jwt, String)] = {
    findJwt(req) match {
      case e @ -\/(_) =>
        discard { req.entity.discardBytes(mat) }
        Future.successful(e)
      case \/-(j) =>
        data(req.entity).map(d => \/-((j, d)))
    }
  }

  def inputSource(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): Error \/ (Jwt, Source[ByteString, Any]) =
    findJwt(req) match {
      case e @ -\/(_) =>
        discard { req.entity.discardBytes(mat) }
        e
      case \/-(j) =>
        \/.right((j, req.entity.dataBytes))

    }

  private[this] def data(entity: RequestEntity): Future[String] =
    entity.toStrict(maxTimeToCollectRequest).map(_.data.utf8String)

  def inputJsVal(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): ET[(Jwt, JsValue)] =
    for {
      t2 <- eitherT(input(req)): ET[(Jwt, String)]
      jsVal <- either(SprayJson.parse(t2._2).liftErr(InvalidUserInput.apply)): ET[JsValue]
    } yield (t2._1, jsVal)

  def findJwt(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): Unauthorized \/ Jwt =
    ensureHttpsForwarded(req) flatMap { _ =>
      req.headers
        .collectFirst { case Authorization(OAuth2BearerToken(token)) =>
          Jwt(token)
        }
        .toRightDisjunction(
          Unauthorized("missing Authorization header with OAuth 2.0 Bearer Token")
        )
    }

  private def isHttps(req: HttpRequest): Boolean = req.uri.scheme == "https"

  private[this] def ensureHttpsForwarded(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): Unauthorized \/ Unit =
    if (allowNonHttps || isForwardedForHttps(req.headers) || isHttps(req)) \/-(())
    else {
      logger.warn(s"$nonHttpsErrorMessage, ${lc.makeString}")
      \/-(())
    }
}

object RouteSetup {
  import Endpoints.IntoEndpointsError

  private val nonHttpsErrorMessage =
    "missing HTTPS reverse-proxy request headers; for development launch with --allow-insecure-tokens"

  def withJwtPayloadLoggingContext[A](jwtPayload: JwtPayloadG)(
      fn: TraceContext => LoggingContextOf[JwtPayloadTag with InstanceUUID with RequestID] => A
  )(implicit lc: LoggingContextOf[InstanceUUID with RequestID], traceContext: TraceContext): A =
    withEnrichedLoggingContext(
      LoggingContextOf.label[JwtPayloadTag],
      "act_as" -> jwtPayload.actAs.toString,
      "application_id" -> jwtPayload.applicationId.toString,
      "read_as" -> jwtPayload.readAs.toString,
    ).run(fn(traceContext))

  def handleFutureFailure[A](fa: Future[A])(implicit
      ec: ExecutionContext
  ): Future[Error \/ A] =
    fa.map(a => \/-(a)).recover(Error.fromThrowable andThen (-\/(_)))

  def handleFutureEitherFailure[A, B](fa: Future[A \/ B])(implicit
      ec: ExecutionContext,
      A: IntoEndpointsError[A],
  ): Future[Error \/ B] =
    fa.map(_ leftMap A.run).recover(Error.fromThrowable andThen (-\/(_)))

  private def isForwardedForHttps(headers: Seq[HttpHeader]): Boolean =
    headers exists {
      case `X-Forwarded-Proto`(protocol) => protocol equalsIgnoreCase "https"
      // the whole "custom headers" thing in pekko-http is a mishmash of
      // actually using the ModeledCustomHeaderCompanion stuff (which works)
      // and "just use ClassTag YOLO" (which won't work)
      case Forwarded(value) => Forwarded(value).proto contains "https"
      case _ => false
    }

  // avoid case class to avoid using the wrong unapply in isForwardedForHttps
  private[endpoints] final class Forwarded(override val value: String)
      extends ModeledCustomHeader[Forwarded] {
    override def companion = Forwarded
    override def renderInRequests = true
    override def renderInResponses = false
    // per discussion https://github.com/digital-asset/daml/pull/5660#discussion_r412539107
    def proto: Option[String] =
      Forwarded.re findFirstMatchIn value map (_.group(1).toLowerCase)
  }

  private[endpoints] object Forwarded extends ModeledCustomHeaderCompanion[Forwarded] {
    override val name = "Forwarded"
    override def parse(value: String) = Try(new Forwarded(value))
    private val re = raw"""(?i)proto\s*=\s*"?(https?)""".r
  }
}
