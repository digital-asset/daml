// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

//TODO (i19539) repackage eventually
import com.daml.error.{ContextualizedErrorLogger, NoLogging}
import com.daml.error.utils.DecodedCantonError
import com.digitalasset.canton.http.json.v2.JsSchema.JsCantonError
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors.InvalidArgument
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.tracing.{TraceContext, W3CTraceContext}
import io.circe.{Decoder, Encoder}
import io.grpc.StatusRuntimeException
import org.apache.pekko.stream.scaladsl.{Flow, Source}
import org.apache.pekko.util
import org.apache.pekko.util.ByteString
import sttp.capabilities.WebSockets
import sttp.capabilities.pekko.PekkoStreams
import sttp.model.Header
import sttp.tapir.generic.auto.*
import sttp.tapir.json.circe.*
import sttp.tapir.server.ServerEndpoint.Full
import sttp.tapir.*
import com.digitalasset.transcode.{MissingFieldException, UnexpectedFieldsException}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

trait Endpoints extends NamedLogging {
  case class Jwt(token: String)

  // added to ease burden if we change what is included in SECURITY_INPUT
  case class CallerContext(jwt: Option[Jwt]) {
    def token(): Option[String] = jwt.map(_.token)
  }

  case class TracedInput[A](in: A, traceContext: TraceContext)

  lazy val baseEndpoint: Endpoint[CallerContext, Unit, Unit, Unit, Any] = endpoint
    .securityIn(
      auth
        .bearer[Option[String]]()
        .map(bearer => bearer.map(Jwt.apply))(
          _.map(_.token)
        )
        .description("Ledger API standard JWT token")
        .and(
          auth
            .apiKey(header[Option[String]]("Sec-WebSocket-Protocol"))
            .map { bearer =>
              bearer.map(Jwt.apply)
            }(_.map(_.token))
            .description("Ledger API standard JWT token (websocket)")
        )
        .map(tokens => CallerContext(tokens._1.orElse(tokens._2)))(cc => (cc.jwt, cc.jwt))
    )

  lazy val v2Endpoint: Endpoint[CallerContext, Unit, JsCantonError, Unit, Any] = baseEndpoint
    .errorOut(jsonBody[JsCantonError])
    .in("v2")

  private val wsSubprotocol = sttp.model.Header("Sec-WebSocket-Protocol", "daml.ws.auth")

  protected def handleErrorResponse[R](implicit
      traceContext: TraceContext
  ): Try[Either[JsCantonError, R]] => Try[Either[JsCantonError, R]] = {
    case Failure(sre: StatusRuntimeException) =>
      Success(
        Left(
          JsCantonError.fromDecodedCantonError(
            DecodedCantonError
              .fromStatusRuntimeException(sre)
              .getOrElse(
                throw new RuntimeException(
                  "Failed to convert response to JsCantonError."
                )
              )
          )
        )
      )
    case Success(value) => Success(value)
    // TODO (i19398): Handle
    case Failure(unhandled) =>
      unhandled match {
        case unexpected: UnexpectedFieldsException =>
          Success(
            Left(
              JsCantonError.fromErrorCode(
                InvalidArgument.Reject(
                  s"Unexpected fields: ${unexpected.unexpectedFields.mkString}"
                )
              )
            )
          )
        case fieldMissing: MissingFieldException =>
          Success(
            Left(
              JsCantonError.fromErrorCode(
                InvalidArgument.Reject(
                  s"Missing field: $fieldMissing"
                )
              )
            )
          )
        case _ => Failure(unhandled)
      }

  }

  def uploadByteString(
      endpoint: Endpoint[CallerContext, Unit, JsCantonError, Unit, Any],
      service: CallerContext => TracedInput[Source[util.ByteString, Any]] => Future[Unit],
  ): Full[CallerContext, CallerContext, TracedInput[Source[
    ByteString,
    Any,
  ]], JsCantonError, Unit, PekkoStreams, Future] =
    endpoint.post
      .in(streamBinaryBody(PekkoStreams)(CodecFormat.OctetStream()))
      .in(headers)
      .mapIn(traceHeadersMapping[Source[util.ByteString, Any]]())
      .serverSecurityLogicSuccess(Future.successful)
      .serverLogic(caller =>
        tracedInput =>
          service(caller)(tracedInput)
            .map(Right(_))(ExecutionContext.parasitic)
            .transform(handleErrorResponse(tracedInput.traceContext))(ExecutionContext.parasitic)
      )

  def downloadByteString[I](
      endpoint: Endpoint[CallerContext, I, JsCantonError, Unit, Any],
      service: CallerContext => TracedInput[I] => Future[PekkoStreams.BinaryStream],
  ): Full[CallerContext, CallerContext, TracedInput[I], JsCantonError, Source[
    ByteString,
    Any,
  ], Any with PekkoStreams, Future] =
    endpoint.get
      .in(headers)
      .mapIn(traceHeadersMapping[I]())
      .serverSecurityLogicSuccess(Future.successful)
      .out(streamBinaryBody(PekkoStreams)(CodecFormat.OctetStream()))
      .serverLogic(jwt =>
        i =>
          service(jwt)(i).resultToRight
            .transform(handleErrorResponse(i.traceContext))(
              ExecutionContext.parasitic
            )
      )

  def jsonWithBody[I: Decoder: Encoder: Schema, R: Decoder: Encoder: Schema, P](
      endpoint: Endpoint[CallerContext, P, JsCantonError, Unit, Any],
      service: CallerContext => (TracedInput[P], I) => Future[Either[JsCantonError, R]],
  ): Full[CallerContext, CallerContext, (TracedInput[P], I), JsCantonError, R, Any, Future] =
    endpoint
      .in(headers)
      .mapIn(traceHeadersMapping[P]())
      .in(jsonBody[I])
      .serverSecurityLogicSuccess(Future.successful)
      .out(jsonBody[R])
      .serverLogic { callerContext => i =>
        service(callerContext)
          .tupled(i)
          .transform(handleErrorResponse(i._1.traceContext))(ExecutionContext.parasitic)
      }

  def json[R: Decoder: Encoder: Schema, P](
      endpoint: Endpoint[CallerContext, P, JsCantonError, Unit, Any],
      service: CallerContext => TracedInput[P] => Future[Either[JsCantonError, R]],
  ): Full[CallerContext, CallerContext, TracedInput[P], JsCantonError, R, Any, Future] =
    endpoint
      .in(headers)
      .mapIn(traceHeadersMapping[P]())
      .serverSecurityLogicSuccess(Future.successful)
      .out(jsonBody[R])
      .serverLogic(callerContext =>
        i =>
          service(callerContext)(i)
            .transform(handleErrorResponse(i.traceContext))(ExecutionContext.parasitic)
      )

  protected def websocket[HI, I: Decoder: Encoder: Schema, O: Decoder: Encoder: Schema](
      endpoint: Endpoint[CallerContext, HI, JsCantonError, Unit, Any],
      service: CallerContext => TracedInput[HI] => Flow[I, O, Any],
  ): Full[CallerContext, CallerContext, HI, JsCantonError, Flow[
    I,
    O,
    Any,
  ], Any with PekkoStreams with WebSockets, Future] =
    endpoint
      // .in(header(wsSubprotocol))  We send wsSubprotocol header, but we do not enforce it
      .out(header(wsSubprotocol))
      .serverSecurityLogicSuccess(Future.successful)
      .out(webSocketBody[I, CodecFormat.Json, O, CodecFormat.Json](PekkoStreams))
      // TODO(i19398): Handle error result
      // TODO(i19103)  decide if tracecontext headers on websockets are handled
      .serverLogicSuccess { jwt => i =>
        Future.successful(service(jwt)(TracedInput(i, TraceContext.empty)))
      }

  def traceHeadersMapping[I]() = new Mapping[(I, List[sttp.model.Header]), TracedInput[I]] {

    override def rawDecode(input: (I, List[Header])): DecodeResult[TracedInput[I]] =
      DecodeResult.Value(
        TracedInput(
          input._1,
          W3CTraceContext
            .fromHeaders(input._2.map(h => (h.name, h.value)).toMap)
            .map(_.toTraceContext)
            .getOrElse(TraceContext.empty),
        )
      )

    override def encode(h: TracedInput[I]): (I, List[Header]) =
      (
        h.in,
        W3CTraceContext.extractHeaders(h.traceContext).map { case (k, v) => Header(k, v) }.toList,
      )

    override def validator: Validator[TracedInput[I]] = Validator.pass
  }

  protected def withTraceHeaders[P, E](
      endpoint: Endpoint[CallerContext, P, E, Unit, Any]
  ) =
    endpoint.in(headers).mapIn(traceHeadersMapping[P]())

  implicit class FutureOps[R](future: Future[R]) {
    implicit val executionContext: ExecutionContext = ExecutionContext.parasitic
    implicit val traceContext: TraceContext = TraceContext.empty
    implicit val errorLogged: ContextualizedErrorLogger = NoLogging
    def resultToRight: Future[Either[JsCantonError, R]] =
      future.map(Right(_))
  }

  def error[R](error: JsCantonError): Future[Either[JsCantonError, R]] =
    Future.successful(Left(error))

}
