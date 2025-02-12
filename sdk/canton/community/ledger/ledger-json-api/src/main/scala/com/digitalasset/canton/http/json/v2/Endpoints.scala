// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import com.daml.error.utils.DecodedCantonError
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.grpc.adapter.client.pekko.ClientAdapter
import com.digitalasset.canton.http.WebsocketConfig
import com.digitalasset.canton.http.json.v2.JsSchema.JsCantonError
import com.digitalasset.canton.ledger.error.LedgerApiErrors
import com.digitalasset.canton.ledger.error.groups.CommandExecutionErrors
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors.InvalidArgument
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.tracing.{TraceContext, W3CTraceContext}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.engine.Error.Preprocessing
import com.digitalasset.daml.lf.language.Ast.TVar
import com.digitalasset.daml.lf.value.Value.ValueUnit
import com.digitalasset.transcode.{MissingFieldException, UnexpectedFieldsException}
import io.circe.{Decoder, Encoder}
import io.grpc.StatusRuntimeException
import io.grpc.stub.StreamObserver
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Flow, Sink, Source}
import sttp.capabilities.WebSockets
import sttp.capabilities.pekko.PekkoStreams
import sttp.model.Header
import sttp.tapir.*
import sttp.tapir.generic.auto.*
import sttp.tapir.json.circe.*
import sttp.tapir.server.ServerEndpoint.Full

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future, TimeoutException}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

trait Endpoints extends NamedLogging {
  import Endpoints.*

  protected def handleErrorResponse[R](implicit
      traceContext: TraceContext
  ): Try[Either[JsCantonError, R]] => Try[Either[JsCantonError, R]] = {
    case Failure(sre: StatusRuntimeException) =>
      val error = JsCantonError.fromDecodedCantonError(
        DecodedCantonError
          .fromStatusRuntimeException(sre)
          .getOrElse(
            throw new RuntimeException(
              s"Failed to convert response to JsCantonError from ${sre.getMessage}",
              sre,
            ) // TODO (i19398) improve error handling in JSON (repeated code)
          )
      )
      Success(
        Left(error)
      )
    case Success(value) => Success(value)
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
                CommandExecutionErrors.Preprocessing.PreprocessingFailed.Reject(
                  // TODO (i19398) introduce JsonSpecific error subgroup
                  Preprocessing.TypeMismatch(
                    TVar(Ref.Name.assertFromString("unknown")),
                    ValueUnit,
                    s"Missing non-optional field: ${fieldMissing.missingField}",
                  )
                )
              )
            )
          )
        case _ =>
          val internalError =
            LedgerApiErrors.InternalError.Generic(unhandled.getMessage, Some(unhandled.getCause))

          Success(
            Left(
              JsCantonError.fromErrorCode(internalError)
            )
          )
      }
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

  protected def websocket[HI, I, O](
      endpoint: Endpoint[
        CallerContext,
        HI,
        JsCantonError,
        Flow[I, Either[JsCantonError, O], Any],
        PekkoStreams & WebSockets,
      ],
      service: CallerContext => TracedInput[HI] => Flow[I, O, Any],
  ): Full[CallerContext, CallerContext, HI, JsCantonError, Flow[
    I,
    Either[JsCantonError, O],
    Any,
  ], PekkoStreams & WebSockets, Future] =
    endpoint
      // .in(header(wsSubprotocol))  We send wsSubprotocol header, but we do not enforce it
      .out(header(wsSubprotocol))
      .serverSecurityLogicSuccess(Future.successful)
      // TODO(i19398): Handle error result
      // TODO(i19013)  decide if tracecontext headers on websockets are handled
      .serverLogicSuccess { jwt => i =>
        val errorHandlingService =
          service(jwt)(TracedInput(i, TraceContext.empty))
            .map(out =>
              Right[JsCantonError, O](out)
            ) // TODO(i19398): Try if it is practicable to deliver an error as CloseReason on websocket
            .recover(handleError)
        Future.successful(errorHandlingService)
      }

  def error[R](error: JsCantonError): Future[Either[JsCantonError, R]] =
    Future.successful(Left(error))

  private def maxRowsToReturn(requestLimit: Option[Long])(implicit wsConfig: WebsocketConfig) =
    Math.min(
      requestLimit.getOrElse(wsConfig.httpListMaxElementsLimit),
      wsConfig.httpListMaxElementsLimit,
    )

  def asList[INPUT, OUTPUT, R](
      endpoint: Endpoint[CallerContext, StreamList[INPUT], JsCantonError, Seq[
        OUTPUT
      ], R],
      service: CallerContext => TracedInput[Unit] => Flow[INPUT, OUTPUT, Any],
      timeoutOpenEndedStream: Boolean = false,
  )(implicit wsConfig: WebsocketConfig, materializer: Materializer) =
    endpoint
      .in(headers)
      .mapIn(traceHeadersMapping[StreamList[INPUT]]())
      .serverSecurityLogicSuccess(Future.successful)
      .serverLogic(caller =>
        (tracedInput: TracedInput[StreamList[INPUT]]) => {
          val flow = service(caller)(tracedInput.copy(in = ()))
          val limit = tracedInput.in.limit
          val idleWaitTime = tracedInput.in.waitTime
            .map(FiniteDuration.apply(_, TimeUnit.MILLISECONDS))
            .getOrElse(wsConfig.httpListWaitTime)
          val source = Source
            .single(tracedInput.in.input)
            .via(flow)
            .take(maxRowsToReturn(limit))
          (if (timeoutOpenEndedStream || tracedInput.in.waitTime.isDefined) {
             source
               .map(Some(_))
               .idleTimeout(idleWaitTime)
               .recover { case _: TimeoutException =>
                 None
               }
               .collect { case Some(elem) =>
                 elem
               }
           } else {
             source
           }).runWith(Sink.seq).resultToRight
        }
      )

  def withServerLogic[INPUT, OUTPUT, R](
      endpoint: Endpoint[CallerContext, INPUT, JsCantonError, OUTPUT, R],
      service: CallerContext => TracedInput[INPUT] => Future[Either[JsCantonError, OUTPUT]],
  ): Full[CallerContext, CallerContext, TracedInput[INPUT], JsCantonError, OUTPUT, R, Future] =
    endpoint
      .in(headers)
      .mapIn(traceHeadersMapping[INPUT]())
      .serverSecurityLogicSuccess(Future.successful)
      .serverLogic(caller =>
        tracedInput =>
          service(caller)(tracedInput)
            .transform(handleErrorResponse(tracedInput.traceContext))(ExecutionContext.parasitic)
      )

  protected def withTraceHeaders[P, E](
      endpoint: Endpoint[CallerContext, P, E, Unit, Any]
  ): Endpoint[CallerContext, TracedInput[P], E, Unit, Any] =
    endpoint.in(headers).mapIn(traceHeadersMapping[P]())

  implicit class FutureOps[R](future: Future[R]) {
    implicit val executionContext: ExecutionContext = ExecutionContext.parasitic
    implicit val traceContext: TraceContext = TraceContext.empty
    def resultToRight: Future[Either[JsCantonError, R]] =
      future
        .map(Right(_))
        .recover(handleError)
  }

  /** Utility to prepare flow from a gRPC method with an observer.
    * @param closeDelay  if true then server will close websocket after a delay when no new elements appear in stream
    */
  protected def prepareSingleWsStream[REQ, RESP, JSRESP](
      stream: (REQ, StreamObserver[RESP]) => Unit,
      mapToJs: RESP => Future[JSRESP],
      withCloseDelay: Boolean = false,
  )(implicit
      esf: ExecutionSequencerFactory,
      wsConfig: WebsocketConfig,
  ): Flow[REQ, JSRESP, NotUsed] = {
    val flow =
      Flow[REQ]
        .take(1) // we take only single request elem
        .flatMapConcat { req =>
          ClientAdapter
            .serverStreaming(req, stream)
        }

    if (withCloseDelay) {
      flow
        .map(Some(_))
        .concat(
          Source
            .single(None)
            .delay(wsConfig.closeDelay)
        )
        .collect { case Some(elem) =>
          elem
        }
        .mapAsync(1)(mapToJs)
    } else {
      flow.mapAsync(1)(mapToJs)
    }
  }

  private def handleError[T]: PartialFunction[Throwable, Either[JsCantonError, T]] = {
    case sre: StatusRuntimeException =>
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
    case NonFatal(e) =>
      // TODO(i19103)  decide if tracecontext headers on websockets are handled
      implicit val tc = TraceContext.empty
      val internalError =
        LedgerApiErrors.InternalError.Generic(
          e.getMessage,
          Some(e.getCause),
        )
      Left(
        JsCantonError.fromErrorCode(internalError)
      )
  }
}

object Endpoints {
  final case class Jwt(token: String)

  // added to ease burden if we change what is included in SECURITY_INPUT
  final case class CallerContext(jwt: Option[Jwt]) {
    def token(): Option[String] = jwt.map(_.token)
  }

  final case class TracedInput[A](in: A, traceContext: TraceContext)

  val wsSubprotocol: Header =
    sttp.model.Header("Sec-WebSocket-Protocol", "daml.ws.auth")

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
              val tokenPrefix = "jwt.token." // TODO (i21030) test this
              bearer
                .map(_.split(",").toSeq)
                .getOrElse(Seq.empty)
                .filter(_.startsWith(tokenPrefix))
                .map(_.substring(tokenPrefix.length))
                .headOption
                .map(Jwt.apply)
            }(_.map(_.token))
            .description("Ledger API standard JWT token (websocket)")
        )
        .map(tokens => CallerContext(tokens._1.orElse(tokens._2)))(cc => (cc.jwt, cc.jwt))
    )

  lazy val v2Endpoint: Endpoint[CallerContext, Unit, JsCantonError, Unit, Any] = baseEndpoint
    .errorOut(jsonBody[JsCantonError])
    .in("v2")

  def traceHeadersMapping[I](): Mapping[(I, List[Header]), TracedInput[I]] =
    new Mapping[(I, List[sttp.model.Header]), TracedInput[I]] {

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

  def error[R](error: JsCantonError): Future[Either[JsCantonError, R]] =
    Future.successful(Left(error))

  private def addStreamListParams[INPUT, OUTPUT, R](
      endpoint: Endpoint[CallerContext, INPUT, JsCantonError, Seq[
        OUTPUT
      ], R]
  ) = endpoint
    .in(
      query[Option[Long]]("limit").description(
        "maximum number of elements to return, this param is ignored if is bigger than server setting"
      )
    )
    .in(
      query[Option[Long]]("stream_idle_timeout_ms").description(
        "timeout to complete and send result if no new elements are received (for open ended streams)"
      )
    )
    .mapIn(new Mapping[(INPUT, Option[Long], Option[Long]), StreamList[INPUT]] {
      override def rawDecode(
          in: (INPUT, Option[Long], Option[Long])
      ): DecodeResult[StreamList[INPUT]] = DecodeResult.Value(
        StreamList[INPUT](in._1, in._2, in._3)
      )

      override def encode(h: StreamList[INPUT]): (INPUT, Option[Long], Option[Long]) =
        (h.input, h.limit, h.waitTime)

      override def validator: Validator[StreamList[INPUT]] = Validator.pass
    })

  implicit class StreamListOps[INPUT, OUTPUT, R](
      endpoint: Endpoint[CallerContext, INPUT, JsCantonError, Seq[
        OUTPUT
      ], R]
  ) {
    def inStreamListParams() = addStreamListParams(endpoint)
  }

}

trait DocumentationEndpoints {
  def documentation: Seq[AnyEndpoint]
}

final case class StreamList[INPUT](input: INPUT, limit: Option[Long], waitTime: Option[Long])
