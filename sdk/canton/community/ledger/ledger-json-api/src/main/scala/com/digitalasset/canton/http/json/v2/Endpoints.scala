// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.grpc.adapter.client.pekko.ClientAdapter
import com.digitalasset.base.error.utils.DecodedCantonError
import com.digitalasset.canton.http.WebsocketConfig
import com.digitalasset.canton.http.json.v2.JsSchema.JsCantonError
import com.digitalasset.canton.ledger.error.groups.CommandExecutionErrors
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors.InvalidArgument
import com.digitalasset.canton.ledger.error.{JsonApiErrors, LedgerApiErrors}
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.tracing.{TraceContext, W3CTraceContext}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.engine.Error.Preprocessing
import com.digitalasset.daml.lf.language.Ast.TVar
import com.digitalasset.daml.lf.value.Value.ValueUnit
import com.digitalasset.transcode.{MissingFieldException, UnexpectedFieldsException}
import io.circe.{Decoder, Encoder}
import io.grpc.stub.StreamObserver
import io.grpc.{Status, StatusRuntimeException}
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Flow, Sink, Source}
import sttp.capabilities.WebSockets
import sttp.capabilities.pekko.PekkoStreams
import sttp.model.{Header, StatusCode}
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
  type CustomError = (StatusCode, JsCantonError)

  import Endpoints.*
  import com.digitalasset.canton.http.util.GrpcHttpErrorCodes.`gRPC status  as sttp`

  protected def handleFailure[R](implicit
      traceContext: TraceContext
  ): Try[Either[CustomError, R]] => Try[Either[CustomError, R]] =
    _.recoverWith { case error =>
      handleErrorResponse(traceContext)(Failure(error))
    }
  protected def handleErrorResponse[R](implicit
      traceContext: TraceContext
  ): Try[Either[JsCantonError, R]] => Try[Either[CustomError, R]] = {
    case Success(Right(value)) => Success(Right(value))
    case Success(Left(error)) =>
      Success(
        Left(
          (
            error.grpcCodeValue
              .map(Status.fromCodeValue(_))
              .map(_.getCode)
              .getOrElse(Status.Code.UNKNOWN)
              .asSttpStatus,
            error,
          )
        )
      )
    case Failure(t: Throwable) if handleError.isDefinedAt(t) =>
      Success(handleError(traceContext)(t))
    case Failure(unhandled) =>
      unhandled match {
        case _ =>
          val internalError =
            LedgerApiErrors.InternalError.Generic(unhandled.getMessage, Some(unhandled))

          Success(
            Left(
              (StatusCode.InternalServerError, JsCantonError.fromErrorCode(internalError))
            )
          )
      }
  }

  def json[R: Decoder: Encoder: Schema, P](
      endpoint: Endpoint[CallerContext, P, CustomError, Unit, Any],
      service: CallerContext => TracedInput[P] => Future[Either[JsCantonError, R]],
  ): Full[CallerContext, CallerContext, TracedInput[P], CustomError, R, Any, Future] =
    endpoint
      .in(headers)
      .mapIn(traceHeadersMapping[P]())
      .serverSecurityLogicSuccess(Future.successful)
      .out(jsonBody[R])
      .serverLogic(callerContext =>
        i =>
          Future
            .delegate(service(callerContext)(i))(ExecutionContext.parasitic)
            .transform(handleErrorResponse(i.traceContext))(ExecutionContext.parasitic)
      )

  protected def websocket[HI, I, O](
      endpoint: Endpoint[
        CallerContext,
        HI,
        CustomError,
        Flow[I, Either[JsCantonError, O], Any],
        PekkoStreams & WebSockets,
      ],
      service: CallerContext => TracedInput[HI] => Flow[I, O, Any],
  ): Full[CallerContext, CallerContext, HI, CustomError, Flow[
    I,
    Either[JsCantonError, O],
    Any,
  ], PekkoStreams & WebSockets, Future] =
    endpoint
      // .in(header(wsSubprotocol))  We send wsSubprotocol header, but we do not enforce it
      .out(header(wsSubprotocol))
      .serverSecurityLogicSuccess(Future.successful)
      .serverLogicSuccess { jwt => i =>
        val errorHandlingService =
          service(jwt)(
            TracedInput(i, TraceContext.empty)
          ) // We do not pass traceheaders on Websockets
            .map(out => Right[JsCantonError, O](out))
            .recover(handleErrorInSocket(TraceContext.empty))
        // According to tapir documentation pekko-http does not expose control frames (Ping, Pong and Close)
        //  We cannot send error as close frame
        Future.successful(errorHandlingService)
      }

  def error[R](error: JsCantonError): Future[Either[JsCantonError, R]] =
    Future.successful(Left(error))

  private def maxRowsToReturn(requestLimit: Option[Long])(implicit wsConfig: WebsocketConfig) =
    Math.min(
      requestLimit.getOrElse(wsConfig.httpListMaxElementsLimit + 1),
      wsConfig.httpListMaxElementsLimit + 1,
    )

  def asList[INPUT, OUTPUT, R](
      endpoint: Endpoint[CallerContext, StreamList[INPUT], CustomError, Seq[
        OUTPUT
      ], R],
      service: CallerContext => TracedInput[Unit] => Flow[INPUT, OUTPUT, Any],
      timeoutOpenEndedStream: INPUT => Boolean = (_: INPUT) => false,
  )(implicit
      wsConfig: WebsocketConfig,
      materializer: Materializer,
  ) =
    endpoint
      .in(headers)
      .mapIn(traceHeadersMapping[StreamList[INPUT]]())
      .serverSecurityLogicSuccess(Future.successful)
      .serverLogic(caller =>
        (tracedInput: TracedInput[StreamList[INPUT]]) => {
          implicit val tc = tracedInput.traceContext
          val flow = service(caller)(tracedInput.copy(in = ()))
          val limit = tracedInput.in.limit
          val elementsLimit = maxRowsToReturn(limit)
          val systemListElementsLimit = wsConfig.httpListMaxElementsLimit
          val idleWaitTime = tracedInput.in.waitTime
            .map(FiniteDuration.apply(_, TimeUnit.MILLISECONDS))
            .getOrElse(wsConfig.httpListWaitTime)
          val source = Source
            .single(tracedInput.in.input)
            .via(flow)
            .take(elementsLimit)
          (if (timeoutOpenEndedStream(tracedInput.in.input) || tracedInput.in.waitTime.isDefined) {
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
           })
            .runWith(Sink.seq)
            .map(
              handleListLimit(systemListElementsLimit, _)
            )(ExecutionContext.parasitic)
            .transform(handleFailure(tracedInput.traceContext))(ExecutionContext.parasitic)
        }
      )

  private def handleListLimit[R, OUTPUT, INPUT](
      systemListElementsLimit: Long,
      elements: Seq[OUTPUT],
  )(implicit traceContext: TraceContext) = {
    def belowSystemLimit = elements.size <= systemListElementsLimit

    if (belowSystemLimit) {
      Right(elements)
    } else {
      Left(
        (
          StatusCode.PayloadTooLarge,
          JsCantonError.fromErrorCode(
            JsonApiErrors.MaximumNumberOfElements
              .Reject(elements.size, systemListElementsLimit)
          ),
        )
      )
    }
  }

  def asPagedList[INPUT, OUTPUT, R](
      endpoint: Endpoint[CallerContext, PagedList[INPUT], (StatusCode, JsCantonError), OUTPUT, R],
      service: CallerContext => TracedInput[PagedList[INPUT]] => Future[
        Either[JsCantonError, OUTPUT]
      ],
  ): Full[CallerContext, CallerContext, TracedInput[
    PagedList[INPUT]
  ], (StatusCode, JsCantonError), OUTPUT, R, Future] =
    endpoint
      .in(headers)
      .mapIn(traceHeadersMapping[PagedList[INPUT]]())
      .serverSecurityLogicSuccess(Future.successful)
      .serverLogic(caller =>
        tracedInput => {
          Future
            .delegate(service(caller)(tracedInput))(ExecutionContext.parasitic)
            .transform(handleErrorResponse(tracedInput.traceContext))(ExecutionContext.parasitic)
        }
      )

  def withServerLogic[INPUT, OUTPUT, R](
      endpoint: Endpoint[CallerContext, INPUT, CustomError, OUTPUT, R],
      service: CallerContext => TracedInput[INPUT] => Future[Either[JsCantonError, OUTPUT]],
  ): Full[CallerContext, CallerContext, TracedInput[INPUT], CustomError, OUTPUT, R, Future] =
    endpoint
      .in(headers)
      .mapIn(traceHeadersMapping[INPUT]())
      .serverSecurityLogicSuccess(Future.successful)
      .serverLogic(caller =>
        tracedInput => {
          Future
            .delegate(service(caller)(tracedInput))(ExecutionContext.parasitic)
            .transform(handleErrorResponse(tracedInput.traceContext))(ExecutionContext.parasitic)
        }
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
        .recover(handleError.andThen(_.left.map(_._2)))

    def resultWithStatusToRight: Future[Either[CustomError, R]] =
      future
        .map(Right(_))
        .recover(handleError)
  }

  /** Utility to prepare flow from a gRPC method with an observer.
    * @param closeDelay
    *   if true then server will close websocket after a delay when no new elements appear in stream
    */
  protected def prepareSingleWsStream[REQ, RESP, JSRESP](
      stream: (REQ, StreamObserver[RESP]) => Unit,
      mapToJs: RESP => Future[JSRESP],
  )(implicit
      esf: ExecutionSequencerFactory
  ): Flow[REQ, JSRESP, NotUsed] =
    Flow[REQ]
      .take(1) // we take only single request elem
      .flatMapConcat { req =>
        ClientAdapter
          .serverStreaming(req, stream)
      }
      .mapAsync(1)(mapToJs)

  private def handleErrorInSocket[T](implicit
      traceContext: TraceContext
  ): PartialFunction[Throwable, Either[JsCantonError, T]] = handleError.andThen(_.left.map(_._2))

  private def handleError[T](implicit
      traceContext: TraceContext
  ): PartialFunction[Throwable, Either[CustomError, T]] = {
    case sre: StatusRuntimeException =>
      Left(
        (
          sre.getStatus.getCode.asSttpStatus,
          DecodedCantonError
            .fromStatusRuntimeException(sre)
            .map(JsCantonError.fromDecodedCantonError)
            .getOrElse {
              // TODO (#27556) we should log these errors / locations and clean all of them up
              //   CantonErrors are logged on creation (normally ...).
              logger.info(
                s"Request failed with legacy error ${sre.getStatus} / ${sre.getMessage}",
                sre.getCause,
              )
              JsCantonError(
                code = sre.getStatus.getDescription,
                cause = sre.getMessage,
                correlationId = None,
                traceId = None,
                context = Map(),
                resources = Seq(),
                errorCategory = -1,
                grpcCodeValue = Some(sre.getStatus.getCode.value()),
                retryInfo = None,
                definiteAnswer = None,
              )
            },
        )
      )
    case unexpected: UnexpectedFieldsException =>
      Left(
        (
          StatusCode.BadRequest,
          JsCantonError.fromErrorCode(
            InvalidArgument.Reject(
              s"Unexpected fields: ${unexpected.unexpectedFields.mkString}"
            )
          ),
        )
      )
    case fieldMissing: MissingFieldException =>
      Left(
        (
          StatusCode.BadRequest,
          JsCantonError.fromErrorCode(
            CommandExecutionErrors.Preprocessing.PreprocessingFailed.Reject(
              Preprocessing.TypeMismatch(
                TVar(Ref.Name.assertFromString("unknown")),
                ValueUnit,
                s"Missing non-optional field: ${fieldMissing.missingField}",
              )
            )
          ),
        )
      )
    case illegalArgument: IllegalArgumentException =>
      Left(
        (
          StatusCode.BadRequest,
          JsCantonError.fromErrorCode(
            InvalidArgument.Reject(illegalArgument.getMessage)
          ),
        )
      )
    case NonFatal(error) =>
      val internalError =
        LedgerApiErrors.InternalError.Generic(
          error.getMessage,
          Some(error),
        )
      Left(
        (StatusCode.InternalServerError, JsCantonError.fromErrorCode(internalError))
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

  lazy val v2Endpoint: Endpoint[CallerContext, Unit, (StatusCode, JsCantonError), Unit, Any] =
    baseEndpoint
      .errorOut(statusCode.and(jsonBody[JsCantonError]))
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

  private def addStreamListParamsAndDescription[INPUT, OUTPUT, R](
      endpoint: Endpoint[CallerContext, INPUT, (StatusCode, JsCantonError), Seq[
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
    .description(
      endpoint.info.description.getOrElse("") +
        """
      |Notice: This endpoint should be used for small results set.
      |When number of results exceeded node configuration limit (`http-list-max-elements-limit`)
      |there will be an error (`413 Content Too Large`) returned.
      |Increasing this limit may lead to performance issues and high memory consumption.
      |Consider using websockets (asyncapi) for better efficiency with larger results.""".stripMargin
    )

  private def addPagedListParams[INPUT, OUTPUT, R](
      endpoint: Endpoint[CallerContext, INPUT, (StatusCode, JsCantonError), OUTPUT, R]
  ) = endpoint
    .in(
      query[Option[Int]]("pageSize").description(
        "maximum number of elements in a returned page"
      )
    )
    .in(
      query[Option[String]]("pageToken").description(
        "token - to continue results from a given page, leave empty to start from the beginning of the list, obtain token from the result of previous page"
      )
    )
    .mapIn(new Mapping[(INPUT, Option[Int], Option[String]), PagedList[INPUT]] {
      override def rawDecode(
          in: (INPUT, Option[Int], Option[String])
      ): DecodeResult[PagedList[INPUT]] = DecodeResult.Value(
        PagedList[INPUT](in._1, in._2, in._3)
      )

      override def encode(h: PagedList[INPUT]): (INPUT, Option[Int], Option[String]) =
        (h.input, h.pageSize, h.pageToken)

      override def validator: Validator[PagedList[INPUT]] = Validator.pass
    })

  implicit class StreamListOps[INPUT, OUTPUT, R](
      endpoint: Endpoint[CallerContext, INPUT, (StatusCode, JsCantonError), Seq[
        OUTPUT
      ], R]
  ) {
    def inStreamListParamsAndDescription() = addStreamListParamsAndDescription(endpoint)

  }

  implicit class PagedListOps[INPUT, OUTPUT, R](
      endpoint: Endpoint[CallerContext, INPUT, (StatusCode, JsCantonError), OUTPUT, R]
  ) {

    def inPagedListParams() = addPagedListParams(endpoint)
  }

}

trait DocumentationEndpoints {
  def documentation: Seq[AnyEndpoint]
}

final case class StreamList[INPUT](input: INPUT, limit: Option[Long], waitTime: Option[Long])

final case class PagedList[INPUT](input: INPUT, pageSize: Option[Int], pageToken: Option[String])
