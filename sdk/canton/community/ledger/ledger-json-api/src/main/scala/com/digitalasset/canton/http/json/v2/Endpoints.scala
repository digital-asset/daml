// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.grpc.adapter.client.pekko.ClientAdapter
import com.digitalasset.base.error.utils.DecodedCantonError
import com.digitalasset.canton.auth.{AuthInterceptor, ClaimSet}
import com.digitalasset.canton.http.WebsocketConfig
import com.digitalasset.canton.http.json.v2.JsSchema.JsCantonError
import com.digitalasset.canton.ledger.error.groups.CommandExecutionErrors
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors.InvalidArgument
import com.digitalasset.canton.ledger.error.{JsonApiErrors, LedgerApiErrors}
import com.digitalasset.canton.logging.audit.ApiRequestLogger
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLogging}
import com.digitalasset.canton.networking.grpc.CallMetadata
import com.digitalasset.canton.tracing.{TraceContext, W3CTraceContext}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.engine.Error.Preprocessing
import com.digitalasset.daml.lf.language.Ast.TVar
import com.digitalasset.daml.lf.value.Value.ValueUnit
import com.digitalasset.transcode.{
  IncorrectVariantRepresentationException,
  MissingFieldsException,
  UnexpectedFieldsException,
}
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

  protected def requestLogger: ApiRequestLogger

  protected implicit def executionContext: ExecutionContext

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
  )(implicit
      authInterceptor: AuthInterceptor
  ): Full[CallerContext, CallerContext, TracedInput[P], CustomError, R, Any, Future] =
    endpoint
      .mapIn(traceHeadersMapping[P]())
      .serverSecurityLogic(validateJwtToken(endpoint))
      .out(jsonBody[R])
      .serverLogic(callerContext =>
        i =>
          Future
            .delegate(service(callerContext)(i))
            .transform(handleErrorResponse(callerContext.traceContext()))
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
  )(implicit
      authInterceptor: AuthInterceptor
  ): Full[CallerContext, CallerContext, HI, CustomError, Flow[
    I,
    Either[JsCantonError, O],
    Any,
  ], PekkoStreams & WebSockets, Future] =
    endpoint
      // .in(header(wsSubprotocol))  We send wsSubprotocol header, but we do not enforce it
      .out(header(wsSubprotocol))
      .serverSecurityLogic(validateJwtToken(endpoint))
      .serverLogicSuccess { jwt => i =>
        val errorHandlingService =
          service(jwt)(
            TracedInput(i)
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
      authInterceptor: AuthInterceptor,
  ) =
    endpoint
      .mapIn(traceHeadersMapping[StreamList[INPUT]]())
      .serverSecurityLogic(validateJwtToken(endpoint))
      .serverLogic(caller =>
        (tracedInput: TracedInput[StreamList[INPUT]]) => {
          implicit val tc = caller.traceContext()
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
            )
            .transform(handleFailure)
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
  )(implicit
      authInterceptor: AuthInterceptor
  ): Full[CallerContext, CallerContext, TracedInput[
    PagedList[INPUT]
  ], (StatusCode, JsCantonError), OUTPUT, R, Future] =
    endpoint
      .mapIn(traceHeadersMapping[PagedList[INPUT]]())
      .serverSecurityLogic(validateJwtToken(endpoint))
      .serverLogic(caller =>
        tracedInput => {
          Future
            .delegate(service(caller)(tracedInput))
            .transform(handleErrorResponse(caller.traceContext()))
        }
      )

  def withServerLogic[INPUT, OUTPUT, R](
      endpoint: Endpoint[CallerContext, INPUT, CustomError, OUTPUT, R],
      service: CallerContext => TracedInput[INPUT] => Future[Either[JsCantonError, OUTPUT]],
  )(implicit
      authInterceptor: AuthInterceptor
  ): Full[CallerContext, CallerContext, TracedInput[INPUT], CustomError, OUTPUT, R, Future] =
    endpoint
      .mapIn(traceHeadersMapping[INPUT]())
      .serverSecurityLogic(validateJwtToken(endpoint))
      .serverLogic(caller =>
        tracedInput => {
          Future
            .delegate(service(caller)(tracedInput))
            .transform(handleErrorResponse(caller.traceContext()))
        }
      )

  private def validateJwtToken(endpointInfo: EndpointMetaOps)(caller: CallerContext)(implicit
      authInterceptor: AuthInterceptor
  ): Future[Either[CustomError, CallerContext]] = {
    implicit val traceContext = caller.traceContext()
    implicit val lc = LoggingContextWithTrace(loggerFactory)
    authInterceptor
      .extractClaims(
        caller.token().map(token => s"Bearer $token"),
        endpointInfo.showShort,
      )
      .map { claims =>
        val authenticatedCaller = caller.copy(claimSet = Some(claims))
        requestLogger.logAuth(caller.call, claims)
        Right(authenticatedCaller)
      }
      .recoverWith { error =>
        requestLogger.logAuthError(caller.call, error)
        Future.successful(handleError(lc.traceContext)(error).left.map {
          case (statusCode, jsCantonError) =>
            (
              statusCode,
              // we add info to context about json ledger api origin of the error
              jsCantonError.copy(context = jsCantonError.context + JsCantonError.tokenProblemError),
            )
        })
      }
  }

  protected def withTraceHeaders[P, E](
      endpoint: Endpoint[CallerContext, P, E, Unit, Any]
  ): Endpoint[CallerContext, TracedInput[P], E, Unit, Any] =
    endpoint.mapIn(traceHeadersMapping[P]())

  implicit class FutureOps[R](future: Future[R]) {
    // TODO(#27556): Pass TraceContext from caller
    implicit val traceContext: TraceContext = TraceContext.empty
    def resultToRight: Future[Either[JsCantonError, R]] =
      future
        .map(Right(_))
        .recover(handleError.andThen(_.left.map(_._2)))
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
                code = Option(sre.getStatus.getDescription)
                  .getOrElse("Status description not available"),
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
    case fieldMissing: MissingFieldsException =>
      Left(
        (
          StatusCode.BadRequest,
          JsCantonError.fromErrorCode(
            CommandExecutionErrors.Preprocessing.PreprocessingFailed.Reject(
              Preprocessing.TypeMismatch(
                TVar(Ref.Name.assertFromString("unknown")),
                ValueUnit,
                s"Missing non-optional fields: ${fieldMissing.missingFields}",
              )
            )
          ),
        )
      )
    case incorrectJson: IncorrectVariantRepresentationException =>
      Left(
        (
          StatusCode.BadRequest,
          JsCantonError.fromErrorCode(
            InvalidArgument.Reject(incorrectJson.getMessage)
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
  final case class CallerContext(
      jwt: Option[Jwt],
      call: CallMetadata,
      claimSet: Option[ClaimSet] = None,
      optTraceContext: Option[TraceContext] = None,
  ) {
    def token(): Option[String] = jwt.map(_.token)

    def traceContext(): TraceContext = optTraceContext.getOrElse(TraceContext.empty)
  }

  // TODO (i28204) remove this class - it was once a wrapper over Input and TraceContext - but TraceContext is now part of  Caller
  final case class TracedInput[A](in: A)

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
        .and(
          extractFromRequest(req =>
            (
              req.headers,
              RequestInterceptorsUtil.extractCallMetadata(req),
            )
          )
            .map { case (headersList: Seq[Header], addr) =>
              val z = W3CTraceContext.fromHeaders(headersList.map(h => (h.name, h.value)).toMap)
              (z.map(_.toTraceContext), addr)
            } { case (tc1, addr) =>
              (
                tc1
                  .map { case tc =>
                    (W3CTraceContext
                      .extractHeaders(tc)
                      .map { case (k, v) =>
                        Header(k, v)
                      }
                      .toList)

                  }
                  .getOrElse(Nil),
                addr,
              )
            }
        )
        .map { case (httpToken, wsToken, traceContext, call) =>
          CallerContext(httpToken.orElse(wsToken), call, None, traceContext)
        }(cc => (cc.jwt, cc.jwt, cc.optTraceContext, cc.call))
    )

  lazy val v2Endpoint: Endpoint[CallerContext, Unit, (StatusCode, JsCantonError), Unit, Any] =
    baseEndpoint
      .errorOut(statusCode.and(jsonBody[JsCantonError]))
      .in("v2")

  def traceHeadersMapping[I](): Mapping[I, TracedInput[I]] =
    new Mapping[I, TracedInput[I]] {

      override def rawDecode(input: I): DecodeResult[TracedInput[I]] =
        DecodeResult.Value(
          TracedInput(
            input
          )
        )

      override def encode(h: TracedInput[I]): I =
        h.in

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
      endpoint.info.description match {
        case Some(desc) =>
          s"$desc\n" + """
                              |Notice: This endpoint should be used for small results set.
                              |When number of results exceeded node configuration limit (`http-list-max-elements-limit`)
                              |there will be an error (`413 Content Too Large`) returned.
                              |Increasing this limit may lead to performance issues and high memory consumption.
                              |Consider using websockets (asyncapi) for better efficiency with larger results.
                              |""".stripMargin.trim
        case None =>
          throw new IllegalArgumentException(s"Description for ${endpoint.info} is missing")
      }
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

  def createProtoRef(methodDescriptor: io.grpc.MethodDescriptor[?, ?]): String =
    ProtoLink(methodDescriptor).toString

  implicit class ProtoRefOps[INPUT, OUTPUT, R](
      endpoint: Endpoint[CallerContext, INPUT, (StatusCode, JsCantonError), OUTPUT, R]
  ) {

    def protoRef(
        methodDescriptor: io.grpc.MethodDescriptor[?, ?]
    ): Endpoint[CallerContext, INPUT, (StatusCode, JsCantonError), OUTPUT, R] =
      endpoint.description(createProtoRef(methodDescriptor))
  }

}

trait DocumentationEndpoints {
  def documentation: Seq[AnyEndpoint]
}

final case class StreamList[INPUT](input: INPUT, limit: Option[Long], waitTime: Option[Long])

final case class PagedList[INPUT](input: INPUT, pageSize: Option[Int], pageToken: Option[String])

final case class ProtoLink(file: String, service: String, method: String) {

  override def toString: String = s"<gRPC:$file/$service/$method>"
}

object ProtoLink {
  private val grpcMethodExtractor =
    raw"com\.daml\.ledger\.api\.v2\.((?:(?:[a-z0-9])*\.)*)([A-Za-z0-9]+)".r

  private val importGRPCCommentPattern =
    raw"<gRPC:([A-Za-z0-9_/]+\.proto)/([A-Za-z0-9_]+)/([A-Za-z0-9_]+)>".r

  def apply(methodDescriptor: io.grpc.MethodDescriptor[?, ?]): ProtoLink = {
    val serviceName: String = methodDescriptor.getServiceName
    val bareMethodName = methodDescriptor.getBareMethodName
    serviceName match {
      case grpcMethodExtractor(packageName, service) =>
        val snake = service.replaceAll("([a-z])([A-Z])", "$1_$2").toLowerCase
        val pck1 = packageName.replace('.', '/')
        ProtoLink(pck1 + snake + ".proto", service, bareMethodName)
      case _ =>
        throw new IllegalArgumentException(
          s"Could not create link to proto documentation for: $methodDescriptor"
        )
    }
  }
  def unapply(link: String): Option[(String, String, String)] = link match {
    case importGRPCCommentPattern(file, service, method) =>
      Some((file, service, method))
    case _ => None
  }
}
