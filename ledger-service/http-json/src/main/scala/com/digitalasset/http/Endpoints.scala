// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import akka.NotUsed
import akka.http.scaladsl.model._
import headers.`Content-Type`
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives.extractClientIP
import akka.http.scaladsl.server.{Directive, Directive0, PathMatcher, Route}
import akka.http.scaladsl.server.RouteResult._
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Source}
import akka.util.ByteString
import ContractsService.SearchResult
import EndpointsCompanion._
import json._
import util.toLedgerId
import util.FutureUtil.{either, rightT}
import util.Logging.{InstanceUUID, RequestID, extendWithRequestIdLogCtx}
import com.daml.logging.LoggingContextOf.withEnrichedLoggingContext
import scalaz.std.scalaFuture._
import scalaz.syntax.std.option._
import scalaz.syntax.traverse._
import scalaz.{-\/, EitherT, \/, \/-}
import spray.json._

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import com.daml.logging.{ContextualizedLogger, LoggingContextOf}
import com.daml.metrics.{Metrics, Timed}
import akka.http.scaladsl.server.Directives._
import com.daml.http.endpoints.{MeteringReportEndpoint, RouteSetup}
import com.daml.jwt.domain.Jwt
import com.daml.ledger.api.{domain => LedgerApiDomain}
import com.daml.ledger.client.services.admin.UserManagementClient
import com.daml.ledger.client.services.identity.LedgerIdentityClient
import com.daml.metrics.api.MetricHandle.Timer
import scalaz.EitherT.eitherT

import scala.util.control.NonFatal

class Endpoints(
    allowNonHttps: Boolean,
    decodeJwt: EndpointsCompanion.ValidateJwt,
    commandService: CommandService,
    contractsService: ContractsService,
    partiesService: PartiesService,
    packageManagementService: PackageManagementService,
    meteringReportService: MeteringReportService,
    healthService: HealthService,
    encoder: DomainJsonEncoder,
    decoder: DomainJsonDecoder,
    shouldLogHttpBodies: Boolean,
    userManagementClient: UserManagementClient,
    ledgerIdentityClient: LedgerIdentityClient,
    maxTimeToCollectRequest: FiniteDuration = FiniteDuration(5, "seconds"),
)(implicit ec: ExecutionContext, mat: Materializer) {

  private[this] val routeSetup: endpoints.RouteSetup = new endpoints.RouteSetup(
    allowNonHttps = allowNonHttps,
    decodeJwt = decodeJwt,
    encoder = encoder,
    userManagementClient,
    ledgerIdentityClient,
    maxTimeToCollectRequest = maxTimeToCollectRequest,
  )

  private[this] val commandsHelper: endpoints.CreateAndExercise =
    new endpoints.CreateAndExercise(routeSetup, decoder, commandService, contractsService)
  import commandsHelper._

  private[this] val userManagement: endpoints.UserManagement = new endpoints.UserManagement(
    decodeJwt = decodeJwt,
    userManagementClient,
  )
  import userManagement._

  private[this] val packagesDars: endpoints.PackagesAndDars =
    new endpoints.PackagesAndDars(packageManagementService)
  import packagesDars._

  private[this] val meteringReportEndpoint =
    new MeteringReportEndpoint(meteringReportService)

  private[this] val contractList: endpoints.ContractList =
    new endpoints.ContractList(routeSetup, decoder, contractsService)
  import contractList._

  private[this] val partiesEP: endpoints.Parties = new endpoints.Parties(partiesService)
  import partiesEP._

  private[this] val logger = ContextualizedLogger.get(getClass)

  // Limit logging of bodies to content with size of less than 10 KiB.
  // Reason is that a char of an UTF-8 string consumes 1 up to 4 bytes such that the string length
  // with this limit will be 2560 chars up to 10240 chars. This can hold already the whole cascade
  // of import statements in this file, which I would consider already as very big string to log.
  private final val maxBodySizeForLogging = Math.pow(2, 10) * 10

  import Endpoints._
  import json.JsonProtocol._
  import util.ErrorOps._

  private def responseToRoute(res: Future[HttpResponse]): Route = _ => res map Complete
  private def toRoute[T: MkHttpResponse](res: => T)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): Route =
    responseToRoute(httpResponse(res))

  private def toPostRoute[Req: JsonReader, Res: JsonWriter](
      httpRequest: HttpRequest,
      fn: (Jwt, Req) => ET[domain.SyncResponse[Res]],
  )(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      metrics: Metrics,
  ): Route = {
    val res = for {
      t <- routeSetup.inputJsVal(httpRequest): ET[(Jwt, JsValue)]
      (jwt, reqBody) = t
      req <- either(SprayJson.decode[Req](reqBody).liftErr(InvalidUserInput)): ET[Req]
      res <- eitherT(RouteSetup.handleFutureEitherFailure(fn(jwt, req).run)): ET[
        domain.SyncResponse[Res]
      ]
    } yield res
    responseToRoute(httpResponse(res))
  }

  private def toGetRoute[Res](
      httpRequest: HttpRequest,
      fn: (Jwt) => ET[domain.SyncResponse[Res]],
  )(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      mkHttpResponse: MkHttpResponse[ET[domain.SyncResponse[Res]]],
  ): Route = {
    val res = for {
      t <- eitherT(routeSetup.input(httpRequest)): ET[(Jwt, String)]
      (jwt, _) = t
      res <- eitherT(RouteSetup.handleFutureEitherFailure(fn(jwt).run)): ET[
        domain.SyncResponse[Res]
      ]
    } yield res
    responseToRoute(httpResponse(res))
  }

  private def toGetRouteLedgerId[Res](
      httpRequest: HttpRequest,
      fn: (Jwt, LedgerApiDomain.LedgerId) => ET[domain.SyncResponse[Res]],
  )(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      mkHttpResponse: MkHttpResponse[ET[domain.SyncResponse[Res]]],
  ): Route = {
    val res = for {
      t <- extractJwtAndLedgerId(httpRequest)
      (jwt, ledgerId) = t
      res <- eitherT(
        RouteSetup.handleFutureEitherFailure(fn(jwt, ledgerId).run)
      ): ET[domain.SyncResponse[Res]]
    } yield res
    responseToRoute(httpResponse(res))
  }

  private def toUploadDarFileRoute(httpRequest: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      metrics: Metrics,
      mkHttpResponse: MkHttpResponse[ET[domain.SyncResponse[Unit]]],
  ): Route = {
    val res: ET[domain.SyncResponse[Unit]] = for {
      parseAndDecodeTimer <- routeSetup.getParseAndDecodeTimerCtx()
      _ <- EitherT.pure(metrics.daml.HttpJsonApi.uploadPackagesThroughput.mark())
      t2 <- eitherT(routeSetup.inputSource(httpRequest))
      (jwt, payload, source) = t2
      _ <- EitherT.pure(parseAndDecodeTimer.stop())

      _ <- eitherT(
        RouteSetup.handleFutureFailure(
          packageManagementService.uploadDarFile(
            jwt,
            toLedgerId(payload.ledgerId),
            source.mapMaterializedValue(_ => NotUsed),
          )
        )
      ): ET[Unit]
    } yield domain.OkResponse(())
    responseToRoute(httpResponse(res))
  }

  private def toDownloadPackageRoute[Res](
      httpRequest: HttpRequest,
      packageId: String,
      fn: (Jwt, LedgerApiDomain.LedgerId, String) => Future[HttpResponse],
  )(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): Route = {
    responseToRoute(
      httpResponse(
        extractJwtAndLedgerId(httpRequest).flatMap { case (jwt, ledgerId) =>
          rightT(fn(jwt, ledgerId, packageId))
        }
      )
    )
  }

  private def extractJwtAndLedgerId(
      httpRequest: HttpRequest
  )(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): ET[(Jwt, LedgerApiDomain.LedgerId)] = for {
    t <- routeSetup
      .inputAndJwtPayload[domain.JwtPayloadLedgerIdOnly](httpRequest): ET[
      (Jwt, domain.JwtPayloadLedgerIdOnly, String)
    ]
    (jwt, jwtBody, _) = t
  } yield (jwt, toLedgerId(jwtBody.ledgerId))

  private def mkRequestLogMsg(request: HttpRequest, remoteAddress: RemoteAddress) =
    s"Incoming ${request.method.value} request on ${request.uri} from $remoteAddress"

  private def mkResponseLogMsg(statusCode: StatusCode) =
    s"Responding to client with HTTP $statusCode"

  // Always put this directive after a path to ensure
  // that you don't log request bodies multiple times (simply because a matching test was made multiple times).
  // TL;DR JUST PUT THIS THING AFTER YOUR FINAL PATH MATCHING
  private def logRequestResponseHelper(
      logIncomingRequest: (HttpRequest, RemoteAddress) => HttpRequest,
      logResponse: HttpResponse => HttpResponse,
  ): Directive0 =
    extractClientIP flatMap { remoteAddress =>
      mapRequest(request => logIncomingRequest(request, remoteAddress)) & mapRouteResultFuture {
        responseF =>
          for {
            response <- responseF
            transformedResponse <- response match {
              case Complete(httpResponse) =>
                Future.successful(Complete(logResponse(httpResponse)))
              case _ =>
                Future.failed(
                  new RuntimeException(
                    """Logging the request & response should never happen on routes which get rejected.
                    |Make sure to place the directive only at places where a match is guaranteed (e.g. after the path directive).""".stripMargin
                  )
                )
            }
          } yield transformedResponse
      }
    }

  private def logJsonRequestAndResult(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): Directive0 = {
    def logWithHttpMessageBodyIfAvailable(
        httpMessage: HttpMessage,
        msg: String,
        kind: String,
    ): httpMessage.Self =
      if (
        httpMessage
          .header[`Content-Type`]
          .map(_.contentType)
          .contains(ContentTypes.`application/json`)
      ) {
        def logWithBodyInCtx(body: com.daml.logging.entries.LoggingValue) =
          withEnrichedLoggingContext(
            LoggingContextOf.label[RequestEntity],
            s"${kind}_body" -> body,
          )
            .run { implicit lc => logger.info(msg) }
        httpMessage.entity.contentLengthOption match {
          case Some(length) if length < maxBodySizeForLogging =>
            import akka.stream.scaladsl._
            httpMessage
              .transformEntityDataBytes(
                Flow.fromFunction { it =>
                  try logWithBodyInCtx(it.utf8String.parseJson)
                  catch {
                    case NonFatal(ex) =>
                      logger.error("Failed to log message body: ", ex)
                  }
                  it
                }
              )
          case other =>
            val reason = other
              .map(length => s"size of $length is too big for logging")
              .getOrElse {
                if (httpMessage.entity.isChunked())
                  "is chunked & overall size is unknown"
                else
                  "size is unknown"
              }
            logWithBodyInCtx(s"omitted because $kind body $reason")
            httpMessage.self
        }
      } else {
        logger.info(msg)
        httpMessage.self
      }

    logRequestResponseHelper(
      (request, remoteAddress) =>
        logWithHttpMessageBodyIfAvailable(
          request,
          mkRequestLogMsg(request, remoteAddress),
          "request",
        ),
      httpResponse =>
        logWithHttpMessageBodyIfAvailable(
          httpResponse,
          mkResponseLogMsg(httpResponse.status),
          "response",
        ),
    )
  }

  def logRequestAndResultSimple(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): Directive0 =
    logRequestResponseHelper(
      (request, remoteAddress) => {
        logger.info(mkRequestLogMsg(request, remoteAddress))
        request
      },
      httpResponse => {
        logger.info(mkResponseLogMsg(httpResponse.status))
        httpResponse
      },
    )

  val logRequestAndResultFn: LoggingContextOf[InstanceUUID with RequestID] => Directive0 =
    if (shouldLogHttpBodies) lc => logJsonRequestAndResult(lc)
    else lc => logRequestAndResultSimple(lc)

  def logRequestAndResult(implicit lc: LoggingContextOf[InstanceUUID with RequestID]): Directive0 =
    logRequestAndResultFn(lc)

  def all(implicit
      lc0: LoggingContextOf[InstanceUUID],
      metrics: Metrics,
  ): Route = extractRequest apply { req =>
    implicit val lc: LoggingContextOf[InstanceUUID with RequestID] =
      extendWithRequestIdLogCtx(identity)(lc0)
    import metrics.daml.HttpJsonApi._
    val markThroughputAndLogProcessingTime: Directive0 = Directive { (fn: Unit => Route) =>
      val t0 = System.nanoTime
      metrics.daml.HttpJsonApi.httpRequestThroughput.mark()
      fn(()).andThen { res =>
        res.onComplete(_ => logger.trace(s"Processed request after ${System.nanoTime() - t0}ns"))
        res
      }
    }
    // As futures are eager it is best to start the timer rather sooner than later
    // to get accurate timings. This also consistent with the implementation above which
    // logs the processing time.
    def withTimer(timer: Timer) = Directive { (fn: Unit => Route) => ctx =>
      Timed.future(timer, fn(())(ctx))
    }
    def path[L](pm: PathMatcher[L]) =
      server.Directives.path(pm) & markThroughputAndLogProcessingTime & logRequestAndResult
    val withCmdSubmitTimer: Directive0 = withTimer(commandSubmissionTimer)
    val withFetchTimer: Directive0 = withTimer(fetchTimer)
    concat(
      pathPrefix("v1") apply concat(
        post apply concat(
          path("create") & withCmdSubmitTimer apply toRoute(create(req)),
          path("exercise") & withCmdSubmitTimer apply toRoute(exercise(req)),
          path("create-and-exercise") & withCmdSubmitTimer apply toRoute(
            createAndExercise(req)
          ),
          path("query") & withTimer(queryMatchingTimer) apply toRoute(query(req)),
          path("fetch") & withFetchTimer apply toRoute(fetch(req)),
          path("user") apply toPostRoute(req, getUser),
          path("user" / "create") apply toPostRoute(req, createUser),
          path("user" / "delete") apply toPostRoute(req, deleteUser),
          path("user" / "rights") apply toPostRoute(req, listUserRights),
          path("user" / "rights" / "grant") apply toPostRoute(req, grantUserRights),
          path("user" / "rights" / "revoke") apply toPostRoute(req, revokeUserRights),
          path("parties") & withFetchTimer apply toPostRoute(req, parties),
          path("parties" / "allocate") & withTimer(allocatePartyTimer) apply toPostRoute(
            req,
            allocateParty,
          ),
          path("packages") apply toUploadDarFileRoute(req),
          path("metering-report") apply toPostRoute(req, meteringReportEndpoint.generateReport),
        ),
        get apply concat(
          path("query") & withTimer(queryAllTimer) apply
            toRoute(retrieveAll(req)),
          path("user") apply toGetRoute(req, getAuthenticatedUser),
          path("user" / "rights") apply toGetRoute(req, listAuthenticatedUserRights),
          path("users") apply toGetRoute(req, listUsers),
          path("parties") & withTimer(getPartyTimer) apply toGetRoute(req, allParties),
          path("packages") apply toGetRouteLedgerId(req, listPackages),
          path("packages" / ".+".r)(packageId =>
            withTimer(downloadPackageTimer) & extractRequest apply (req =>
              toDownloadPackageRoute(req, packageId, downloadPackage)
            )
          ),
        ),
      ),
      path("livez") apply responseToRoute(Future.successful(HttpResponse(status = StatusCodes.OK))),
      path("readyz") apply responseToRoute(healthService.ready().map(_.toHttpResponse)),
    )
  }

  private def httpResponse[T](output: T)(implicit
      T: MkHttpResponse[T],
      lc: LoggingContextOf[InstanceUUID with RequestID],
  ): Future[HttpResponse] =
    T.run(output)
      .recover(Error.fromThrowable andThen (httpResponseError(_)))

  private implicit def sourceStreamSearchResults[A: JsonWriter](implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): MkHttpResponse[ET[domain.SyncResponse[Source[Error \/ A, NotUsed]]]] =
    MkHttpResponse { output =>
      implicitly[MkHttpResponse[Future[Error \/ SearchResult[Error \/ JsValue]]]]
        .run(output.map(_ map (_ map (_ map ((_: A).toJson)))).run)
    }

  private implicit def searchResults(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): MkHttpResponse[Future[Error \/ SearchResult[Error \/ JsValue]]] =
    MkHttpResponse { output =>
      output.map(_.fold(httpResponseError, searchHttpResponse))
    }

  private implicit def mkHttpResponseEitherT(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): MkHttpResponse[ET[HttpResponse]] =
    MkHttpResponse { output =>
      implicitly[MkHttpResponse[Future[Error \/ HttpResponse]]].run(output.run)
    }

  private implicit def mkHttpResponse(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): MkHttpResponse[Future[Error \/ HttpResponse]] =
    MkHttpResponse { output =>
      output.map(_.fold(httpResponseError, identity))
    }

  private def searchHttpResponse(searchResult: SearchResult[Error \/ JsValue]): HttpResponse = {
    import json.JsonProtocol._

    val response: Source[ByteString, NotUsed] = searchResult match {
      case domain.OkResponse(result, warnings, _) =>
        val warningsJsVal: Option[JsValue] = warnings.map(SprayJson.encodeUnsafe(_))
        ResponseFormats.resultJsObject(result via filterStreamErrors, warningsJsVal)
      case error: domain.ErrorResponse =>
        val jsVal: JsValue = SprayJson.encodeUnsafe(error)
        Source.single(ByteString(jsVal.compactPrint))
    }

    HttpResponse(
      status = StatusCodes.OK,
      entity = HttpEntity
        .Chunked(ContentTypes.`application/json`, response.map(HttpEntity.ChunkStreamPart(_))),
    )
  }

  private[this] def filterStreamErrors[E, A]: Flow[Error \/ A, Error \/ A, NotUsed] =
    Flow[Error \/ A].map {
      case -\/(ServerError(_)) => -\/(ServerError.fromMsg("internal server error"))
      case o => o
    }

  private implicit def fullySync[A: JsonWriter](implicit
      metrics: Metrics,
      lc: LoggingContextOf[InstanceUUID with RequestID],
  ): MkHttpResponse[ET[domain.SyncResponse[A]]] = MkHttpResponse { result =>
    Timed.future(
      metrics.daml.HttpJsonApi.responseCreationTimer,
      result
        .flatMap { x =>
          either(SprayJson.encode1(x).map(y => (y, x.status)).liftErr(ServerError.fromMsg))
        }
        .run
        .map {
          case -\/(e) =>
            httpResponseError(e)
          case \/-((jsVal, status)) =>
            HttpResponse(
              entity = HttpEntity.Strict(ContentTypes.`application/json`, format(jsVal)),
              status = status,
            )
        },
    )
  }

}

object Endpoints {
  private[http] type ET[A] = EitherT[Future, Error, A]

  private[http] final class IntoEndpointsError[-A](val run: A => Error) extends AnyVal
  private[http] object IntoEndpointsError {
    import LedgerClientJwt.Grpc.Category

    implicit val id: IntoEndpointsError[Error] = new IntoEndpointsError(identity)

    implicit val fromCommands: IntoEndpointsError[CommandService.Error] = new IntoEndpointsError({
      case CommandService.InternalError(id, reason) =>
        ServerError(
          new Exception(
            s"command service error, ${id.cata(sym => s"${sym.name}: ", "")}${reason.getMessage}",
            reason,
          )
        )
      case CommandService.GrpcError(status) =>
        ParticipantServerError(status)
      case CommandService.ClientError(-\/(Category.PermissionDenied), message) =>
        Unauthorized(message)
      case CommandService.ClientError(\/-(Category.InvalidArgument), message) =>
        InvalidUserInput(message)
    })

    implicit val fromContracts: IntoEndpointsError[ContractsService.Error] =
      new IntoEndpointsError({ case ContractsService.InternalError(id, msg) =>
        ServerError.fromMsg(s"contracts service error, ${id.name}: $msg")
      })
  }

  private final case class MkHttpResponse[-T](run: T => Future[HttpResponse])
}
