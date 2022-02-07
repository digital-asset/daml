// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import akka.NotUsed
import akka.http.scaladsl.model._, headers.`Content-Type`
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives.extractClientIP
import akka.http.scaladsl.server.{Directive, Directive0, PathMatcher, Route}
import akka.http.scaladsl.server.RouteResult._
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Source}
import akka.util.ByteString
import com.codahale.metrics.Timer
import com.daml.lf.value.{Value => LfValue}
import ContractsService.SearchResult
import EndpointsCompanion._
import com.daml.scalautil.Statement.discard
import domain.{JwtPayload, JwtPayloadLedgerIdOnly}
import json._
import util.Collections.toNonEmptySet
import util.FutureUtil.{either, eitherT}
import util.Logging.{InstanceUUID, RequestID, extendWithRequestIdLogCtx}
import util.{ProtobufByteStrings, toLedgerId}
import util.JwtParties._
import com.daml.jwt.domain.Jwt
import com.daml.logging.LoggingContextOf.withEnrichedLoggingContext
import scalaz.std.scalaFuture._
import scalaz.syntax.std.option._
import scalaz.syntax.traverse._
import scalaz.{-\/, EitherT, NonEmptyList, \/, \/-}
import spray.json._

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import com.daml.logging.{ContextualizedLogger, LoggingContextOf}
import com.daml.metrics.{Metrics, Timed}
import akka.http.scaladsl.server.Directives._
import com.daml.ledger.api.{domain => LedgerApiDomain}
import com.daml.ledger.client.services.admin.UserManagementClient
import com.daml.ledger.client.services.identity.LedgerIdentityClient

class Endpoints(
    allowNonHttps: Boolean,
    decodeJwt: EndpointsCompanion.ValidateJwt,
    commandService: CommandService,
    contractsService: ContractsService,
    partiesService: PartiesService,
    packageManagementService: PackageManagementService,
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
  import routeSetup._, endpoints.RouteSetup._

  private[this] val commandsHelper: endpoints.CreateAndExercise =
    new endpoints.CreateAndExercise(routeSetup, decoder, commandService, contractsService)
  import commandsHelper._

  private[this] val userManagement: endpoints.UserManagement = new endpoints.UserManagement(
    routeSetup = routeSetup,
    decodeJwt = decodeJwt,
    userManagementClient,
  )
  import userManagement._

  private[this] val logger = ContextualizedLogger.get(getClass)

  import Endpoints._
  import json.JsonProtocol._
  import util.ErrorOps._

  private def responseToRoute(res: Future[HttpResponse]): Route = _ => res map Complete
  private def toRoute[T: MkHttpResponse](res: => T): Route =
    responseToRoute(httpResponse(res))

  private def mkRequestLogMsg(request: HttpRequest, remoteAddress: RemoteAddress) =
    s"Incoming ${request.method.value} request on ${request.uri} from $remoteAddress"

  private def mkResponseLogMsg(response: HttpResponse) =
    s"Responding to client with HTTP ${response.status}"

  // Always put this directive after a path to ensure
  // that you don't log request bodies multiple times (simply because a matching test was made multiple times).
  // TL;DR JUST PUT THIS THING AFTER YOUR FINAL PATH MATCHING
  private def logRequestResponseHelper(
      logIncomingRequest: (HttpRequest, RemoteAddress) => Future[Unit],
      logResponse: HttpResponse => Future[Unit],
  ): Directive0 =
    extractRequest & extractClientIP tflatMap { case (request, remoteAddress) =>
      mapRouteResultFuture { responseF =>
        for {
          _ <- logIncomingRequest(request, remoteAddress)
          response <- responseF
          _ <- response match {
            case Complete(httpResponse) => logResponse(httpResponse)
            case _ =>
              Future.failed(
                new RuntimeException(
                  """Logging the request & response should never happen on routes which get rejected.
                    |Make sure to place the directive only at places where a match is guaranteed (e.g. after the path directive).""".stripMargin
                )
              )
          }
        } yield response
      }
    }

  private def logJsonRequestAndResult(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): Directive0 = {
    def logWithHttpMessageBodyIfAvailable(
        httpMessage: HttpMessage,
        msg: String,
        bodyKind: String,
    ): Future[Unit] =
      if (
        httpMessage
          .header[`Content-Type`]
          .map(_.contentType)
          .contains(ContentTypes.`application/json`)
      )
        httpMessage
          .entity()
          .toStrict(maxTimeToCollectRequest)
          .map(it =>
            withEnrichedLoggingContext(
              LoggingContextOf.label[RequestEntity],
              s"${bodyKind}_body" -> it.data.utf8String.parseJson,
            )
              .run(implicit lc => logger.info(msg))
          )
          .recover { case ex =>
            logger.error("Failed to extract body for logging", ex)
          }
      else Future.successful(logger.info(msg))
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
          mkResponseLogMsg(httpResponse),
          "response",
        ),
    )
  }

  def logRequestAndResultSimple(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): Directive0 =
    logRequestResponseHelper(
      (request, remoteAddress) =>
        Future.successful(logger.info(mkRequestLogMsg(request, remoteAddress))),
      httpResponse => Future.successful(logger.info(mkResponseLogMsg(httpResponse))),
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
          path("user") apply toRoute(getUser(req)),
          path("user" / "create") apply toRoute(createUser(req)),
          path("user" / "delete") apply toRoute(deleteUser(req)),
          path("user" / "rights") apply toRoute(listUserRights(req)),
          path("user" / "rights" / "grant") apply toRoute(grantUserRights(req)),
          path("user" / "rights" / "revoke") apply toRoute(revokeUserRights(req)),
          path("parties") & withFetchTimer apply toRoute(parties(req)),
          path("parties" / "allocate") & withTimer(
            allocatePartyTimer
          ) apply toRoute(allocateParty(req)),
          path("packages") apply toRoute(uploadDarFile(req)),
        ),
        get apply concat(
          path("query") & withTimer(queryAllTimer) apply
            toRoute(retrieveAll(req)),
          path("user") apply toRoute(getAuthenticatedUser(req)),
          path("user" / "rights") apply toRoute(
            listAuthenticatedUserRights(req)
          ),
          path("users") apply toRoute(listUsers(req)),
          path("parties") & withTimer(getPartyTimer) apply
            toRoute(allParties(req)),
          path("packages") apply toRoute(listPackages(req)),
          path("packages" / ".+".r)(packageId =>
            withTimer(downloadPackageTimer) & extractRequest apply (req =>
              responseToRoute(downloadPackage(req, packageId))
            )
          ),
        ),
      ),
      path("livez") apply responseToRoute(Future.successful(HttpResponse(status = StatusCodes.OK))),
      path("readyz") apply responseToRoute(healthService.ready().map(_.toHttpResponse)),
    )
  }

  def fetch(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      ec: ExecutionContext,
      metrics: Metrics,
  ): ET[domain.SyncResponse[JsValue]] =
    for {
      parseAndDecodeTimerCtx <- getParseAndDecodeTimerCtx()
      input <- inputJsValAndJwtPayload(req): ET[(Jwt, JwtPayload, JsValue)]

      (jwt, jwtPayload, reqBody) = input

      jsVal <- withJwtPayloadLoggingContext(jwtPayload) { implicit lc =>
        logger.debug(s"/v1/fetch reqBody: $reqBody")
        for {
          fr <-
            either(
              SprayJson
                .decode[domain.FetchRequest[JsValue]](reqBody)
                .liftErr[Error](InvalidUserInput)
            )
              .flatMap(
                _.traverseLocator(
                  decoder
                    .decodeContractLocatorKey(_, jwt, toLedgerId(jwtPayload.ledgerId))
                    .liftErr(InvalidUserInput)
                )
              ): ET[domain.FetchRequest[LfValue]]
          _ <- EitherT.pure(parseAndDecodeTimerCtx.close())
          _ = logger.debug(s"/v1/fetch fr: $fr")

          _ <- either(ensureReadAsAllowedByJwt(fr.readAs, jwtPayload))
          ac <- eitherT(
            handleFutureFailure(contractsService.lookup(jwt, jwtPayload, fr))
          ): ET[Option[domain.ActiveContract[JsValue]]]

          jsVal <- either(
            ac.cata(x => toJsValue(x), \/-(JsNull))
          ): ET[JsValue]
        } yield jsVal
      }

    } yield domain.OkResponse(jsVal)

  def retrieveAll(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      metrics: Metrics,
  ): Future[Error \/ SearchResult[Error \/ JsValue]] = for {
    parseAndDecodeTimerCtx <- Future(
      metrics.daml.HttpJsonApi.incomingJsonParsingAndValidationTimer.time()
    )
    res <- inputAndJwtPayload[JwtPayload](req).run.map {
      _.map { case (jwt, jwtPayload, _) =>
        parseAndDecodeTimerCtx.close()
        withJwtPayloadLoggingContext(jwtPayload) { implicit lc =>
          val result: SearchResult[ContractsService.Error \/ domain.ActiveContract[LfValue]] =
            contractsService.retrieveAll(jwt, jwtPayload)

          domain.SyncResponse.covariant.map(result) { source =>
            source
              .via(handleSourceFailure)
              .map(_.flatMap(lfAcToJsValue)): Source[Error \/ JsValue, NotUsed]
          }
        }
      }
    }
  } yield res

  def query(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      metrics: Metrics,
  ): Future[Error \/ SearchResult[Error \/ JsValue]] = {
    for {
      it <- inputAndJwtPayload[JwtPayload](req).leftMap(identity[Error])
      (jwt, jwtPayload, reqBody) = it
      res <- withJwtPayloadLoggingContext(jwtPayload) { implicit lc =>
        val res = for {
          cmd <- SprayJson
            .decode[domain.GetActiveContractsRequest](reqBody)
            .liftErr[Error](InvalidUserInput)
          _ <- ensureReadAsAllowedByJwt(cmd.readAs, jwtPayload)
        } yield withEnrichedLoggingContext(
          LoggingContextOf.label[domain.GetActiveContractsRequest],
          "cmd" -> cmd.toString,
        ).run { implicit lc =>
          logger.debug("Processing a query request")
          contractsService
            .search(jwt, jwtPayload, cmd)
            .map(
              domain.SyncResponse.covariant.map(_)(
                _.via(handleSourceFailure)
                  .map(_.flatMap(toJsValue[domain.ActiveContract[JsValue]](_)))
              )
            )
        }
        eitherT(res.sequence)
      }
    } yield res
  }.run

  def allParties(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): ET[domain.SyncResponse[List[domain.PartyDetails]]] =
    proxyWithoutCommand((jwt, _) => partiesService.allParties(jwt))(req)
      .flatMap(pd => either(pd map (domain.OkResponse(_))))

  def parties(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): ET[domain.SyncResponse[List[domain.PartyDetails]]] =
    proxyWithCommand[NonEmptyList[domain.Party], (Set[domain.PartyDetails], Set[domain.Party])](
      (jwt, cmd) => partiesService.parties(jwt, toNonEmptySet(cmd))
    )(req)
      .map(ps => partiesResponse(parties = ps._1.toList, unknownParties = ps._2.toList))

  def allocateParty(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      metrics: Metrics,
  ): ET[domain.SyncResponse[domain.PartyDetails]] =
    EitherT
      .pure(metrics.daml.HttpJsonApi.allocatePartyThroughput.mark())
      .flatMap(_ => proxyWithCommand(partiesService.allocate)(req).map(domain.OkResponse(_)))

  def listPackages(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): ET[domain.SyncResponse[Seq[String]]] =
    proxyWithoutCommand(packageManagementService.listPackages)(req).map(domain.OkResponse(_))

  def downloadPackage(req: HttpRequest, packageId: String)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): Future[HttpResponse] = {
    val et: ET[admin.GetPackageResponse] =
      proxyWithoutCommand((jwt, ledgerId) =>
        packageManagementService.getPackage(jwt, ledgerId, packageId)
      )(req)
    val fa: Future[Error \/ admin.GetPackageResponse] = et.run
    fa.map {
      case -\/(e) =>
        httpResponseError(e)
      case \/-(x) =>
        HttpResponse(
          entity = HttpEntity.apply(
            ContentTypes.`application/octet-stream`,
            ProtobufByteStrings.toSource(x.archivePayload),
          )
        )
    }
  }

  def uploadDarFile(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      metrics: Metrics,
  ): ET[domain.SyncResponse[Unit]] =
    for {
      parseAndDecodeTimerCtx <- getParseAndDecodeTimerCtx()
      _ <- EitherT.pure(metrics.daml.HttpJsonApi.uploadPackagesThroughput.mark())
      t2 <- inputSource(req)
      (jwt, payload, source) = t2
      _ <- EitherT.pure(parseAndDecodeTimerCtx.close())

      _ <- eitherT(
        handleFutureFailure(
          packageManagementService.uploadDarFile(
            jwt,
            toLedgerId(payload.ledgerId),
            source.mapMaterializedValue(_ => NotUsed),
          )
        )
      ): ET[Unit]

    } yield domain.OkResponse(())

  private def handleFutureFailure[A](fa: Future[A])(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): Future[Error \/ A] =
    fa.map(a => \/-(a)).recover(logException("Future") andThen Error.fromThrowable andThen (-\/(_)))

  private def handleSourceFailure[E, A](implicit
      E: IntoEndpointsError[E],
      lc: LoggingContextOf[InstanceUUID with RequestID],
  ): Flow[E \/ A, Error \/ A, NotUsed] =
    Flow
      .fromFunction((_: E \/ A).leftMap(E.run))
      .recover(logException("Source") andThen Error.fromThrowable andThen (-\/(_)))

  private def httpResponse[T](output: T)(implicit T: MkHttpResponse[T]): Future[HttpResponse] =
    T.run(output)
      .recover(Error.fromThrowable andThen (httpResponseError(_)))

  private implicit def sourceStreamSearchResults[A: JsonWriter]
      : MkHttpResponse[ET[domain.SyncResponse[Source[Error \/ A, NotUsed]]]] =
    MkHttpResponse { output =>
      implicitly[MkHttpResponse[Future[Error \/ SearchResult[Error \/ JsValue]]]]
        .run(output.map(_ map (_ map (_ map ((_: A).toJson)))).run)
    }

  private implicit def searchResults
      : MkHttpResponse[Future[Error \/ SearchResult[Error \/ JsValue]]] =
    MkHttpResponse { output =>
      output.map(_.fold(httpResponseError, searchHttpResponse))
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
      case -\/(ServerError(_)) => -\/(ServerError("internal server error"))
      case o => o
    }

  private implicit def fullySync[A: JsonWriter](implicit
      metrics: Metrics
  ): MkHttpResponse[ET[domain.SyncResponse[A]]] = MkHttpResponse { result =>
    Timed.future(
      metrics.daml.HttpJsonApi.responseCreationTimer,
      result
        .flatMap { x =>
          either(SprayJson.encode1(x).map(y => (y, x.status)).liftErr(ServerError))
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

  private[http] def inputAndJwtPayload[P](
      req: HttpRequest
  )(implicit
      legacyParse: ParsePayload[P],
      createFromUserToken: CreateFromUserToken[P],
      lc: LoggingContextOf[InstanceUUID with RequestID],
  ): EitherT[Future, Unauthorized, (Jwt, P, String)] =
    eitherT(input(req)).flatMap(it => withJwtPayload[String, P](it))

  private[http] def inputSource(
      req: HttpRequest
  )(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): ET[(Jwt, JwtPayloadLedgerIdOnly, Source[ByteString, Any])] =
    either(findJwt(req))
      .leftMap { e =>
        discard { req.entity.discardBytes(mat) }
        e: Error
      }
      .flatMap(j =>
        withJwtPayload[Source[ByteString, Any], JwtPayloadLedgerIdOnly]((j, req.entity.dataBytes))
          .leftMap(it => it: Error)
      )

  private def proxyWithoutCommand[A](
      fn: (Jwt, LedgerApiDomain.LedgerId) => Future[A]
  )(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): ET[A] =
    for {
      t3 <- inputAndJwtPayload[JwtPayloadLedgerIdOnly](req).leftMap(it => it: Error)
      a <- eitherT(handleFutureFailure(fn(t3._1, toLedgerId(t3._2.ledgerId)))): ET[A]
    } yield a

}

object Endpoints {
  import json.JsonProtocol._
  import util.ErrorOps._

  private[http] type ET[A] = EitherT[Future, Error, A]

  private[http] final class IntoEndpointsError[-A](val run: A => Error) extends AnyVal
  private[http] object IntoEndpointsError {
    import LedgerClientJwt.Grpc.Category

    implicit val id: IntoEndpointsError[Error] = new IntoEndpointsError(identity)

    implicit val fromCommands: IntoEndpointsError[CommandService.Error] = new IntoEndpointsError({
      case CommandService.InternalError(id, message) =>
        ServerError(s"command service error, ${id.cata(sym => s"${sym.name}: ", "")}$message")
      case CommandService.GrpcError(status) =>
        ParticipantServerError(status.getCode, Option(status.getDescription))
      case CommandService.ClientError(-\/(Category.PermissionDenied), message) =>
        Unauthorized(message)
      case CommandService.ClientError(\/-(Category.InvalidArgument), message) =>
        InvalidUserInput(message)
    })

    implicit val fromContracts: IntoEndpointsError[ContractsService.Error] =
      new IntoEndpointsError({ case ContractsService.InternalError(id, msg) =>
        ServerError(s"contracts service error, ${id.name}: $msg")
      })
  }

  private final case class MkHttpResponse[-T](run: T => Future[HttpResponse])

  private def lfValueToJsValue(a: LfValue): Error \/ JsValue =
    \/.attempt(LfValueCodec.apiValueToJsValue(a))(identity).liftErr(ServerError)

  private def lfAcToJsValue(a: domain.ActiveContract[LfValue]): Error \/ JsValue = {
    for {
      b <- a.traverse(lfValueToJsValue): Error \/ domain.ActiveContract[JsValue]
      c <- toJsValue(b)
    } yield c
  }

  private def partiesResponse(
      parties: List[domain.PartyDetails],
      unknownParties: List[domain.Party],
  ): domain.SyncResponse[List[domain.PartyDetails]] = {

    val warnings: Option[domain.UnknownParties] =
      if (unknownParties.isEmpty) None
      else Some(domain.UnknownParties(unknownParties))

    domain.OkResponse(parties, warnings)
  }

  private def toJsValue[A: JsonWriter](a: A): Error \/ JsValue = {
    SprayJson.encode(a).liftErr(ServerError)
  }
}
