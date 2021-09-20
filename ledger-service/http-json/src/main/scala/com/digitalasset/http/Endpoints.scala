// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import akka.NotUsed
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{
  Authorization,
  ModeledCustomHeader,
  ModeledCustomHeaderCompanion,
  OAuth2BearerToken,
  `Content-Type`,
  `X-Forwarded-Proto`,
}
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives.extractClientIP
import akka.http.scaladsl.server.{Directive, Directive0, PathMatcher, Route}
import akka.http.scaladsl.server.RouteResult._
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Source}
import akka.util.ByteString
import com.codahale.metrics.Timer
import com.daml.lf
import com.daml.http.ContractsService.SearchResult
import com.daml.http.EndpointsCompanion._
import com.daml.scalautil.Statement.discard
import com.daml.http.domain.{
  JwtPayload,
  JwtPayloadG,
  JwtPayloadLedgerIdOnly,
  JwtPayloadTag,
  JwtWritePayload,
  TemplateId,
}
import com.daml.http.json._
import com.daml.http.util.Collections.toNonEmptySet
import com.daml.http.util.FutureUtil.{either, eitherT}
import com.daml.http.util.Logging.{InstanceUUID, RequestID, extendWithRequestIdLogCtx}
import com.daml.http.util.{ProtobufByteStrings, toLedgerId}
import com.daml.jwt.domain.Jwt
import com.daml.ledger.api.{v1 => lav1}
import com.daml.logging.LoggingContextOf.withEnrichedLoggingContext
import com.daml.scalautil.ExceptionOps._
import scalaz.std.scalaFuture._
import scalaz.syntax.std.option._
import scalaz.syntax.show._
import scalaz.syntax.traverse._
import scalaz.{-\/, EitherT, NonEmptyList, Show, Traverse, \/, \/-}
import spray.json._

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try
import scala.util.control.NonFatal
import com.daml.logging.{ContextualizedLogger, LoggingContextOf}
import com.daml.metrics.{Metrics, Timed}
import akka.http.scaladsl.server.Directives._
import com.daml.ledger.api.{domain => LedgerApiDomain}

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
    maxTimeToCollectRequest: FiniteDuration = FiniteDuration(5, "seconds"),
)(implicit ec: ExecutionContext, mat: Materializer) {

  private[this] val logger = ContextualizedLogger.get(getClass)

  import Endpoints._
  import encoder.implicits._
  import json.JsonProtocol._
  import util.ErrorOps._

  private def responseToRoute(res: Future[HttpResponse]): Route = _ => res map Complete
  private def toRoute[A](
      res: ET[domain.SyncResponse[A]]
  )(implicit metrics: Metrics, jsonWriter: JsonWriter[A]): Route =
    responseToRoute(httpResponse(res))
  private def toRoute(res: => Future[Error \/ SearchResult[Error \/ JsValue]]): Route =
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
          path("parties") & withFetchTimer apply toRoute(parties(req)),
          path("parties" / "allocate") & withTimer(
            allocatePartyTimer
          ) apply toRoute(allocateParty(req)),
          path("packages") apply toRoute(uploadDarFile(req)),
        ),
        get apply concat(
          path("query") & withTimer(queryAllTimer) apply
            toRoute(retrieveAll(req)),
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

  def getParseAndDecodeTimerCtx()(implicit
      metrics: Metrics
  ): ET[Timer.Context] =
    EitherT.pure(metrics.daml.HttpJsonApi.incomingJsonParsingAndValidationTimer.time())

  def withJwtPayloadLoggingContext[A](jwtPayload: JwtPayloadG)(
      fn: LoggingContextOf[JwtPayloadTag with InstanceUUID with RequestID] => A
  )(implicit lc: LoggingContextOf[InstanceUUID with RequestID]): A =
    withEnrichedLoggingContext(
      LoggingContextOf.label[JwtPayloadTag],
      "ledger_id" -> jwtPayload.ledgerId.toString,
      "act_as" -> jwtPayload.actAs.toString,
      "application_id" -> jwtPayload.applicationId.toString,
      "read_as" -> jwtPayload.readAs.toString,
    ).run(fn)

  def handleCommand[T[_]](req: HttpRequest)(
      fn: (
          Jwt,
          JwtWritePayload,
          JsValue,
          Timer.Context,
      ) => LoggingContextOf[JwtPayloadTag with InstanceUUID with RequestID] => ET[
        T[ApiValue]
      ]
  )(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      ev1: JsonWriter[T[JsValue]],
      ev2: Traverse[T],
      metrics: Metrics,
  ): ET[domain.SyncResponse[JsValue]] =
    for {
      parseAndDecodeTimerCtx <- getParseAndDecodeTimerCtx()
      _ <- EitherT.pure(metrics.daml.HttpJsonApi.commandSubmissionThroughput.mark())
      t3 <- inputJsValAndJwtPayload(req): ET[(Jwt, JwtWritePayload, JsValue)]
      (jwt, jwtPayload, reqBody) = t3
      resp <- withJwtPayloadLoggingContext(jwtPayload)(
        fn(jwt, jwtPayload, reqBody, parseAndDecodeTimerCtx)
      )
      jsVal <- either(SprayJson.encode1(resp).liftErr(ServerError)): ET[JsValue]
    } yield domain.OkResponse(jsVal)

  def create(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      ec: ExecutionContext,
      metrics: Metrics,
  ): ET[domain.SyncResponse[JsValue]] =
    handleCommand(req) { (jwt, jwtPayload, reqBody, parseAndDecodeTimerCtx) => implicit lc =>
      for {
        cmd <-
          decoder
            .decodeCreateCommand(reqBody, jwt, toLedgerId(jwtPayload.ledgerId))
            .liftErr(InvalidUserInput): ET[domain.CreateCommand[ApiRecord, TemplateId.RequiredPkg]]
        _ <- EitherT.pure(parseAndDecodeTimerCtx.close())

        ac <- eitherT(
          Timed.future(
            metrics.daml.HttpJsonApi.commandSubmissionLedgerTimer,
            handleFutureEitherFailure(commandService.create(jwt, jwtPayload, cmd)),
          )
        ): ET[domain.ActiveContract[ApiValue]]
      } yield ac
    }

  def exercise(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      ec: ExecutionContext,
      metrics: Metrics,
  ): ET[domain.SyncResponse[JsValue]] =
    handleCommand(req) { (jwt, jwtPayload, reqBody, parseAndDecodeTimerCtx) => implicit lc =>
      for {
        cmd <-
          decoder
            .decodeExerciseCommand(reqBody, jwt, toLedgerId(jwtPayload.ledgerId))
            .liftErr(InvalidUserInput): ET[
            domain.ExerciseCommand[LfValue, domain.ContractLocator[LfValue]]
          ]
        _ <- EitherT.pure(parseAndDecodeTimerCtx.close())
        resolvedRef <- eitherT(
          resolveReference(jwt, jwtPayload, cmd.reference)
        ): ET[domain.ResolvedContractRef[ApiValue]]

        apiArg <- either(lfValueToApiValue(cmd.argument)): ET[ApiValue]

        resolvedCmd = cmd.copy(argument = apiArg, reference = resolvedRef)

        resp <- eitherT(
          Timed.future(
            metrics.daml.HttpJsonApi.commandSubmissionLedgerTimer,
            handleFutureEitherFailure(
              commandService.exercise(jwt, jwtPayload, resolvedCmd)
            ),
          )
        ): ET[domain.ExerciseResponse[ApiValue]]

      } yield resp
    }

  def createAndExercise(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      metrics: Metrics,
  ): ET[domain.SyncResponse[JsValue]] =
    handleCommand(req) { (jwt, jwtPayload, reqBody, parseAndDecodeTimerCtx) => implicit lc =>
      for {
        cmd <-
          decoder
            .decodeCreateAndExerciseCommand(reqBody, jwt, toLedgerId(jwtPayload.ledgerId))
            .liftErr(InvalidUserInput): ET[
            domain.CreateAndExerciseCommand[ApiRecord, ApiValue, TemplateId.RequiredPkg]
          ]
        _ <- EitherT.pure(parseAndDecodeTimerCtx.close())

        resp <- eitherT(
          Timed.future(
            metrics.daml.HttpJsonApi.commandSubmissionLedgerTimer,
            handleFutureEitherFailure(
              commandService.createAndExercise(jwt, jwtPayload, cmd)
            ),
          )
        ): ET[domain.ExerciseResponse[ApiValue]]
      } yield resp
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
          cl <-
            decoder
              .decodeContractLocator(reqBody, jwt, toLedgerId(jwtPayload.ledgerId))
              .liftErr(InvalidUserInput): ET[domain.ContractLocator[LfValue]]
          _ <- EitherT.pure(parseAndDecodeTimerCtx.close())
          _ = logger.debug(s"/v1/fetch cl: $cl")

          ac <- eitherT(
            handleFutureFailure(contractsService.lookup(jwt, jwtPayload, cl))
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
    res <- inputAndJwtPayload[JwtPayload](req).map {
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
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): Future[Error \/ SearchResult[Error \/ JsValue]] = {
    for {
      it <- EitherT.eitherT(inputAndJwtPayload[JwtPayload](req))
      (jwt, jwtPayload, reqBody) = it
      res <- withJwtPayloadLoggingContext(jwtPayload) { implicit lc =>
        val res =
          SprayJson
            .decode[domain.GetActiveContractsRequest](reqBody)
            .liftErr[Error](InvalidUserInput)
            .map { cmd =>
              withEnrichedLoggingContext(
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
            }
            .sequence
        eitherT(res)
      }
    } yield res
  }.run

  def allParties(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): ET[domain.SyncResponse[List[domain.PartyDetails]]] =
    proxyWithoutCommand((jwt, _) => partiesService.allParties(jwt))(req).map(domain.OkResponse(_))

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
      t2 <- either(inputSource(req)): ET[(Jwt, JwtPayloadLedgerIdOnly, Source[ByteString, Any])]
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

  private def handleFutureEitherFailure[A, B](fa: Future[A \/ B])(implicit
      A: IntoServerError[A],
      lc: LoggingContextOf[InstanceUUID with RequestID],
  ): Future[Error \/ B] =
    fa.map(_ leftMap A.run).recover { case NonFatal(e) =>
      logger.error("Future failed", e)
      -\/(ServerError(e.description))
    }

  private def handleFutureFailure[E >: ServerError, A](fa: Future[A])(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): Future[E \/ A] =
    fa.map(a => \/-(a)).recover { case NonFatal(e) =>
      logger.error("Future failed", e)
      -\/(ServerError(e.description))
    }

  private def handleSourceFailure[E: Show, A](implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): Flow[E \/ A, Error \/ A, NotUsed] =
    Flow
      .fromFunction((_: E \/ A).liftErr[Error](ServerError))
      .recover { case NonFatal(e) =>
        logger.error("Source failed", e)
        -\/(ServerError(e.description))
      }

  private def httpResponse(
      output: Future[Error \/ SearchResult[Error \/ JsValue]]
  ): Future[HttpResponse] =
    output
      .map {
        case -\/(e) => httpResponseError(e)
        case \/-(searchResult) => httpResponse(searchResult)
      }
      .recover { case NonFatal(e) =>
        httpResponseError(ServerError(e.description))
      }

  private def httpResponse(searchResult: SearchResult[Error \/ JsValue]): HttpResponse = {
    import json.JsonProtocol._

    val response: Source[ByteString, NotUsed] = searchResult match {
      case domain.OkResponse(result, warnings, _) =>
        val warningsJsVal: Option[JsValue] = warnings.map(SprayJson.encodeUnsafe(_))
        ResponseFormats.resultJsObject(result, warningsJsVal)
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

  private def httpResponse[A: JsonWriter](
      result: ET[domain.SyncResponse[A]]
  )(implicit metrics: Metrics): Future[HttpResponse] = {
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
        }
        .recover { case NonFatal(e) =>
          httpResponseError(ServerError(e.description))
        },
    )
  }

  private[http] def data(entity: RequestEntity): Future[String] =
    entity.toStrict(maxTimeToCollectRequest).map(_.data.utf8String)

  private[http] def input(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): Future[Unauthorized \/ (Jwt, String)] = {
    findJwt(req) match {
      case e @ -\/(_) =>
        discard { req.entity.discardBytes(mat) }
        Future.successful(e)
      case \/-(j) =>
        data(req.entity).map(d => \/-((j, d)))
    }
  }

  private[http] def inputJsVal(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): ET[(Jwt, JsValue)] =
    for {
      t2 <- eitherT(input(req)): ET[(Jwt, String)]
      jsVal <- either(SprayJson.parse(t2._2).liftErr(InvalidUserInput)): ET[JsValue]
    } yield (t2._1, jsVal)

  private[http] def withJwtPayload[A, P](fa: (Jwt, A))(implicit
      parse: ParsePayload[P]
  ): Unauthorized \/ (Jwt, P, A) =
    decodeAndParsePayload[P](fa._1, decodeJwt).map(t2 => (t2._1, t2._2, fa._2))

  private[http] def inputAndJwtPayload[P](
      req: HttpRequest
  )(implicit
      parse: ParsePayload[P],
      lc: LoggingContextOf[InstanceUUID with RequestID],
  ): Future[Error \/ (Jwt, P, String)] =
    input(req).map(_.flatMap(withJwtPayload[String, P]))

  private[http] def inputJsValAndJwtPayload[P](req: HttpRequest)(implicit
      parse: ParsePayload[P],
      lc: LoggingContextOf[InstanceUUID with RequestID],
  ): ET[(Jwt, P, JsValue)] =
    inputJsVal(req).flatMap(x => either(withJwtPayload[JsValue, P](x)))

  private[http] def inputSource(
      req: HttpRequest
  )(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): Error \/ (Jwt, JwtPayloadLedgerIdOnly, Source[ByteString, Any]) =
    findJwt(req)
      .leftMap { e =>
        discard { req.entity.discardBytes(mat) }
        e
      }
      .flatMap(j =>
        withJwtPayload[Source[ByteString, Any], JwtPayloadLedgerIdOnly]((j, req.entity.dataBytes))
      )

  private[this] def findJwt(req: HttpRequest)(implicit
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

  private[this] def ensureHttpsForwarded(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): Unauthorized \/ Unit =
    if (allowNonHttps || isForwardedForHttps(req.headers)) \/-(())
    else {
      logger.warn(nonHttpsErrorMessage)
      \/-(())
    }

  private[this] def isForwardedForHttps(headers: Seq[HttpHeader]): Boolean =
    headers exists {
      case `X-Forwarded-Proto`(protocol) => protocol equalsIgnoreCase "https"
      // the whole "custom headers" thing in akka-http is a mishmash of
      // actually using the ModeledCustomHeaderCompanion stuff (which works)
      // and "just use ClassTag YOLO" (which won't work)
      case Forwarded(value) => Forwarded(value).proto contains "https"
      case _ => false
    }

  private def resolveReference(
      jwt: Jwt,
      jwtPayload: JwtWritePayload,
      reference: domain.ContractLocator[LfValue],
  )(implicit
      lc: LoggingContextOf[JwtPayloadTag with InstanceUUID with RequestID],
      metrics: Metrics,
  ): Future[Error \/ domain.ResolvedContractRef[ApiValue]] =
    contractsService
      .resolveContractReference(jwt, jwtPayload.parties, reference, toLedgerId(jwtPayload.ledgerId))
      .map { o: Option[domain.ResolvedContractRef[LfValue]] =>
        val a: Error \/ domain.ResolvedContractRef[LfValue] =
          o.toRightDisjunction(InvalidUserInput(ErrorMessages.cannotResolveTemplateId(reference)))
        a.flatMap {
          case -\/((tpId, key)) => lfValueToApiValue(key).map(k => -\/((tpId, k)))
          case a @ \/-((_, _)) => \/-(a)
        }
      }

  private def proxyWithoutCommand[A](
      fn: (Jwt, LedgerApiDomain.LedgerId) => Future[A]
  )(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): ET[A] =
    for {
      t3 <- eitherT(inputAndJwtPayload[JwtPayloadLedgerIdOnly](req)): ET[
        (Jwt, JwtPayloadLedgerIdOnly, _)
      ]
      a <- eitherT(handleFutureFailure(fn(t3._1, toLedgerId(t3._2.ledgerId)))): ET[A]
    } yield a

  private def proxyWithCommand[A: JsonReader, R](
      fn: (Jwt, A) => Future[Error \/ R]
  )(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): ET[R] =
    for {
      t2 <- inputJsVal(req): ET[(Jwt, JsValue)]
      (jwt, reqBody) = t2
      a <- either(SprayJson.decode[A](reqBody).liftErr(InvalidUserInput)): ET[A]
      b <- eitherT(handleFutureEitherFailure(fn(jwt, a))): ET[R]
    } yield b
}

object Endpoints {
  import json.JsonProtocol._
  import util.ErrorOps._

  private type ET[A] = EitherT[Future, Error, A]

  private type ApiRecord = lav1.value.Record
  private type ApiValue = lav1.value.Value

  private type LfValue = lf.value.Value

  private final class IntoServerError[-A](val run: A => Error) extends AnyVal
  private object IntoServerError extends IntoServerErrorLow {
    implicit val id: IntoServerError[Error] = new IntoServerError(identity)
  }
  private sealed abstract class IntoServerErrorLow {
    implicit def shown[A: Show]: IntoServerError[A] = new IntoServerError(a => ServerError(a.shows))
  }

  private def lfValueToJsValue(a: LfValue): Error \/ JsValue =
    \/.attempt(LfValueCodec.apiValueToJsValue(a))(identity).liftErr(ServerError)

  private def lfValueToApiValue(a: LfValue): Error \/ ApiValue =
    JsValueToApiValueConverter.lfValueToApiValue(a).liftErr(ServerError)

  private def lfAcToJsValue(a: domain.ActiveContract[LfValue]): Error \/ JsValue = {
    for {
      b <- a.traverse(lfValueToJsValue): Error \/ domain.ActiveContract[JsValue]
      c <- toJsValue(b)
    } yield c
  }

  private[http] val nonHttpsErrorMessage =
    "missing HTTPS reverse-proxy request headers; for development launch with --allow-insecure-tokens"

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

  // avoid case class to avoid using the wrong unapply in isForwardedForHttps
  private[http] final class Forwarded(override val value: String)
      extends ModeledCustomHeader[Forwarded] {
    override def companion = Forwarded
    override def renderInRequests = true
    override def renderInResponses = false
    // per discussion https://github.com/digital-asset/daml/pull/5660#discussion_r412539107
    def proto: Option[String] =
      Forwarded.re findFirstMatchIn value map (_.group(1).toLowerCase)
  }

  private[http] object Forwarded extends ModeledCustomHeaderCompanion[Forwarded] {
    override val name = "Forwarded"
    override def parse(value: String) = Try(new Forwarded(value))
    private val re = raw"""(?i)proto\s*=\s*"?(https?)""".r
  }
}
