// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import akka.stream.scaladsl.{Flow, Source, Sink}
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
import util.JwtParties._
import com.daml.jwt.domain.Jwt
import com.daml.ledger.api.{v1 => lav1}
import com.daml.logging.LoggingContextOf.withEnrichedLoggingContext
import scalaz.std.scalaFuture._
import scalaz.syntax.std.option._
import scalaz.syntax.traverse._
import scalaz.{-\/, EitherT, NonEmptyList, Traverse, \/, \/-}
import spray.json._

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try
import scala.util.control.NonFatal
import com.daml.logging.{ContextualizedLogger, LoggingContextOf}
import com.daml.metrics.{Metrics, Timed}
import akka.http.scaladsl.server.Directives._
import com.daml.ledger.api.domain.{User, UserRight}
import com.daml.ledger.api.{domain => LedgerApiDomain}
import com.daml.ledger.client.services.admin.UserManagementClient
import com.daml.ledger.client.services.identity.LedgerIdentityClient
import com.daml.lf.data.Ref.UserId

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

  private[this] val logger = ContextualizedLogger.get(getClass)

  import Endpoints._
  import encoder.implicits._
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
          resolveReference(jwt, jwtPayload, cmd.meta, cmd.reference)
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

  def deleteUser(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): ET[domain.SyncResponse[spray.json.JsObject]] =
    proxyWithCommandET { (jwt, deleteUserRequest: domain.DeleteUserRequest) =>
      for {
        userId <- parseUserId(deleteUserRequest.userId)
        _ <- EitherT.rightT(userManagementClient.deleteUser(userId, Some(jwt.value)))
      } yield emptyObjectResponse
    }(req)

  private def aggregateListUserPages(
      token: Option[String],
      pageToken: String = "",
      pageSize: Int = 1000, // TODO could be made configurable in the future
  ): Future[Seq[User]] = {
    import scalaz.std.option._
    val userPageStream: Source[Seq[User], NotUsed] = Source.unfoldAsync(some(pageToken)) {
      _ traverse { pageToken =>
        userManagementClient.listUsers(token, pageToken, pageSize).map {
          case (users, "") => (None, users)
          case (users, pageToken) => (Some(pageToken), users)
        }
      }
    }
    userPageStream mapConcat identity runWith Sink.seq
  }

  def listUsers(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): ET[domain.SyncResponse[List[domain.UserDetails]]] =
    for {
      jwt <- eitherT(input(req)).bimap(identity[Error], _._1)
      users <- EitherT.rightT(aggregateListUserPages(Some(jwt.value)))
    } yield domain.OkResponse(users.map(domain.UserDetails.fromUser).toList)

  def listUserRights(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): ET[domain.SyncResponse[List[domain.UserRight]]] =
    proxyWithCommandET { (jwt, listUserRightsRequest: domain.ListUserRightsRequest) =>
      for {
        userId <- parseUserId(listUserRightsRequest.userId)
        rights <- EitherT.rightT(
          userManagementClient.listUserRights(userId, Some(jwt.value))
        )
      } yield domain
        .OkResponse(domain.UserRights.fromLedgerUserRights(rights)): domain.SyncResponse[
        List[domain.UserRight]
      ]
    }(req)

  def listAuthenticatedUserRights(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): ET[domain.SyncResponse[List[domain.UserRight]]] =
    for {
      jwt <- eitherT(input(req)).bimap(identity[Error], _._1)
      userId <- getUserIdFromToken(jwt)
      rights <- EitherT.rightT(
        userManagementClient.listUserRights(userId, Some(jwt.value))
      )
    } yield domain
      .OkResponse(domain.UserRights.fromLedgerUserRights(rights)): domain.SyncResponse[List[
      domain.UserRight
    ]]

  def grantUserRights(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): ET[domain.SyncResponse[List[domain.UserRight]]] =
    proxyWithCommandET { (jwt, grantUserRightsRequest: domain.GrantUserRightsRequest) =>
      for {
        userId <- parseUserId(grantUserRightsRequest.userId)
        rights <- either(
          domain.UserRights.toLedgerUserRights(grantUserRightsRequest.rights)
        ).leftMap(InvalidUserInput): ET[List[UserRight]]
        grantedUserRights <- EitherT.rightT(
          userManagementClient.grantUserRights(userId, rights, Some(jwt.value))
        )
      } yield domain.OkResponse(
        domain.UserRights.fromLedgerUserRights(grantedUserRights)
      ): domain.SyncResponse[List[domain.UserRight]]
    }(req)

  def revokeUserRights(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): ET[domain.SyncResponse[List[domain.UserRight]]] =
    proxyWithCommandET { (jwt, revokeUserRightsRequest: domain.RevokeUserRightsRequest) =>
      for {
        userId <- parseUserId(revokeUserRightsRequest.userId)
        rights <- either(
          domain.UserRights.toLedgerUserRights(revokeUserRightsRequest.rights)
        ).leftMap(InvalidUserInput): ET[List[UserRight]]
        revokedUserRights <- EitherT.rightT(
          userManagementClient.revokeUserRights(userId, rights, Some(jwt.value))
        )
      } yield domain.OkResponse(
        domain.UserRights.fromLedgerUserRights(revokedUserRights)
      ): domain.SyncResponse[List[domain.UserRight]]
    }(req)

  def getUser(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): ET[domain.SyncResponse[domain.UserDetails]] =
    proxyWithCommandET { (jwt, getUserRequest: domain.GetUserRequest) =>
      for {
        userId <- parseUserId(getUserRequest.userId)
        user <- EitherT.rightT(userManagementClient.getUser(userId, Some(jwt.value)))
      } yield domain.OkResponse(
        domain.UserDetails(user.id, user.primaryParty)
      ): domain.SyncResponse[domain.UserDetails]
    }(req)

  def getAuthenticatedUser(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): ET[domain.SyncResponse[domain.UserDetails]] =
    for {
      jwt <- eitherT(input(req)).bimap(identity[Error], _._1)
      userId <- getUserIdFromToken(jwt)
      user <- EitherT.rightT(userManagementClient.getUser(userId, Some(jwt.value)))
    } yield domain.OkResponse(domain.UserDetails(user.id, user.primaryParty))

  def parties(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): ET[domain.SyncResponse[List[domain.PartyDetails]]] =
    proxyWithCommand[NonEmptyList[domain.Party], (Set[domain.PartyDetails], Set[domain.Party])](
      (jwt, cmd) => partiesService.parties(jwt, toNonEmptySet(cmd))
    )(req)
      .map(ps => partiesResponse(parties = ps._1.toList, unknownParties = ps._2.toList))

  def createUser(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): ET[domain.SyncResponse[spray.json.JsObject]] =
    proxyWithCommand { (jwt, createUserRequest: domain.CreateUserRequest) =>
      {
        import scalaz.std.option._
        import scalaz.syntax.traverse._
        import scalaz.syntax.std.either._
        import com.daml.lf.data.Ref
        val input =
          for {
            username <- UserId.fromString(createUserRequest.userId).disjunction
            primaryParty <- createUserRequest.primaryParty.traverse(it =>
              Ref.Party.fromString(it).disjunction
            )
            rights <- domain.UserRights.toLedgerUserRights(
              createUserRequest.rights.getOrElse(List.empty)
            )
          } yield (username, primaryParty, rights)
        for {
          info <- EitherT.either(input.leftMap(InvalidUserInput)): ET[
            (UserId, Option[Ref.Party], List[UserRight])
          ]
          (username, primaryParty, initialRights) = info
          _ <- EitherT.rightT(
            userManagementClient.createUser(
              User(username, primaryParty),
              initialRights,
              Some(jwt.value),
            )
          )
        } yield emptyObjectResponse
      }.run
    }(req)

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

  private def handleFutureEitherFailure[A, B](fa: Future[A \/ B])(implicit
      A: IntoEndpointsError[A],
      lc: LoggingContextOf[InstanceUUID with RequestID],
  ): Future[Error \/ B] =
    fa.map(_ leftMap A.run)
      .recover(logException("Future") andThen Error.fromThrowable andThen (-\/(_)))

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

  private implicit def hrSearchResults
      : MkHttpResponse[Future[Error \/ SearchResult[Error \/ JsValue]]] =
    MkHttpResponse { output =>
      output
        .map {
          case -\/(e) => httpResponseError(e)
          case \/-(searchResult) => searchHttpResponse(searchResult)
        }
        .recover(Error.fromThrowable andThen (httpResponseError(_)))
    }

  private[this] def logException(fromWhat: String)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): Throwable PartialFunction Throwable = { case NonFatal(e) =>
    logger.error(s"$fromWhat failed", e)
    e
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

  private implicit def hrFullySync[A: JsonWriter](implicit
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
        }
        .recover(Error.fromThrowable andThen (httpResponseError(_))),
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
      lc: LoggingContextOf[InstanceUUID with RequestID],
      legacyParse: ParsePayload[P],
      createFromUserToken: CreateFromUserToken[P],
  ): EitherT[Future, Unauthorized, (Jwt, P, A)] =
    decodeAndParsePayload[P](fa._1, decodeJwt, userManagementClient, ledgerIdentityClient).map(t2 =>
      (t2._1, t2._2, fa._2)
    )

  private[http] def inputAndJwtPayload[P](
      req: HttpRequest
  )(implicit
      legacyParse: ParsePayload[P],
      createFromUserToken: CreateFromUserToken[P],
      lc: LoggingContextOf[InstanceUUID with RequestID],
  ): EitherT[Future, Unauthorized, (Jwt, P, String)] =
    eitherT(input(req)).flatMap(it => withJwtPayload[String, P](it))

  private[http] def inputJsValAndJwtPayload[P](req: HttpRequest)(implicit
      legacyParse: ParsePayload[P],
      createFromUserToken: CreateFromUserToken[P],
      lc: LoggingContextOf[InstanceUUID with RequestID],
  ): EitherT[Future, Error, (Jwt, P, JsValue)] =
    inputJsVal(req).flatMap(x => withJwtPayload[JsValue, P](x).leftMap(it => it: Error))

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
      meta: Option[domain.CommandMeta],
      reference: domain.ContractLocator[LfValue],
  )(implicit
      lc: LoggingContextOf[JwtPayloadTag with InstanceUUID with RequestID],
      metrics: Metrics,
  ): Future[Error \/ domain.ResolvedContractRef[ApiValue]] =
    contractsService
      .resolveContractReference(
        jwt,
        resolveRefParties(meta, jwtPayload),
        reference,
        toLedgerId(jwtPayload.ledgerId),
      )
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
      t3 <- inputAndJwtPayload[JwtPayloadLedgerIdOnly](req).leftMap(it => it: Error)
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

  private def proxyWithCommandET[A: JsonReader, R](
      fn: (Jwt, A) => ET[R]
  )(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): ET[R] = proxyWithCommand((jwt, a: A) => fn(jwt, a).run)(req)

  private def getUserIdFromToken(jwt: Jwt): ET[UserId] =
    decodeAndParseUserIdFromToken(jwt, decodeJwt).leftMap(identity[Error])

  private def parseUserId(rawUserId: String): ET[UserId] = {
    import scalaz.syntax.std.either._
    either(
      UserId.fromString(rawUserId).disjunction.leftMap(InvalidUserInput)
    )
  }

  private val emptyObjectResponse: domain.SyncResponse[spray.json.JsObject] =
    domain.OkResponse(spray.json.JsObject())
}

object Endpoints {
  import json.JsonProtocol._
  import util.ErrorOps._

  private type ET[A] = EitherT[Future, Error, A]

  private type ApiRecord = lav1.value.Record
  private type ApiValue = lav1.value.Value

  private type LfValue = lf.value.Value

  private final class IntoEndpointsError[-A](val run: A => Error) extends AnyVal
  private object IntoEndpointsError {
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

  @java.lang.FunctionalInterface
  private final case class MkHttpResponse[-T](run: T => Future[HttpResponse])

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

  private val nonHttpsErrorMessage =
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
