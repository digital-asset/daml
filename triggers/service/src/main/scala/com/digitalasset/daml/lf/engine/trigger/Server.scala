// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine.trigger

import java.io.ByteArrayInputStream
import java.util.UUID
import java.util.zip.ZipInputStream

import akka.actor.ActorSystem
import akka.actor.typed.scaladsl.adapter._
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior, Scheduler}
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.Uri.Path
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Directive1, ExceptionHandler, Route}
import akka.http.scaladsl.settings.ServerSettings
import akka.http.scaladsl.unmarshalling.Unmarshaller
import akka.pattern.StatusReply
import akka.stream.Materializer
import akka.util.{ByteString, Timeout}

import scala.concurrent.duration._
import com.daml.daml_lf_dev.DamlLf
import com.daml.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.daml.dbutils.JdbcConfig
import com.daml.ledger.api.refinements.ApiTypes.{ApplicationId, Party}
import com.daml.ledger.resources.ResourceContext
import com.daml.lf.archive.{Dar, DarReader, Decode, Reader}
import com.daml.lf.data.Ref.{Identifier, PackageId}
import com.daml.lf.engine._
import com.daml.lf.engine.trigger.Request.StartParams
import com.daml.lf.engine.trigger.Response._
import com.daml.lf.engine.trigger.TriggerRunner._
import com.daml.lf.engine.trigger.dao._
import com.daml.lf.engine.trigger.metrics.TriggerServiceMetrics
import com.daml.auth.middleware.api.Request.Claims
import com.daml.auth.middleware.api.{
  Client => AuthClient,
  Request => AuthRequest,
  Response => AuthResponse,
}
import com.daml.lf.speedy.Compiler
import com.daml.metrics.akkahttp.HttpMetricsInterceptor
import com.daml.metrics.api.reporters.{MetricsReporter, MetricsReporting}
import com.daml.scalautil.Statement.discard
import com.daml.scalautil.ExceptionOps._
import com.typesafe.scalalogging.StrictLogging
import scalaz.Tag
import scalaz.syntax.traverse._
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

class Server(
    authRoutes: Option[Directive1[AuthClient.Routes]],
    triggerDao: RunningTriggerDao,
    compilerConfig: Compiler.Config,
    val logTriggerStatus: (UUID, String) => Unit,
)(implicit ctx: ActorContext[Server.Message])
    extends StrictLogging {

  // We keep the compiled packages in memory as it is required to construct a trigger Runner.
  // When running with a persistent store we also write the encoded packages so we can recover
  // our state after the service shuts down or crashes.
  val compiledPackages: MutableCompiledPackages =
    ConcurrentCompiledPackages(compilerConfig)

  private def addPackagesInMemory(pkgs: List[(PackageId, DamlLf.ArchivePayload)]): Unit = {
    // We store decoded packages in memory
    val pkgMap = pkgs.map { case (pkgId, payload) =>
      Decode.assertDecodeArchivePayload(Reader.readArchivePayload(pkgId, payload).toTry.get)
    }.toMap

    // `addPackage` returns a ResultNeedPackage if a dependency is not yet uploaded.
    // So we need to use the entire `darMap` to complete each call to `addPackage`.
    // This will result in repeated calls to `addPackage` for the same package, but
    // this is harmless and not expensive.
    @scala.annotation.tailrec
    def complete(r: Result[Unit]): Unit = r match {
      case ResultDone(()) => ()
      case ResultNeedPackage(dep, resume) =>
        complete(resume(pkgMap.get(dep)))
      case _ =>
        throw new RuntimeException(s"Unexpected engine result $r from attempt to add package.")
    }

    pkgMap foreach { case (pkgId, pkg) =>
      logger.info(s"uploading package $pkgId")
      complete(compiledPackages.addPackage(pkgId, pkg))
    }
  }

  // Add a dar to compiledPackages (in memory) and to persistent storage if using it.
  // Uploads of packages that already exist are considered harmless and are ignored.
  private def addDar(
      encodedDar: Dar[(PackageId, DamlLf.ArchivePayload)]
  )(implicit ec: ExecutionContext): Future[Unit] = {
    Future { addPackagesInMemory(encodedDar.all) }.flatMap { _ =>
      triggerDao.persistPackages(encodedDar)
    }
  }

  private def restartTriggers(
      triggers: Vector[RunningTrigger]
  )(implicit ec: ExecutionContext, sys: ActorSystem): Future[Either[String, Unit]] = {
    import cats.implicits._
    triggers.toList.traverse(runningTrigger =>
      runningTrigger.withTriggerLogContext(implicit triggerContext =>
        Trigger
          .fromIdentifier(compiledPackages, runningTrigger.triggerName)
          .map(trigger => (trigger, runningTrigger))
      )
    ) match {
      case Left(err) => Future.successful(Left(err))
      case Right(triggers) =>
        Future.traverse(triggers)(x => startTrigger(x._1, x._2)).map(_ => Right(()))
    }
  }

  private def getRunner(
      triggerInstance: UUID
  )(implicit sys: ActorSystem): Future[Option[ActorRef[TriggerRunner.Message]]] = {
    implicit val timeout: Timeout = Timeout(5 seconds)
    implicit val scheduler: Scheduler = schedulerFromActorSystem(sys.toTyped)
    ctx.self.ask(Server.GetRunner(_, triggerInstance))
  }

  // The static config of a trigger, i.e., RunningTrigger but without a token.
  case class TriggerConfig(
      instance: UUID,
      name: Identifier,
      party: Party,
      applicationId: ApplicationId,
      readAs: Set[Party],
  )

  private def newTrigger(
      party: Party,
      triggerName: Identifier,
      optApplicationId: Option[ApplicationId],
      readAs: Set[Party],
  ): TriggerConfig = {
    val newInstance = UUID.randomUUID()
    val applicationId = optApplicationId.getOrElse(Tag(newInstance.toString): ApplicationId)
    TriggerConfig(newInstance, triggerName, party, applicationId, readAs)
  }

  // Add a new trigger to the database and return the resulting Trigger.
  // Note that this does not yet start the trigger.
  private def addNewTrigger(
      config: TriggerConfig,
      auth: Option[AuthResponse.Authorize],
  )(implicit ec: ExecutionContext): Future[Either[String, (Trigger, RunningTrigger)]] = {
    val runningTrigger =
      RunningTrigger(
        config.instance,
        config.name,
        config.party,
        config.applicationId,
        auth.map(_.accessToken),
        auth.flatMap(_.refreshToken),
        config.readAs,
      )
    runningTrigger.withTriggerLogContext(implicit loggingContext =>
      // Validate trigger id before persisting to DB
      Trigger.fromIdentifier(compiledPackages, runningTrigger.triggerName) match {
        case Left(value) => Future.successful(Left(value))
        case Right(trigger) =>
          triggerDao.addRunningTrigger(runningTrigger).map(_ => Right((trigger, runningTrigger)))
      }
    )
  }

  private def startTrigger(trigger: Trigger, runningTrigger: RunningTrigger)(implicit
      ec: ExecutionContext,
      sys: ActorSystem,
  ): Future[JsValue] = {
    implicit val timeout: Timeout = Timeout(5 seconds)
    implicit val scheduler: Scheduler = schedulerFromActorSystem(sys.toTyped)
    ctx.self
      .askWithStatus(x => Server.StartTrigger(trigger, runningTrigger, compiledPackages, x))
      .map { case () =>
        JsObject(("triggerId", runningTrigger.triggerInstance.toString.toJson))
      }
  }

  private def stopTrigger(
      uuid: UUID
  )(implicit ec: ExecutionContext, sys: ActorSystem): Future[Option[JsValue]] = {
    triggerDao.removeRunningTrigger(uuid).flatMap {
      case false => Future.successful(None)
      case true =>
        getRunner(uuid).map { optRunner =>
          optRunner.foreach { runner =>
            runner ! TriggerRunner.Stop
          }
          logTriggerStatus(uuid, "stopped: by user request")
          Some(JsObject(("triggerId", uuid.toString.toJson)))
        }
    }
  }

  // Left for errors, None for not found, Right(Some(_)) for everything else
  private def triggerStatus(
      uuid: UUID
  )(implicit ec: ExecutionContext, sys: ActorSystem): Future[Option[JsValue]] = {
    import Request.IdentifierFormat
    final case class Result(status: TriggerStatus, triggerId: Identifier, party: Party)
    implicit val partyFormat: JsonFormat[Party] = Tag.subst(implicitly[JsonFormat[String]])
    implicit val resultFormat: RootJsonFormat[Result] = jsonFormat3(Result)
    triggerDao.getRunningTrigger(uuid).flatMap {
      case None => Future.successful(None)
      case Some(t) =>
        getRunner(uuid).flatMap {
          case None =>
            Future.successful(Some(Result(Stopped, t.triggerName, t.triggerParty).toJson))
          case Some(act) =>
            implicit val timeout: Timeout = Timeout(5 seconds)
            implicit val scheduler: Scheduler = schedulerFromActorSystem(sys.toTyped)
            act.ask(TriggerRunner.Status).map { status =>
              Some(Result(status, t.triggerName, t.triggerParty).toJson)
            }
        }
    }
  }

  private def listTriggers(party: Party)(implicit ec: ExecutionContext): Future[JsValue] = {
    triggerDao.listRunningTriggers(party) map { triggerInstances =>
      JsObject(("triggerIds", triggerInstances.map(_.toString).toJson))
    }
  }

  // This directive requires authorization for the given claims via the auth middleware, if configured.
  // If no auth middleware is configured, then the request will proceed without attempting authorization.
  private def authorize(claims: AuthRequest.Claims): Directive1[Option[AuthResponse.Authorize]] =
    authRoutes match {
      case None => provide(None)
      case Some(extractRoutes) =>
        handleExceptions(ExceptionHandler { case ex: AuthClient.ClientException =>
          logger.error(ex.getLocalizedMessage)
          complete(
            errorResponse(
              StatusCodes.InternalServerError,
              "Failed to authorize with auth middleware",
            )
          )
        }).tflatMap { _ =>
          extractRoutes.flatMap { routes =>
            routes.authorize(claims).flatMap {
              case AuthClient.Authorized(authorization) => provide(Some(authorization))
              case AuthClient.Unauthorized =>
                // Authorization failed after login - respond with 401
                // TODO[AH] Add WWW-Authenticate header
                complete(errorResponse(StatusCodes.Unauthorized))
              case AuthClient.LoginFailed(AuthResponse.LoginError(error, errorDescription)) =>
                complete(
                  errorResponse(
                    StatusCodes.Forbidden,
                    s"Failed to authenticate: $error${errorDescription.fold("")(": " + _)}",
                  )
                )
            }
          }
        }
    }

  // This directive requires authorization for the party of the given running trigger via the auth middleware, if configured.
  // If no auth middleware is configured, then the request will proceed without attempting authorization.
  // If the trigger does not exist, then the request will also proceed without attempting authorization.
  private def authorizeForTrigger(
      uuid: UUID,
      readOnly: Boolean = false,
  ): Directive1[Option[AuthResponse.Authorize]] =
    authRoutes match {
      case None => provide(None)
      case Some(_) =>
        extractExecutionContext.flatMap { implicit ec =>
          onComplete(triggerDao.getRunningTrigger(uuid)).flatMap {
            case Failure(e) =>
              complete(errorResponse(StatusCodes.InternalServerError, e.description))
            case Success(None) => provide(None)
            case Success(Some(trigger)) =>
              val parties = List(trigger.triggerParty)
              val claims = if (readOnly) Claims(readAs = parties) else Claims(actAs = parties)
              authorize(claims)
          }
        }
    }

  private implicit val unmarshalParty: Unmarshaller[String, Party] = Unmarshaller.strict(Party(_))

  private val route = concat(
    pathPrefix("v1" / "triggers") {
      concat(
        pathEnd {
          concat(
            // Start a new trigger given its identifier and the party it
            // should be running as.  Returns a UUID for the newly
            // started trigger.
            post {
              entity(as[StartParams]) { params =>
                val config =
                  newTrigger(
                    params.party,
                    params.triggerName,
                    params.applicationId,
                    params.readAs.map(_.toSet).getOrElse(Set.empty),
                  )
                val claims =
                  AuthRequest.Claims(
                    actAs = List(params.party),
                    applicationId = Some(config.applicationId),
                    readAs = config.readAs.toList,
                  )
                authorize(claims) { auth =>
                  extractExecutionContext { implicit ec =>
                    extractActorSystem { implicit sys =>
                      val instOrErr: Future[Either[String, JsValue]] =
                        addNewTrigger(config, auth)
                          .flatMap {
                            case Left(value) => Future.successful(Left(value))
                            case Right((trigger, runningTrigger)) =>
                              startTrigger(trigger, runningTrigger).map(Right(_))
                          }
                      onComplete(instOrErr) {
                        case Failure(exception) =>
                          complete(
                            errorResponse(StatusCodes.InternalServerError, exception.description)
                          )
                        case Success(Left(err)) =>
                          complete(errorResponse(StatusCodes.UnprocessableEntity, err))
                        case Success(triggerInstance) =>
                          complete(successResponse(triggerInstance))
                      }
                    }
                  }
                }
              }
            },
            // List triggers currently running for the given party.
            get {
              parameters(Symbol("party").as[Party]) { party =>
                val claims = Claims(readAs = List(party))
                authorize(claims) { _ =>
                  extractExecutionContext { implicit ec =>
                    onComplete(listTriggers(party)) {
                      case Failure(err) =>
                        complete(errorResponse(StatusCodes.InternalServerError, err.description))
                      case Success(triggerInstances) => complete(successResponse(triggerInstances))
                    }
                  }
                }
              }
            },
          )
        },
        path(JavaUUID) { uuid =>
          concat(
            get {
              authorizeForTrigger(uuid, readOnly = true) { _ =>
                extractExecutionContext { implicit ec =>
                  extractActorSystem { implicit sys =>
                    onComplete(triggerStatus(uuid)) {
                      case Failure(err) =>
                        complete(errorResponse(StatusCodes.InternalServerError, err.description))
                      case Success(None) =>
                        complete(
                          errorResponse(StatusCodes.NotFound, s"No trigger found with id $uuid")
                        )
                      case Success(Some(status)) => complete(successResponse(status))
                    }
                  }
                }
              }
            },
            delete {
              authorizeForTrigger(uuid) { _ =>
                extractExecutionContext { implicit ec =>
                  extractActorSystem { implicit sys =>
                    onComplete(stopTrigger(uuid)) {
                      case Failure(err) =>
                        complete(errorResponse(StatusCodes.InternalServerError, err.description))
                      case Success(None) =>
                        val err = s"No trigger running with id $uuid"
                        complete(errorResponse(StatusCodes.NotFound, err))
                      case Success(Some(stoppedTriggerId)) =>
                        complete(successResponse(stoppedTriggerId))
                    }
                  }
                }
              }
            },
          )
        },
      )
    },
    path("v1" / "packages") {
      // upload a DAR as a multi-part form request with a single field called
      // "dar".
      post {
        val claims = Claims(admin = true)
        authorize(claims) { _ =>
          fileUpload("dar") { case (_, byteSource) =>
            extractMaterializer { implicit mat =>
              val byteStringF: Future[ByteString] = byteSource.runFold(ByteString(""))(_ ++ _)
              onSuccess(byteStringF) { byteString =>
                val inputStream = new ByteArrayInputStream(byteString.toArray)
                DarReader
                  .readArchive("package-upload", new ZipInputStream(inputStream)) match {
                  case Left(err) =>
                    complete(errorResponse(StatusCodes.UnprocessableEntity, err.toString))
                  case Right(dar) =>
                    extractExecutionContext { implicit ec =>
                      onComplete(addDar(dar.map(p => p.pkgId -> p.proto))) {
                        case Failure(err: archive.Error) =>
                          complete(errorResponse(StatusCodes.UnprocessableEntity, err.description))
                        case Failure(exception) =>
                          complete(
                            errorResponse(StatusCodes.InternalServerError, exception.description)
                          )
                        case Success(()) =>
                          val mainPackageId =
                            JsObject(("mainPackageId", dar.main.pkgId.name.toJson))
                          complete(successResponse(mainPackageId))
                      }
                    }
                }
              }
            }
          }
        }
      }
    },
    path("readyz") {
      extractActorSystem { implicit sys =>
        implicit val timeout: Timeout = Timeout(5 seconds)
        implicit val scheduler: Scheduler = schedulerFromActorSystem(sys.toTyped)
        onSuccess(ctx.self.ask(ref => Server.GetServerState(ref))) {
          case Server.Running => complete(StatusCodes.OK)
          case Server.Starting => complete(StatusCodes.NotFound)
        }
      }
    },
    path("livez") {
      complete((StatusCodes.OK, JsObject(("status", "pass".toJson))))
    },
    // Authorization callback endpoint
    authRoutes.fold(reject: Route) { case extractRoutes =>
      path("cb") { get { extractRoutes { _.callbackHandler } } }
    },
  )
}

object Server {

  sealed trait Message

  final case class GetServerBinding(replyTo: ActorRef[ServerBinding]) extends Message

  final case class StartFailed(cause: Throwable) extends Message

  final case class Started(binding: ServerBinding) extends Message

  final case class GetServerState(replyTo: ActorRef[ServerState]) extends Message
  sealed trait ServerState extends Message
  case object Running extends ServerState
  case object Starting extends ServerState

  case object Stop extends Message

  final case class StartTrigger(
      trigger: Trigger,
      runningTrigger: RunningTrigger,
      compiledPackages: CompiledPackages,
      replyTo: ActorRef[StatusReply[Unit]],
  ) extends Message

  final case class RestartTrigger(
      trigger: Trigger,
      runningTrigger: RunningTrigger,
      compiledPackages: CompiledPackages,
  ) extends Message

  final case class GetRunner(replyTo: ActorRef[Option[ActorRef[TriggerRunner.Message]]], uuid: UUID)
      extends Message

  final case class TriggerTokenRefreshFailed(triggerInstance: UUID, cause: Throwable)
      extends Message

  // Messages passed to the server from a TriggerRunner

  final case class TriggerTokenExpired(
      triggerInstance: UUID,
      trigger: Trigger,
      compiledPackages: CompiledPackages,
  ) extends Message

  // Messages passed to the server from a TriggerRunnerImpl

  final case class TriggerStarting(triggerInstance: UUID) extends Message

  final case class TriggerStarted(triggerInstance: UUID) extends Message

  final case class TriggerInitializationFailure(
      triggerInstance: UUID,
      cause: String,
  ) extends Message

  final case class TriggerRuntimeFailure(
      triggerInstance: UUID,
      cause: String,
  ) extends Message

  private def triggerRunnerName(triggerInstance: UUID): String =
    triggerInstance.toString ++ "-monitor"

  def apply(
      host: String,
      port: Int,
      maxAuthCallbacks: Int,
      authCallbackTimeout: FiniteDuration,
      maxHttpEntityUploadSize: Long,
      httpEntityUploadTimeout: FiniteDuration,
      authConfig: AuthConfig,
      authRedirectToLogin: AuthClient.RedirectToLogin,
      authCallback: Option[Uri],
      ledgerConfig: LedgerConfig,
      restartConfig: TriggerRestartConfig,
      initialDars: List[Dar[(PackageId, DamlLf.ArchivePayload)]],
      jdbcConfig: Option[JdbcConfig],
      allowExistingSchema: Boolean,
      compilerConfig: speedy.Compiler.Config,
      triggerConfig: TriggerRunnerConfig,
      metricsReporter: Option[MetricsReporter],
      metricsReportingInterval: FiniteDuration,
      logTriggerStatus: (UUID, String) => Unit = (_, _) => (),
      registerGlobalOpenTelemetry: Boolean,
  ): Behavior[Message] = Behaviors.setup { implicit ctx =>
    // Implicit boilerplate.
    // These are required to execute methods in the Server class and are passed
    // implicitly to the Server constructor.
    implicit val ec: ExecutionContext = ctx.system.executionContext
    // Akka HTTP doesn't know about Akka Typed so provide untyped system.
    implicit val untypedSystem: akka.actor.ActorSystem = ctx.system.toClassic
    implicit val materializer: Materializer = Materializer(untypedSystem)
    implicit val esf: ExecutionSequencerFactory =
      new AkkaExecutionSequencerPool("TriggerService")(untypedSystem)

    implicit val rc: ResourceContext = ResourceContext(ec)

    val metricsReporting = new MetricsReporting(
      getClass.getName,
      metricsReporter,
      metricsReportingInterval,
      registerGlobalOpenTelemetry,
    )((_, otelMeter) => TriggerServiceMetrics(otelMeter))
    val metricsResource = metricsReporting.acquire()

    val rateDurationSizeMetrics = metricsResource.asFuture.map { implicit metrics =>
      HttpMetricsInterceptor.rateDurationSizeMetrics(
        metrics.http
      )
    }

    val authClientRoutes = authConfig match {
      case NoAuth => None
      case AuthMiddleware(int, ext) =>
        val client =
          AuthClient(
            AuthClient.Config(
              authMiddlewareInternalUri = int,
              authMiddlewareExternalUri = ext,
              redirectToLogin = authRedirectToLogin,
              maxAuthCallbacks = maxAuthCallbacks,
              authCallbackTimeout = authCallbackTimeout,
              maxHttpEntityUploadSize = maxHttpEntityUploadSize,
              httpEntityUploadTimeout = httpEntityUploadTimeout,
            )
          )
        val routes = client.routesAuto(authCallback.getOrElse(Uri().withPath(Path./("cb"))))
        Some((client, routes))
    }
    val authClient = authClientRoutes.map(_._1)
    val authRoutes = authClientRoutes.map(_._2)

    val (dao, server, initializeF): (RunningTriggerDao, Server, Future[Unit]) = jdbcConfig match {
      case None =>
        val dao = InMemoryTriggerDao()
        val server = new Server(authRoutes, dao, compilerConfig, logTriggerStatus)
        (dao, server, Future.successful(()))
      case Some(c) =>
        val dao = DbTriggerDao(c)
        val server = new Server(authRoutes, dao, compilerConfig, logTriggerStatus)
        val initialize = for {
          _ <- dao.initialize(allowExistingSchema)
          packages <- dao.readPackages
          _ = server.addPackagesInMemory(packages)
          triggers <- dao.readRunningTriggers
          _ <- server.restartTriggers(triggers)
        } yield ()
        (dao, server, initialize)
    }

    def logTriggerStarting(m: TriggerStarting): Unit =
      server.logTriggerStatus(m.triggerInstance, "starting")

    def logTriggerStarted(m: TriggerStarted): Unit =
      server.logTriggerStatus(m.triggerInstance, "running")

    def spawnTrigger(
        trigger: Trigger,
        runningTrigger: RunningTrigger,
        compiledPackages: CompiledPackages,
    ): ActorRef[TriggerRunner.Message] =
      ctx.spawn(
        TriggerRunner(
          new TriggerRunner.Config(
            ctx.self,
            runningTrigger.triggerInstance,
            runningTrigger.triggerParty,
            runningTrigger.triggerApplicationId,
            runningTrigger.triggerAccessToken,
            runningTrigger.triggerRefreshToken,
            compiledPackages,
            trigger,
            triggerConfig,
            ledgerConfig,
            restartConfig,
            runningTrigger.triggerReadAs,
          ),
          runningTrigger.triggerInstance.toString,
        ),
        triggerRunnerName(runningTrigger.triggerInstance),
      )

    def startTrigger(req: StartTrigger): Unit = {
      Try(spawnTrigger(req.trigger, req.runningTrigger, req.compiledPackages)) match {
        case Failure(exception) => req.replyTo ! StatusReply.error(exception)
        case Success(_) => req.replyTo ! StatusReply.success(())
      }
    }

    def restartTrigger(req: RestartTrigger): Unit = {
      // If the trigger is still running we need to shut it down first
      // and wait for it to terminate before we can spawn it again.
      // Otherwise, akka will raise an error due to a non-unique actor name.
      getRunnerRef(req.runningTrigger.triggerInstance) match {
        case Some(runner) =>
          ctx.watchWith(runner, req)
          ctx.stop(runner)
        case None =>
          discard(spawnTrigger(req.trigger, req.runningTrigger, req.compiledPackages))
      }
    }

    def getRunnerRef(triggerInstance: UUID): Option[ActorRef[TriggerRunner.Message]] = {
      ctx
        .child(triggerRunnerName(triggerInstance))
        .asInstanceOf[Option[ActorRef[TriggerRunner.Message]]]
    }

    def getRunner(req: GetRunner): Unit = {
      req.replyTo ! getRunnerRef(req.uuid)
    }

    def refreshAccessToken(triggerInstance: UUID): Future[RunningTrigger] = {
      def getOrFail[T](result: Option[T], ex: => Throwable): Future[T] = result match {
        case Some(value) => Future.successful(value)
        case None => Future.failed(ex)
      }

      for {
        // Lookup running trigger
        runningTrigger <- dao
          .getRunningTrigger(triggerInstance)
          .flatMap(getOrFail(_, new RuntimeException(s"Unknown trigger $triggerInstance")))
        // Request a token refresh
        client <- getOrFail(
          authClient,
          new RuntimeException("Cannot refresh token without authorization service"),
        )
        refreshToken <- getOrFail(
          runningTrigger.triggerRefreshToken,
          new RuntimeException(s"No refresh token for $triggerInstance"),
        )
        authorization <- client.requestRefresh(refreshToken)
        // Update the tokens in the trigger db
        newAccessToken = authorization.accessToken
        newRefreshToken = authorization.refreshToken
        _ <- dao.updateRunningTriggerToken(triggerInstance, newAccessToken, newRefreshToken)
      } yield runningTrigger
        .copy(triggerAccessToken = Some(newAccessToken), triggerRefreshToken = newRefreshToken)
    }

    // The server running state.
    def running(binding: ServerBinding): Behavior[Message] =
      Behaviors
        .receiveMessage[Message] {
          case req: StartTrigger =>
            startTrigger(req)
            Behaviors.same
          case req: RestartTrigger =>
            restartTrigger(req)
            Behaviors.same
          case req: GetRunner =>
            getRunner(req)
            Behaviors.same
          case m: TriggerStarting =>
            logTriggerStarting(m)
            Behaviors.same

          // Running triggers are added to the store optimistically when the user makes a start
          // request so we don't need to add an entry here.
          case m: TriggerStarted =>
            logTriggerStarted(m)
            Behaviors.same

          // Trigger failures are handled by the TriggerRunner actor using a restart strategy with
          // exponential backoff. The trigger is never really "stopped" this way (though it could
          // fail and restart indefinitely) so in particular we don't need to change the store of
          // running triggers. Entries are removed from there only when the user explicitly stops
          // the trigger with a request.
          case TriggerInitializationFailure(triggerInstance, cause @ _) =>
            server.logTriggerStatus(triggerInstance, "stopped: initialization failure")
            // Don't send any messages to the runner here (it's under
            // the management of a supervision strategy).
            Behaviors.same
          case TriggerRuntimeFailure(triggerInstance, cause @ _) =>
            server.logTriggerStatus(triggerInstance, "stopped: runtime failure")
            // Don't send any messages to the runner here (it's under
            // the management of a supervision strategy).
            Behaviors.same

          case TriggerTokenExpired(triggerInstance, trigger, compiledPackages) =>
            ctx.log.info(s"Updating token for $triggerInstance")
            ctx.pipeToSelf(refreshAccessToken(triggerInstance)) {
              case Success(runningTrigger) =>
                RestartTrigger(trigger, runningTrigger, compiledPackages)
              case Failure(cause) => TriggerTokenRefreshFailed(triggerInstance, cause)
            }
            Behaviors.same
          case TriggerTokenRefreshFailed(triggerInstance, cause) =>
            server.logTriggerStatus(triggerInstance, s"stopped: failed to refresh token: $cause")
            Behaviors.same

          case GetServerBinding(replyTo) =>
            replyTo ! binding
            Behaviors.same
          case GetServerState(replyTo) =>
            replyTo ! Running
            Behaviors.same
          case Running | Starting =>
            Behaviors.same
          case StartFailed(_) => Behaviors.unhandled // Will never be received in this state.
          case Started(_) => Behaviors.unhandled // Will never be received in this state.
          case Stop =>
            ctx.log.info(
              "Stopping server http://{}:{}/",
              binding.localAddress.getHostString,
              binding.localAddress.getPort,
            )
            // TODO SC until this future returns, connections may still be accepted. Consider
            // coordinating this future with the actor in some way, or use addToCoordinatedShutdown
            // (though I have a feeling it will not work out so neatly)
            discard[Future[akka.Done]](binding.unbind())
            discard[Try[Unit]](Try(dao.close()))
            Behaviors.stopped // Automatically stops all actors.
        } // receiveSignal PostStop does not work, see #7092 20c1f241d5

    // The server starting state.
    def starting(
        wasStopped: Boolean,
        req: Option[ActorRef[ServerBinding]],
    ): Behaviors.Receive[Message] =
      Behaviors.receiveMessage[Message] {
        case StartFailed(cause) =>
          if (wasStopped) {
            Behaviors.stopped
          } else {
            throw new RuntimeException("Server failed to start", cause)
          }
        case Started(binding) =>
          ctx.log.info(
            "Server online at http://{}:{}/",
            binding.localAddress.getHostString,
            binding.localAddress.getPort,
          )
          req.foreach(ref => ref ! binding)
          if (wasStopped) ctx.self ! Stop
          running(binding)
        case GetServerBinding(replyTo) =>
          starting(wasStopped, Some(replyTo))
        case GetServerState(replyTo) =>
          replyTo ! Starting
          Behaviors.same
        case Running | Starting =>
          Behaviors.same
        case Stop =>
          // We got a stop message but haven't completed starting
          // yet. We cannot stop until starting has completed.
          starting(wasStopped = true, req = None)

        case m: TriggerStarting =>
          logTriggerStarting(m)
          Behaviors.same

        case m: TriggerStarted =>
          logTriggerStarted(m)
          Behaviors.same

        case req: StartTrigger =>
          startTrigger(req)
          Behaviors.same
        case req: RestartTrigger =>
          restartTrigger(req)
          Behaviors.same
        case req: GetRunner =>
          getRunner(req)
          Behaviors.same

        case _: TriggerInitializationFailure | _: TriggerRuntimeFailure =>
          Behaviors.unhandled

        case _: TriggerTokenExpired | _: TriggerTokenRefreshFailed =>
          Behaviors.unhandled
      }

    // The server binding is a future that on completion will be piped
    // to a message to this actor.
    val serverBinding = for {
      _ <- initializeF
      _ <- Future.traverse(initialDars)(server.addDar(_))
      metricsInterceptor <- rateDurationSizeMetrics
      binding <- Http()
        .newServerAt(host, port)
        .withSettings(ServerSettings(untypedSystem).withTransparentHeadRequests(true))
        .bind(metricsInterceptor apply server.route)
    } yield binding
    ctx.pipeToSelf(serverBinding) {
      case Success(binding) => Started(binding)
      case Failure(ex) => StartFailed(ex)
    }

    starting(wasStopped = false, req = None)
  }
}
