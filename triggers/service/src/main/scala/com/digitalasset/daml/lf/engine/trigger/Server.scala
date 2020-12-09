// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.Uri.Path
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Directive, Directive1, Route}
import akka.http.scaladsl.settings.ServerSettings
import akka.http.scaladsl.unmarshalling.{Unmarshal, Unmarshaller}
import akka.pattern.StatusReply
import akka.stream.Materializer
import akka.util.{ByteString, Timeout}

import scala.concurrent.duration._
import com.daml.daml_lf_dev.DamlLf
import com.daml.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.daml.ledger.api.refinements.ApiTypes.{ApplicationId, Party}
import com.daml.lf.archive.Reader.ParseError
import com.daml.lf.archive.{Dar, DarReader, Decode}
import com.daml.lf.data.Ref.{Identifier, PackageId}
import com.daml.lf.engine._
import com.daml.lf.engine.trigger.Request.StartParams
import com.daml.lf.engine.trigger.Response._
import com.daml.lf.engine.trigger.TriggerRunner._
import com.daml.lf.engine.trigger.dao._
import com.daml.auth.middleware.api.Request.Claims
import com.daml.auth.middleware.api.Tagged.RefreshToken
import com.daml.auth.middleware.api.{
  JsonProtocol => AuthJsonProtocol,
  Request => AuthRequest,
  Response => AuthResponse
}
import com.daml.scalautil.Statement.discard
import com.daml.util.ExceptionOps._
import com.typesafe.scalalogging.StrictLogging
import scalaz.Tag
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.collection.concurrent.TrieMap
import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

case class AuthClientConfig(
    authMiddlewareUri: Uri,
    callbackUri: Uri,
    maxHttpEntityUploadSize: Long,
    httpEntityUploadTimeout: FiniteDuration,
)
object AuthClient {
  sealed trait AuthorizeResult
  final case class Authorized(authorization: AuthResponse.Authorize) extends AuthorizeResult
  object Unauthorized extends AuthorizeResult
  final case class LoginFailed(loginError: AuthResponse.LoginError) extends AuthorizeResult
  def apply(config: AuthClientConfig): AuthClient = new AuthClient(config)
}
class AuthClient(config: AuthClientConfig) extends StrictLogging {
  // TODO[AH] Make sure this is bounded in size.
  private val callbacks: TrieMap[UUID, AuthResponse.Login => Route] = TrieMap()

  /**
    * Handler for the callback in a login flow.
    *
    * Note, a GET request on the `callbackUri` must map to this route.
    */
  val callbackHandler: Route =
    parameters('state.as[UUID]) { requestId =>
      callbacks.remove(requestId) match {
        case None => complete(StatusCodes.NotFound)
        case Some(callback) =>
          AuthResponse.Login.callbackParameters { callback }
      }
    }

  /**
    * This directive requires authorization for the given claims via the auth middleware.
    *
    * Authorization follows the steps defined in `triggers/service/authentication.md`.
    * First asking for a token on the `/auth` endpoint and redirecting to `/login` if none was returned.
    * If a login is required then this will store the current continuation
    * to proceed once the login flow completed and authentication succeeded.
    *
    * A route for the [[callbackHandler]] must be configured.
    */
  def authorize(claims: AuthRequest.Claims)(
      implicit ec: ExecutionContext,
      system: ActorSystem): Directive1[AuthClient.AuthorizeResult] = {
    auth(claims)(ec, system).flatMap {
      // Authorization successful - pass token to continuation
      case Some(authorization) => provide(AuthClient.Authorized(authorization))
      // Authorization failed - login and retry on callback request.
      case None =>
        // Ensure that the request is fully uploaded.
        val timeout = config.httpEntityUploadTimeout
        val maxBytes = config.maxHttpEntityUploadSize
        toStrictEntity(timeout, maxBytes).tflatMap { _ =>
          extractRequestContext.flatMap { ctx =>
            Directive { inner =>
              def continue(result: AuthClient.AuthorizeResult) =
                mapRequestContext(_ => ctx) {
                  inner(Tuple1(result))
                }
              val callback: AuthResponse.Login => Route = {
                case AuthResponse.LoginSuccess =>
                  auth(claims)(ec, system) {
                    case None => continue(AuthClient.Unauthorized)
                    case Some(authorization) => continue(AuthClient.Authorized(authorization))
                  }
                case loginError: AuthResponse.LoginError =>
                  continue(AuthClient.LoginFailed(loginError))
              }
              login(claims, callback)
            }
          }
        }
    }
  }

  /**
    * This directive attempts to obtain an access token from the middleware's auth endpoint for the given claims.
    *
    * Forwards the current request's cookies. Completes with 500 on an unexpected response from the auth middleware.
    *
    * @return `None` if the request was denied otherwise `Some` access and optionally refresh token.
    */
  def auth(claims: AuthRequest.Claims)(
      implicit ec: ExecutionContext,
      system: ActorSystem): Directive1[Option[AuthResponse.Authorize]] =
    extract(_.request.headers[headers.Cookie]).flatMap { cookies =>
      onComplete(requestAuth(claims, cookies)).flatMap {
        case Success(result) => provide(result)
        case Failure(ex) =>
          logger.error(ex.getMessage)
          complete(
            errorResponse(
              StatusCodes.InternalServerError,
              "Failed to authorize with auth middleware"))
      }
    }

  /**
    * Redirect the client to login with the auth middleware.
    *
    * @param callback Will be stored and executed once the login flow completed.
    */
  def login(claims: AuthRequest.Claims, callback: AuthResponse.Login => Route): Route = {
    val requestId = UUID.randomUUID()
    callbacks.update(requestId, callback)
    redirect(loginUri(claims, Some(requestId)), StatusCodes.Found)
  }

  /**
    * Request authentication/authorization on the auth middleware's auth endpoint.
    *
    * @return `None` if the request was denied otherwise `Some` access and optionally refresh token.
    */
  def requestAuth(claims: AuthRequest.Claims, cookies: immutable.Seq[headers.Cookie])(
      implicit ec: ExecutionContext,
      system: ActorSystem): Future[Option[AuthResponse.Authorize]] =
    for {
      response <- Http().singleRequest(
        HttpRequest(
          method = HttpMethods.GET,
          uri = authUri(claims),
          headers = cookies,
        ))
      authorize <- response.status match {
        case StatusCodes.OK =>
          import AuthJsonProtocol._
          Unmarshal(response.entity).to[AuthResponse.Authorize].map(Some(_))
        case StatusCodes.Unauthorized =>
          Future.successful(None)
        case status =>
          Unmarshal(response).to[String].flatMap { msg =>
            Future.failed(
              new RuntimeException(s"Failed to authorize with middleware ($status): $msg"))
          }
      }
    } yield authorize

  /**
    * Request a token refresh on the auth middleware's refresh endpoint.
    */
  def requestRefresh(refreshToken: RefreshToken)(
      implicit ec: ExecutionContext,
      system: ActorSystem): Future[AuthResponse.Authorize] =
    for {
      requestEntity <- {
        import AuthJsonProtocol._
        Marshal(AuthRequest.Refresh(refreshToken))
          .to[RequestEntity]
      }
      response <- Http().singleRequest(
        HttpRequest(
          method = HttpMethods.POST,
          uri = refreshUri,
          entity = requestEntity,
        ))
      authorize <- response.status match {
        case StatusCodes.OK =>
          import AuthJsonProtocol._
          Unmarshal(response.entity).to[AuthResponse.Authorize]
        case status =>
          Unmarshal(response).to[String].flatMap { msg =>
            Future.failed(new RuntimeException(s"Failed to refresh token ($status): $msg"))
          }
      }
    } yield authorize

  private def authUri(claims: AuthRequest.Claims): Uri =
    config.authMiddlewareUri
      .withPath(Path./("auth"))
      .withQuery(AuthRequest.Auth(claims).toQuery)

  private def loginUri(claims: AuthRequest.Claims, requestId: Option[UUID]): Uri =
    config.authMiddlewareUri
      .withPath(Path./("login"))
      .withQuery(AuthRequest.Login(config.callbackUri, claims, requestId.map(_.toString)).toQuery)

  private val refreshUri: Uri = config.authMiddlewareUri
    .withPath(Path./("refresh"))
}

class Server(
    authClient: Option[AuthClient],
    triggerDao: RunningTriggerDao,
    val logTriggerStatus: (UUID, String) => Unit)(
    implicit ctx: ActorContext[Server.Message],
    materializer: Materializer)
    extends StrictLogging {

  // We keep the compiled packages in memory as it is required to construct a trigger Runner.
  // When running with a persistent store we also write the encoded packages so we can recover
  // our state after the service shuts down or crashes.
  val compiledPackages: MutableCompiledPackages =
    ConcurrentCompiledPackages(speedy.Compiler.Config.Dev)

  private def addPackagesInMemory(pkgs: List[(PackageId, DamlLf.ArchivePayload)]): Unit = {
    // We store decoded packages in memory
    val pkgMap = pkgs.map((Decode.readArchivePayload _).tupled).toMap

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

    pkgMap foreach {
      case (pkgId, pkg) =>
        logger.info(s"uploading package $pkgId")
        complete(compiledPackages.addPackage(pkgId, pkg))
    }
  }

  // Add a dar to compiledPackages (in memory) and to persistent storage if using it.
  // Uploads of packages that already exist are considered harmless and are ignored.
  private def addDar(encodedDar: Dar[(PackageId, DamlLf.ArchivePayload)])(
      implicit ec: ExecutionContext): Future[Unit] = {
    Future { addPackagesInMemory(encodedDar.all) }.flatMap { _ =>
      triggerDao.persistPackages(encodedDar)
    }
  }

  private def restartTriggers(triggers: Vector[RunningTrigger])(
      implicit ec: ExecutionContext): Future[Either[String, Unit]] = {
    import cats.implicits._
    triggers.toList.traverse(
      runningTrigger =>
        Trigger
          .fromIdentifier(compiledPackages, runningTrigger.triggerName)
          .map(trigger => (trigger, runningTrigger))) match {
      case Left(err) => Future.successful(Left(err))
      case Right(triggers) =>
        Future.traverse(triggers)(x => startTrigger(x._1, x._2)).map(_ => Right(()))
    }
  }

  private def getRunner(triggerInstance: UUID): Future[Option[ActorRef[TriggerRunner.Message]]] = {
    implicit val timeout: Timeout = Timeout(5 seconds)
    implicit val scheduler: Scheduler = schedulerFromActorSystem(ctx.system)
    ctx.self.ask(Server.GetRunner(_, triggerInstance))
  }

  // The static config of a trigger, i.e., RunningTrigger but without a token.
  case class TriggerConfig(
      instance: UUID,
      name: Identifier,
      party: Party,
      applicationId: ApplicationId
  )

  private def newTrigger(
      party: Party,
      triggerName: Identifier,
      optApplicationId: Option[ApplicationId],
  ): TriggerConfig = {
    val newInstance = UUID.randomUUID()
    val applicationId = optApplicationId.getOrElse(Tag(newInstance.toString): ApplicationId)
    TriggerConfig(newInstance, triggerName, party, applicationId)
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
        auth.flatMap(_.refreshToken))
    // Validate trigger id before persisting to DB
    Trigger.fromIdentifier(compiledPackages, runningTrigger.triggerName) match {
      case Left(value) => Future.successful(Left(value))
      case Right(trigger) =>
        triggerDao.addRunningTrigger(runningTrigger).map(_ => Right((trigger, runningTrigger)))
    }
  }

  private def startTrigger(trigger: Trigger, runningTrigger: RunningTrigger)(
      implicit ec: ExecutionContext): Future[JsValue] = {
    implicit val timeout: Timeout = Timeout(5 seconds)
    implicit val scheduler: Scheduler = schedulerFromActorSystem(ctx.system)
    ctx.self
      .askWithStatus(x => Server.StartTrigger(trigger, runningTrigger, compiledPackages, x))
      .map {
        case () =>
          JsObject(("triggerId", runningTrigger.triggerInstance.toString.toJson))
      }
  }

  private def stopTrigger(uuid: UUID)(implicit ec: ExecutionContext): Future[Option[JsValue]] = {
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
      uuid: UUID)(implicit ec: ExecutionContext, sys: ActorSystem): Future[Option[JsValue]] = {
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
  private def authorize(claims: AuthRequest.Claims)(
      implicit ec: ExecutionContext,
      system: ActorSystem): Directive1[Option[AuthResponse.Authorize]] =
    authClient match {
      case None => provide(None)
      case Some(client) =>
        client.authorize(claims).flatMap {
          case AuthClient.Authorized(authorization) => provide(Some(authorization))
          case AuthClient.Unauthorized =>
            // Authorization failed after login - respond with 401
            // TODO[AH] Add WWW-Authenticate header
            complete(errorResponse(StatusCodes.Unauthorized))
          case AuthClient.LoginFailed(AuthResponse.LoginError(error, errorDescription)) =>
            complete(
              errorResponse(
                StatusCodes.Forbidden,
                s"Failed to authenticate: $error${errorDescription.fold("")(": " + _)}"))
        }
    }

  // This directive requires authorization for the party of the given running trigger via the auth middleware, if configured.
  // If no auth middleware is configured, then the request will proceed without attempting authorization.
  // If the trigger does not exist, then the request will also proceed without attempting authorization.
  private def authorizeForTrigger(uuid: UUID, readOnly: Boolean = false)(
      implicit ec: ExecutionContext,
      system: ActorSystem): Directive1[Option[AuthResponse.Authorize]] =
    authClient match {
      case None => provide(None)
      case Some(_) =>
        onComplete(triggerDao.getRunningTrigger(uuid)).flatMap {
          case Failure(e) => complete(errorResponse(StatusCodes.InternalServerError, e.description))
          case Success(None) => provide(None)
          case Success(Some(trigger)) =>
            val parties = List(trigger.triggerParty)
            val claims = if (readOnly) Claims(readAs = parties) else Claims(actAs = parties)
            authorize(claims)
        }
    }

  private implicit val unmarshalParty: Unmarshaller[String, Party] = Unmarshaller.strict(Party(_))

  private def route(implicit ec: ExecutionContext, system: ActorSystem) = concat(
    pathPrefix("v1" / "triggers") {
      concat(
        pathEnd {
          concat(
            // Start a new trigger given its identifier and the party it
            // should be running as.  Returns a UUID for the newly
            // started trigger.
            post {
              entity(as[StartParams]) {
                params =>
                  val config = newTrigger(params.party, params.triggerName, params.applicationId)
                  val claims =
                    AuthRequest.Claims(
                      actAs = List(params.party),
                      applicationId = Some(config.applicationId))
                  // TODO[AH] Why do we need to pass ec, system explicitly?
                  authorize(claims)(ec, system) {
                    auth =>
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
                            errorResponse(StatusCodes.InternalServerError, exception.description))
                        case Success(Left(err)) =>
                          complete(errorResponse(StatusCodes.UnprocessableEntity, err))
                        case Success(triggerInstance) => complete(successResponse(triggerInstance))
                      }
                  }
              }
            },
            // List triggers currently running for the given party.
            get {
              parameters('party.as[Party]) { party =>
                val claims = Claims(readAs = List(party))
                authorize(claims)(ec, system) { _ =>
                  onComplete(listTriggers(party)) {
                    case Failure(err) =>
                      complete(errorResponse(StatusCodes.InternalServerError, err.description))
                    case Success(triggerInstances) => complete(successResponse(triggerInstances))
                  }
                }
              }
            }
          )
        },
        path(JavaUUID) {
          uuid =>
            concat(
              get {
                authorizeForTrigger(uuid, readOnly = true)(ec, system) { _ =>
                  onComplete(triggerStatus(uuid)) {
                    case Failure(err) =>
                      complete(errorResponse(StatusCodes.InternalServerError, err.description))
                    case Success(None) =>
                      complete(
                        errorResponse(StatusCodes.NotFound, s"No trigger found with id $uuid"))
                    case Success(Some(status)) => complete(successResponse(status))
                  }
                }
              },
              delete {
                authorizeForTrigger(uuid)(ec, system) {
                  _ =>
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
            )
        }
      )
    },
    path("v1" / "packages") {
      // upload a DAR as a multi-part form request with a single field called
      // "dar".
      post {
        val claims = Claims(admin = true)
        authorize(claims)(ec, system) {
          _ =>
            fileUpload("dar") {
              case (_, byteSource) =>
                val byteStringF: Future[ByteString] = byteSource.runFold(ByteString(""))(_ ++ _)
                onSuccess(byteStringF) {
                  byteString =>
                    val inputStream = new ByteArrayInputStream(byteString.toArray)
                    DarReader()
                      .readArchive("package-upload", new ZipInputStream(inputStream)) match {
                      case Failure(err) =>
                        complete(errorResponse(StatusCodes.UnprocessableEntity, err.toString))
                      case Success(dar) =>
                        onComplete(addDar(dar)) {
                          case Failure(err: ParseError) =>
                            complete(
                              errorResponse(StatusCodes.UnprocessableEntity, err.description))
                          case Failure(exception) =>
                            complete(
                              errorResponse(StatusCodes.InternalServerError, exception.description))
                          case Success(()) =>
                            val mainPackageId =
                              JsObject(("mainPackageId", dar.main._1.name.toJson))
                            complete(successResponse(mainPackageId))
                        }
                    }
                }
            }
        }
      }
    },
    path("livez") {
      complete((StatusCodes.OK, JsObject(("status", "pass".toJson))))
    },
    // Authorization callback endpoint
    authClient.fold(reject: Route)(client => path("cb") { get { client.callbackHandler } }),
  )
}

object Server {

  sealed trait Message

  final case class GetServerBinding(replyTo: ActorRef[ServerBinding]) extends Message

  final case class StartFailed(cause: Throwable) extends Message

  final case class Started(binding: ServerBinding) extends Message

  case object Stop extends Message

  final case class StartTrigger(
      trigger: Trigger,
      runningTrigger: RunningTrigger,
      compiledPackages: CompiledPackages,
      replyTo: ActorRef[StatusReply[Unit]])
      extends Message

  final case class RestartTrigger(
      trigger: Trigger,
      runningTrigger: RunningTrigger,
      compiledPackages: CompiledPackages)
      extends Message

  final case class GetRunner(replyTo: ActorRef[Option[ActorRef[TriggerRunner.Message]]], uuid: UUID)
      extends Message

  final case class TriggerTokenRefreshFailed(triggerInstance: UUID, cause: Throwable)
      extends Message

  // Messages passed to the server from a TriggerRunner

  final case class TriggerTokenExpired(
      triggerInstance: UUID,
      trigger: Trigger,
      compiledPackages: CompiledPackages)
      extends Message

  // Messages passed to the server from a TriggerRunnerImpl

  final case class TriggerStarting(triggerInstance: UUID) extends Message

  final case class TriggerStarted(triggerInstance: UUID) extends Message

  final case class TriggerInitializationFailure(
      triggerInstance: UUID,
      cause: String
  ) extends Message

  final case class TriggerRuntimeFailure(
      triggerInstance: UUID,
      cause: String
  ) extends Message

  private def triggerRunnerName(triggerInstance: UUID): String =
    triggerInstance.toString ++ "-monitor"

  def apply(
      host: String,
      port: Int,
      maxHttpEntityUploadSize: Long,
      httpEntityUploadTimeout: FiniteDuration,
      authConfig: AuthConfig,
      ledgerConfig: LedgerConfig,
      restartConfig: TriggerRestartConfig,
      initialDars: List[Dar[(PackageId, DamlLf.ArchivePayload)]],
      jdbcConfig: Option[JdbcConfig],
      logTriggerStatus: (UUID, String) => Unit = (_, _) => (),
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

    val authClient = authConfig match {
      case NoAuth => None
      case AuthMiddleware(uri) =>
        Some(
          AuthClient(AuthClientConfig(
            authMiddlewareUri = uri,
            // TODO[AH] Make the redirect URI configurable, especially the authority. E.g. when running behind nginx.
            callbackUri = Uri().withScheme("http").withAuthority(host, port).withPath(Path./("cb")),
            maxHttpEntityUploadSize = maxHttpEntityUploadSize,
            httpEntityUploadTimeout = httpEntityUploadTimeout,
          )))
    }

    val (dao, server, initializeF): (RunningTriggerDao, Server, Future[Unit]) = jdbcConfig match {
      case None =>
        val dao = InMemoryTriggerDao()
        val server = new Server(authClient, dao, logTriggerStatus)
        (dao, server, Future.successful(()))
      case Some(c) =>
        val dao = DbTriggerDao(c)
        val server = new Server(authClient, dao, logTriggerStatus)
        val initialize = for {
          _ <- dao.initialize
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
        compiledPackages: CompiledPackages): ActorRef[TriggerRunner.Message] =
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
            ledgerConfig,
            restartConfig
          ),
          runningTrigger.triggerInstance.toString
        ),
        triggerRunnerName(runningTrigger.triggerInstance)
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

    def getRunner(req: GetRunner) = {
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
          new RuntimeException("Cannot refresh token without authorization service"))
        refreshToken <- getOrFail(
          runningTrigger.triggerRefreshToken,
          new RuntimeException(s"No refresh token for $triggerInstance"))
        authorization <- client.requestRefresh(refreshToken)
        // Update the tokens in the trigger db
        newAccessToken = authorization.accessToken
        newRefreshToken = authorization.refreshToken
        _ <- dao.updateRunningTriggerToken(triggerInstance, newAccessToken, newRefreshToken)
      } yield
        runningTrigger
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
        req: Option[ActorRef[ServerBinding]]): Behaviors.Receive[Message] =
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
      binding <- Http()
        .newServerAt(host, port)
        .withSettings(ServerSettings(untypedSystem).withTransparentHeadRequests(true))
        .bind(server.route)
    } yield binding
    ctx.pipeToSelf(serverBinding) {
      case Success(binding) => Started(binding)
      case Failure(ex) => StartFailed(ex)
    }

    starting(wasStopped = false, req = None)
  }
}
