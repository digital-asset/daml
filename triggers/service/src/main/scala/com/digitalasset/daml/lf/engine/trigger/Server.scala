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
import akka.http.scaladsl.model.Uri.Path
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Directive, Directive1, Route}
import akka.http.scaladsl.unmarshalling.{Unmarshal, Unmarshaller}
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
import com.daml.lf.engine.trigger.Tagged.AccessToken
import com.daml.lf.engine.trigger.TriggerRunner._
import com.daml.lf.engine.trigger.dao._
import com.daml.oauth.middleware.Request.Claims
import com.daml.oauth.middleware.{
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
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

class Server(
    authConfig: AuthConfig,
    ledgerConfig: LedgerConfig,
    restartConfig: TriggerRestartConfig,
    triggerDao: RunningTriggerDao,
    val logTriggerStatus: (UUID, String) => Unit)(
    implicit ctx: ActorContext[Server.Message],
    materializer: Materializer,
    esf: ExecutionSequencerFactory)
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

  private def restartTriggers(triggers: Vector[RunningTrigger]): Either[String, Unit] = {
    import cats.implicits._ // needed for traverse
    triggers.traverse_(runningTrigger =>
      for {
        trigger <- Trigger.fromIdentifier(compiledPackages, runningTrigger.triggerName)
      } yield startTrigger(trigger, runningTrigger))
  }

  private def triggerRunnerName(triggerInstance: UUID): String =
    triggerInstance.toString ++ "-monitor"

  private def getRunner(triggerInstance: UUID): Option[ActorRef[TriggerRunner.Message]] =
    ctx
      .child(triggerRunnerName(triggerInstance))
      .asInstanceOf[Option[ActorRef[TriggerRunner.Message]]]

  // Add a new trigger to the database and return the resulting RunningTrigger.
  // Note that this does not yet start the trigger.
  private def addNewTrigger(
      party: Party,
      triggerName: Identifier,
      optApplicationId: Option[ApplicationId],
      token: Option[AccessToken]
  )(implicit ec: ExecutionContext): Future[Either[String, (Trigger, RunningTrigger)]] = {
    val newInstance = UUID.randomUUID()
    val applicationId = optApplicationId.getOrElse(Tag(newInstance.toString): ApplicationId)
    val runningTrigger = RunningTrigger(newInstance, triggerName, party, applicationId, token)
    // Validate trigger id before persisting to DB
    Trigger.fromIdentifier(compiledPackages, runningTrigger.triggerName) match {
      case Left(value) => Future.successful(Left(value))
      case Right(trigger) =>
        triggerDao.addRunningTrigger(runningTrigger).map(_ => Right((trigger, runningTrigger)))
    }
  }

  private def startTrigger(trigger: Trigger, runningTrigger: RunningTrigger): JsValue = {
    discard[ActorRef[Message]](
      ctx.spawn(
        TriggerRunner(
          new TriggerRunner.Config(
            ctx.self,
            runningTrigger.triggerInstance,
            runningTrigger.triggerParty,
            runningTrigger.triggerApplicationId,
            AccessToken.unsubst(runningTrigger.triggerToken),
            compiledPackages,
            trigger,
            ledgerConfig,
            restartConfig
          ),
          runningTrigger.triggerInstance.toString
        ),
        triggerRunnerName(runningTrigger.triggerInstance)
      ))
    JsObject(("triggerId", runningTrigger.triggerInstance.toString.toJson))
  }

  private def stopTrigger(uuid: UUID)(implicit ec: ExecutionContext): Future[Option[JsValue]] = {
    triggerDao.removeRunningTrigger(uuid) map {
      case false => None
      case true =>
        getRunner(uuid) foreach { runner =>
          runner ! TriggerRunner.Stop
        }
        // If we couldn't find the runner then there is nothing to stop anyway,
        // so pretend everything went normally.
        logTriggerStatus(uuid, "stopped: by user request")
        Some(JsObject(("triggerId", uuid.toString.toJson)))
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
        getRunner(uuid) match {
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

  // TODO[AH] Make sure this is bounded in size.
  private val authCallbacks: TrieMap[UUID, Route] = TrieMap()

  // This directive requires authorization for the given claims via the auth middleware, if configured.
  // If no auth middleware is configured, then the request will proceed without attempting authorization.
  //
  // Authorization follows the steps defined in `triggers/service/authentication.md`.
  // First asking for a token on the `/auth` endpoint and redirecting to `/login` if none was returned.
  // If a login is required then this will store the current continuation in `authCallbacks`
  // to proceed once the login flow completed and authentication succeeded.
  private def authorize(claims: AuthRequest.Claims)(
      implicit ec: ExecutionContext,
      system: ActorSystem): Directive1[Option[AccessToken]] =
    authConfig match {
      case NoAuth => provide(None)
      case AuthMiddleware(authUri) =>
        // Attempt to obtain the access token from the middleware's /auth endpoint.
        // Forwards the current request's cookies.
        // Fails if the response is not OK or Unauthorized.
        def auth: Directive1[Option[AccessToken]] = {
          val uri = authUri
            .withPath(Path./("auth"))
            .withQuery(AuthRequest.Auth(claims).toQuery)
          import AuthJsonProtocol._
          extract(_.request.headers[headers.Cookie]).flatMap { cookies =>
            onSuccess(Http().singleRequest(HttpRequest(uri = uri, headers = cookies))).flatMap {
              case HttpResponse(StatusCodes.OK, _, entity, _) =>
                onSuccess(Unmarshal(entity).to[AuthResponse.Authorize]).map { auth =>
                  Some(AccessToken(auth.accessToken)): Option[AccessToken]
                }
              case HttpResponse(StatusCodes.Unauthorized, _, _, _) =>
                provide(None)
              case resp @ HttpResponse(code, _, _, _) =>
                onSuccess(Unmarshal(resp).to[String]).flatMap { msg =>
                  logger.error(s"Failed to authorize with middleware ($code): $msg")
                  complete(
                    errorResponse(
                      StatusCodes.InternalServerError,
                      "Failed to authorize with middleware"))
                }
            }
          }
        }
        Directive { inner =>
          auth {
            // Authorization successful - pass token to continuation
            case Some(token) => inner(Tuple1(Some(token)))
            // Authorization failed - login and retry on callback request.
            case None => { ctx =>
              val requestId = UUID.randomUUID()
              authCallbacks.update(
                requestId, {
                  auth {
                    case None => {
                      // Authorization failed after login - respond with 401
                      // TODO[AH] Add WWW-Authenticate header
                      complete(errorResponse(StatusCodes.Unauthorized))
                    }
                    case Some(token) =>
                      // Authorization successful after login - use old request context and pass token to continuation.
                      mapRequestContext(_ => ctx) {
                        inner(Tuple1(Some(token)))
                      }
                  }
                }
              )
              // TODO[AH] Make the redirect URI configurable, especially the authority. E.g. when running behind nginx.
              val callbackUri = Uri()
                .withScheme(ctx.request.uri.scheme)
                .withAuthority(ctx.request.uri.authority)
                .withPath(Path./("cb"))
              val uri = authUri
                .withPath(Path./("login"))
                .withQuery(AuthRequest.Login(callbackUri, claims, Some(requestId.toString)).toQuery)
              ctx.redirect(uri, StatusCodes.Found)
            }
          }
        }
    }

  // This directive requires authorization for the party of the given running trigger via the auth middleware, if configured.
  // If no auth middleware is configured, then the request will proceed without attempting authorization.
  // If the trigger does not exist, then the request will also proceed without attempting authorization.
  private def authorizeForTrigger(uuid: UUID, readOnly: Boolean = false)(
      implicit ec: ExecutionContext,
      system: ActorSystem): Directive1[Option[AccessToken]] =
    authConfig match {
      case NoAuth => provide(None)
      case AuthMiddleware(_) =>
        onComplete(triggerDao.getRunningTrigger(uuid)).flatMap {
          case Failure(e) => complete(errorResponse(StatusCodes.InternalServerError, e.description))
          case Success(None) => provide(None)
          case Success(Some(trigger)) =>
            val parties = List(trigger.triggerParty)
            val claims = if (readOnly) Claims(readAs = parties) else Claims(actAs = parties)
            authorize(claims)
        }
    }

  private val authCallback: Route =
    parameters('state.as[UUID]) { requestId =>
      authCallbacks.remove(requestId) match {
        case None => complete(StatusCodes.NotFound)
        case Some(callback) =>
          concat(
            parameters(('error, 'error_description ?)) { (error, errorDescription) =>
              complete(
                errorResponse(
                  StatusCodes.Forbidden,
                  s"Failed to authenticate: $error${errorDescription.fold("")(": " + _)}"))
            },
            callback
          )
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
                  val claims =
                    AuthRequest.Claims(actAs = List(params.party))
                  // TODO[AH] Why do we need to pass ec, system explicitly?
                  authorize(claims)(ec, system) {
                    token =>
                      val instOrErr: Future[Either[String, JsValue]] =
                        addNewTrigger(params.party, params.triggerName, params.applicationId, token)
                          .map(_.map {
                            case (trigger, runningTrigger) => startTrigger(trigger, runningTrigger)
                          })
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
                        complete(errorResponse(StatusCodes.UnprocessableEntity, err.description))
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
    },
    path("livez") {
      complete((StatusCodes.OK, JsObject(("status", "pass".toJson))))
    },
    // Authorization callback endpoint
    path("cb") { get { authCallback } },
  )
}

object Server {

  sealed trait Message

  final case class GetServerBinding(replyTo: ActorRef[ServerBinding]) extends Message

  final case class StartFailed(cause: Throwable) extends Message

  final case class Started(binding: ServerBinding) extends Message

  case object Stop extends Message

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

  def apply(
      host: String,
      port: Int,
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

    val (dao, server, initializeF): (RunningTriggerDao, Server, Future[Unit]) = jdbcConfig match {
      case None =>
        val dao = InMemoryTriggerDao()
        val server = new Server(authConfig, ledgerConfig, restartConfig, dao, logTriggerStatus)
        (dao, server, Future.successful(()))
      case Some(c) =>
        val dao = DbTriggerDao(c)
        val server = new Server(authConfig, ledgerConfig, restartConfig, dao, logTriggerStatus)
        val initialize = for {
          _ <- dao.initialize
          packages <- dao.readPackages
          _ = server.addPackagesInMemory(packages)
          triggers <- dao.readRunningTriggers
          _ = server.restartTriggers(triggers)
        } yield ()
        (dao, server, initialize)
    }

    def logTriggerStarting(m: TriggerStarting): Unit =
      server.logTriggerStatus(m.triggerInstance, "starting")

    def logTriggerStarted(m: TriggerStarted): Unit =
      server.logTriggerStatus(m.triggerInstance, "running")

    // The server running state.
    def running(binding: ServerBinding): Behavior[Message] =
      Behaviors
        .receiveMessage[Message] {
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

        case _: TriggerInitializationFailure | _: TriggerRuntimeFailure =>
          Behaviors.unhandled
      }

    // The server binding is a future that on completion will be piped
    // to a message to this actor.
    val serverBinding = for {
      _ <- initializeF
      _ <- Future.traverse(initialDars)(server.addDar(_))
      binding <- Http().bindAndHandle(Route.handlerFlow(server.route), host, port)
    } yield binding
    ctx.pipeToSelf(serverBinding) {
      case Success(binding) => Started(binding)
      case Failure(ex) => StartFailed(ex)
    }

    starting(wasStopped = false, req = None)
  }
}
