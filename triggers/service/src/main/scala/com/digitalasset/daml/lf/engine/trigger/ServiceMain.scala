// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import akka.actor.typed.{ActorRef, ActorSystem, Behavior, Scheduler}
import akka.actor.typed.PostStop
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.adapter._
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.model._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.directives._
import akka.http.scaladsl.server.Route
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import akka.util.{ByteString, Timeout}

import scala.util.Try
import spray.json.DefaultJsonProtocol._
import spray.json._
import com.daml.lf.archive.{Dar, DarReader, Decode}
import com.daml.lf.archive.Reader.ParseError
import com.daml.lf.data.Ref.{Identifier, PackageId}
import com.daml.lf.engine.{
  ConcurrentCompiledPackages,
  MutableCompiledPackages,
  Result,
  ResultDone,
  ResultNeedPackage
}
import com.daml.lf.language.Ast._
import com.daml.lf.engine.trigger.Request.StartParams
import com.daml.lf.engine.trigger.Response._
import com.daml.daml_lf_dev.DamlLf
import com.daml.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.daml.platform.services.time.TimeProviderType
import com.daml.jwt.domain.Jwt
import com.daml.ledger.api.refinements.ApiTypes.Party
import scalaz.syntax.traverse._

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import scala.sys.ShutdownHookThread
import java.io.ByteArrayInputStream
import java.time.Duration
import java.util.UUID
import java.util.zip.ZipInputStream
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

case class LedgerConfig(
    host: String,
    port: Int,
    timeProvider: TimeProviderType,
    commandTtl: Duration,
)

final case class RunningTrigger(
    triggerInstance: UUID,
    triggerName: Identifier,
    jwt: Jwt,
    runner: ActorRef[TriggerRunner.Message]
)

class Server(dar: Option[Dar[(PackageId, Package)]], jdbcConfig: Option[JdbcConfig]) {

  private var triggers: Map[UUID, RunningTrigger] = Map.empty;
  private var triggersByToken: Map[Jwt, Set[UUID]] = Map.empty;
  private var triggerLog: Map[UUID, Vector[(String, String)]] = Map.empty;

  val compiledPackages: MutableCompiledPackages = ConcurrentCompiledPackages()
  dar.foreach(addDar(_))

  private def addDar(dar: Dar[(PackageId, Package)]) = {
    val darMap = dar.all.toMap
    darMap.foreach {
      case (pkgId, pkg) =>
        // If packages are not in topological order, we will get back
        // ResultNeedPackage.  The way the code is structured here we
        // will still call addPackage even if we already fed the
        // package via the callback but this is harmless and not
        // expensive.
        def go(r: Result[Unit]): Unit = r match {
          case ResultDone(()) => ()
          case ResultNeedPackage(pkgId, resume) =>
            go(resume(darMap.get(pkgId)))
          case _ => throw new RuntimeException(s"Unexpected engine result $r")
        }
        go(compiledPackages.addPackage(pkgId, pkg))
    }
  }

  private def getRunningTrigger(uuid: UUID): RunningTrigger = {
    triggers.get(uuid).get // TODO: Improve as might throw NoSuchElementException.
  }

  private def addRunningTrigger(t: RunningTrigger): Unit = {
    triggers = triggers + (t.triggerInstance -> t)
    triggersByToken = triggersByToken + (t.jwt -> (triggersByToken.getOrElse(t.jwt, Set()) + t.triggerInstance))
  }

  private def removeRunningTrigger(t: RunningTrigger): Unit = {
    triggers = triggers - t.triggerInstance
    triggersByToken = triggersByToken + (t.jwt -> (triggersByToken
      .get(t.jwt)
      .get - t.triggerInstance))
  }

  private def listRunningTriggers(jwt: Jwt): List[String] = {
    triggersByToken.getOrElse(jwt, Set()).map(_.toString).toList
  }

  private def timeStamp(): String = {
    DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mmss").format(LocalDateTime.now)
  }

  private def logTriggerStatus(t: RunningTrigger, msg: String): Unit = {
    val id = t.triggerInstance
    val entry = (timeStamp(), msg)
    triggerLog += triggerLog
      .get(id)
      .map(logs => id -> (logs :+ entry))
      .getOrElse(id -> Vector(entry))
  }

  private def getTriggerStatus(uuid: UUID): Vector[(String, String)] = {
    triggerLog.getOrElse(uuid, Vector())
  }

}

object Server {

  sealed trait Message

  final case class GetServerBinding(replyTo: ActorRef[ServerBinding]) extends Message
  final case object Stop extends Message

  private final case class StartFailed(cause: Throwable) extends Message
  private final case class Started(binding: ServerBinding) extends Message

  final case class TriggerStarting(runningTrigger: RunningTrigger) extends Message

  final case class TriggerStarted(runningTrigger: RunningTrigger) extends Message

  final case class TriggerInitializationFailure(
      runningTrigger: RunningTrigger,
      cause: String
  ) extends Message

  final case class TriggerRuntimeFailure(
      runningTrigger: RunningTrigger,
      cause: String
  ) extends Message

  def apply(
      host: String,
      port: Int,
      ledgerConfig: LedgerConfig,
      maxInboundMessageSize: Int,
      maxFailureNumberOfRetries: Int,
      failureRetryTimeRange: Duration,
      dar: Option[Dar[(PackageId, Package)]],
      jdbcConfig: Option[JdbcConfig],
  ): Behavior[Message] = Behaviors.setup { ctx =>
    val server = new Server(dar, jdbcConfig)

    // http doesn't know about akka typed so provide untyped system
    implicit val untypedSystem: akka.actor.ActorSystem = ctx.system.toClassic
    implicit val materializer: Materializer = Materializer(untypedSystem)
    implicit val esf: ExecutionSequencerFactory =
      new AkkaExecutionSequencerPool("TriggerService")(untypedSystem)

    def startTrigger(
        ctx: ActorContext[Server.Message],
        token: (Jwt, JwtPayload),
        trigger: Trigger,
        triggerName: Identifier,
        ledgerConfig: LedgerConfig,
        maxInboundMessageSize: Int,
        maxFailureNumberOfRetries: Int,
        failureRetryTimeRange: Duration,
    ): JsValue = {
      val jwt: Jwt = token._1
      val jwtPayload: JwtPayload = token._2
      val party: Party = Party(jwtPayload.party);
      val triggerInstance = UUID.randomUUID
      val ref = ctx.spawn(
        TriggerRunner(
          new TriggerRunner.Config(
            ctx.self,
            triggerInstance,
            triggerName,
            jwt,
            server.compiledPackages,
            trigger,
            ledgerConfig,
            maxInboundMessageSize,
            maxFailureNumberOfRetries,
            failureRetryTimeRange,
            party
          ),
          triggerInstance.toString
        ),
        triggerInstance.toString + "-monitor"
      )
      JsObject(("triggerId", triggerInstance.toString.toJson))
    }

    def stopTrigger(uuid: UUID, token: (Jwt, JwtPayload)): JsValue = {
      //TODO(SF, 2020-05-20): At least check that the provided token
      //is the same as the one used to start the trigger and fail with
      //'Unauthorized' if not (expect we'll be able to do better than
      //this).
      val runningTrigger = server.getRunningTrigger(uuid)
      runningTrigger.runner ! TriggerRunner.Stop
      server.logTriggerStatus(runningTrigger, "stopped: by user request")
      server.removeRunningTrigger(runningTrigger)
      JsObject(("triggerId", uuid.toString.toJson))
    }

    def listTriggers(token: (Jwt, JwtPayload)): JsValue = {
      JsObject(("triggerIds", server.listRunningTriggers(token._1).toJson))
    }

    // 'fileUpload' triggers this warning we don't have a fix right
    // now so we disable it.
    val route = concat(
      post {
        concat(
          // Start a new trigger given its identifier and the party it
          // should be running as.  Returns a UUID for the newly
          // started trigger.
          path("v1" / "start") {
            extractRequest {
              request =>
                entity(as[StartParams]) {
                  params =>
                    TokenManagement
                      .findJwt(request)
                      .flatMap { jwt =>
                        TokenManagement.decodeAndParsePayload(jwt, TokenManagement.decodeJwt)
                      }
                      .fold(
                        unauthorized =>
                          complete(
                            errorResponse(StatusCodes.UnprocessableEntity, unauthorized.message)),
                        token =>
                          Trigger
                            .fromIdentifier(server.compiledPackages, params.triggerName) match {
                            case Left(err) =>
                              complete(errorResponse(StatusCodes.UnprocessableEntity, err))
                            case Right(trigger) => {
                              val triggerInstance = startTrigger(
                                ctx,
                                token,
                                trigger,
                                params.triggerName,
                                ledgerConfig,
                                maxInboundMessageSize,
                                maxFailureNumberOfRetries,
                                failureRetryTimeRange
                              )
                              complete(successResponse(triggerInstance))
                            }
                        }
                      )
                }
            }
          },
          // upload a DAR as a multi-part form request with a single field called
          // "dar".
          path("v1" / "upload_dar") {
            fileUpload("dar") {
              case (metadata: FileInfo, byteSource: Source[ByteString, Any]) =>
                val byteStringF: Future[ByteString] = byteSource.runFold(ByteString(""))(_ ++ _)
                onSuccess(byteStringF) {
                  byteString =>
                    val inputStream = new ByteArrayInputStream(byteString.toArray)
                    DarReader()
                      .readArchive("package-upload", new ZipInputStream(inputStream)) match {
                      case Failure(err) =>
                        complete(errorResponse(StatusCodes.UnprocessableEntity, err.toString))
                      case Success(encodedDar) =>
                        try {
                          val dar = encodedDar.map {
                            case (pkgId, payload) => Decode.readArchivePayload(pkgId, payload)
                          }
                          server.addDar(dar)
                          val mainPackageId = JsObject(("mainPackageId", dar.main._1.name.toJson))
                          complete(successResponse(mainPackageId))
                        } catch {
                          case err: ParseError =>
                            complete(errorResponse(StatusCodes.UnprocessableEntity, err.toString))
                        }
                    }
                }
            }
          }
        )
      },
      get {
        // Convenience endpoint for tests (roughly follow
        // https://tools.ietf.org/id/draft-inadarei-api-health-check-01.html).
        concat(
          path("v1" / "health") {
            complete((StatusCodes.OK, JsObject(("status", "pass".toJson))))
          },
          // List triggers currently running for the given party.
          path("v1" / "list") {
            extractRequest {
              request =>
                TokenManagement
                  .findJwt(request)
                  .flatMap { jwt =>
                    TokenManagement.decodeAndParsePayload(jwt, TokenManagement.decodeJwt)
                  }
                  .map { token =>
                    listTriggers(token)
                  }
                  .fold(
                    unauthorized =>
                      complete(
                        errorResponse(StatusCodes.UnprocessableEntity, unauthorized.message)),
                    triggerInstances => complete(successResponse(triggerInstances))
                  )
            }
          },
          // Produce logs for the given trigger.
          pathPrefix("v1" / "status" / JavaUUID) { uuid =>
            complete(successResponse(JsObject(("logs", server.getTriggerStatus(uuid).toJson))))
          }
        )
      },
      // Stop a trigger given its UUID
      delete {
        pathPrefix("v1" / "stop" / JavaUUID) {
          uuid =>
            extractRequest {
              request =>
                TokenManagement
                  .findJwt(request)
                  .flatMap { jwt =>
                    TokenManagement.decodeAndParsePayload(jwt, TokenManagement.decodeJwt)
                  }
                  .map { token =>
                    stopTrigger(uuid, token)
                  }
                  .fold(
                    unauthorized =>
                      complete(
                        errorResponse(StatusCodes.UnprocessableEntity, unauthorized.message)),
                    triggerInstance => complete(successResponse(triggerInstance))
                  )
            }
        }
      },
    )

    // The server binding is a future that on completion will be piped
    // to a message to this actor.
    val serverBinding = Http().bindAndHandle(Route.handlerFlow(route), host, port)
    ctx.pipeToSelf(serverBinding) {
      case Success(binding) => Started(binding)
      case Failure(ex) => StartFailed(ex)
    }

    // The server running state.
    def running(binding: ServerBinding): Behavior[Message] =
      Behaviors
        .receiveMessage[Message] {
          case TriggerStarting(runningTrigger) =>
            server.logTriggerStatus(runningTrigger, "starting")
            Behaviors.same
          case TriggerStarted(runningTrigger) =>
            // The trigger has successfully started. Update the
            // running triggers tables.
            server.logTriggerStatus(runningTrigger, "running")
            server.addRunningTrigger(runningTrigger)
            Behaviors.same
          case TriggerInitializationFailure(runningTrigger, cause) =>
            // The trigger has failed to start. Send the runner a stop
            // message. There's no point in it remaining alive since
            // its child actor is stopped and won't be restarted.
            server.logTriggerStatus(runningTrigger, "stopped: initialization failure")
            runningTrigger.runner ! TriggerRunner.Stop
            // No need to update the running triggers tables since
            // this trigger never made it there.
            Behaviors.same
          case TriggerRuntimeFailure(runningTrigger, cause) =>
            // The trigger has failed. Remove it from the running
            // triggers tables.
            server.logTriggerStatus(runningTrigger, "stopped: runtime failure")
            server.removeRunningTrigger(runningTrigger)
            // Don't send any messages to the runner. Its supervision
            // strategy will automatically restart the trigger up to
            // some number of times beyond which it will remain
            // stopped.
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
            Behaviors.stopped // Automatically stops all actors.
        }
        .receiveSignal {
          case (_, PostStop) =>
            binding.unbind()
            Behaviors.same
        }

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
        case _ =>
          Behaviors.unhandled
      }

    starting(wasStopped = false, req = None)
  }
}

object ServiceMain {
  // This is mainly intended for tests
  def startServer(
      host: String,
      port: Int,
      ledgerConfig: LedgerConfig,
      maxInboundMessageSize: Int,
      maxFailureNumberOfRetries: Int,
      failureRetryTimeRange: Duration,
      dar: Option[Dar[(PackageId, Package)]],
      jdbcConfig: Option[JdbcConfig]): Future[(ServerBinding, ActorSystem[Server.Message])] = {
    val system: ActorSystem[Server.Message] =
      ActorSystem(
        Server(
          host,
          port,
          ledgerConfig,
          maxInboundMessageSize,
          maxFailureNumberOfRetries,
          failureRetryTimeRange,
          dar,
          jdbcConfig),
        "TriggerService")
    // timeout chosen at random, change freely if you see issues
    implicit val timeout: Timeout = 15.seconds
    implicit val scheduler: Scheduler = system.scheduler
    implicit val ec: ExecutionContext = system.executionContext
    val bindingFuture = system.ask((ref: ActorRef[ServerBinding]) => Server.GetServerBinding(ref))
    bindingFuture.map(server => (server, system))
  }

  def initDatabase(c: JdbcConfig)(implicit ec: ExecutionContext): Either[String, Unit] = {
    val triggerDao = TriggerDao(c)(ec)
    val transaction = triggerDao.transact(TriggerDao.initialize(triggerDao.logHandler))
    Try(transaction.unsafeRunSync()) match {
      case Failure(err) => Left(err.toString)
      case Success(()) => Right(())
    }
  }

  def main(args: Array[String]): Unit = {
    ServiceConfig.parse(args) match {
      case None => sys.exit(1)
      case Some(config) =>
        val dar: Option[Dar[(PackageId, Package)]] = config.darPath.map {
          case darPath =>
            val encodedDar: Dar[(PackageId, DamlLf.ArchivePayload)] =
              DarReader().readArchiveFromFile(darPath.toFile) match {
                case Failure(err) => sys.error(s"Failed to read archive: $err")
                case Success(dar) => dar
              }
            encodedDar.map {
              case (pkgId, pkgArchive) => Decode.readArchivePayload(pkgId, pkgArchive)
            }
        }
        val ledgerConfig =
          LedgerConfig(
            config.ledgerHost,
            config.ledgerPort,
            config.timeProviderType,
            config.commandTtl,
          )
        val system: ActorSystem[Server.Message] =
          ActorSystem(
            Server(
              "localhost",
              config.httpPort,
              ledgerConfig,
              config.maxInboundMessageSize,
              config.maxFailureNumberOfRetries,
              config.failureRetryTimeRange,
              dar,
              config.jdbcConfig
            ),
            "TriggerService"
          )
        // Timeout chosen at random, change freely if you see issues.
        implicit val timeout: Timeout = 15.seconds
        implicit val scheduler: Scheduler = system.scheduler
        implicit val ec: ExecutionContext = system.executionContext

        (config.init, config.jdbcConfig) match {
          case (true, None) =>
            system.log.error("No JDBC configuration for database initialization.")
            sys.exit(1)
          case (true, Some(jdbcConfig)) =>
            initDatabase(jdbcConfig) match {
              case Left(err) =>
                system.log.error(err)
                sys.exit(1)
              case Right(()) =>
                system.log.info("Initialized database.")
                sys.exit(0)
            }
          case _ =>
        }

        // Shutdown gracefully on SIGINT.
        val serviceF: Future[ServerBinding] =
          system.ask((ref: ActorRef[ServerBinding]) => Server.GetServerBinding(ref))
        val _: ShutdownHookThread = sys.addShutdownHook {
          system ! Server.Stop
          serviceF.onComplete {
            case Success(_) =>
              system.log.info("Server is offline, the system will now terminate")
            case Failure(ex) =>
              system.log.info("Failure encountered shutting down the server: " + ex.toString)
          }
          val _: Future[ServerBinding] = Await.ready(serviceF, 5.seconds)
        }
    }
  }
}
