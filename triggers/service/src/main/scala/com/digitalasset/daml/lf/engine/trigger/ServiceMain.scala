// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import akka.actor.typed.{ActorRef, ActorSystem, Behavior, Scheduler}
import akka.actor.typed.PostStop
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.scaladsl.Behaviors
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
import com.daml.lf.engine.trigger.Request.StartParams
import com.daml.lf.engine.trigger.Response._
import com.daml.daml_lf_dev.DamlLf
import com.daml.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.daml.platform.services.time.TimeProviderType
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

import com.daml.lf.engine.trigger.dao._

case class LedgerConfig(
    host: String,
    port: Int,
    timeProvider: TimeProviderType,
    commandTtl: Duration,
)

final case class SecretKey(value: String)
final case class UserCredentials(token: EncryptedToken)

final case class RunningTrigger(
    triggerInstance: UUID,
    triggerName: Identifier,
    credentials: UserCredentials,
    // TODO(SF, 2020-0610): Add access token field here in the
    // presence of authentication.
    runner: ActorRef[TriggerRunner.Message]
)

class Server(triggerDao: RunningTriggerDao) {

  private var triggerLog: Map[UUID, Vector[(LocalDateTime, String)]] = Map.empty

  // We keep the compiled packages in memory as it is required to construct a trigger Runner.
  // When running with a persistent store we also write packages to it so we can recover our state
  // after the service shuts down or crashes.
  val compiledPackages: MutableCompiledPackages = ConcurrentCompiledPackages()

  private def addPackagesInMemory(encodedDar: Dar[(PackageId, DamlLf.ArchivePayload)]): Unit = {
    // Decode the dar for the in-memory store.
    val dar = encodedDar.map((Decode.readArchivePayload _).tupled)
    val darMap = dar.all.toMap

    // `addPackage` returns a ResultNeedPackage if a dependency is not yet uploaded.
    // So we need to use the entire `darMap` to complete each call to `addPackage`.
    // This will result in repeated calls to `addPackage` for the same package, but
    // this is harmless and not expensive.
    @scala.annotation.tailrec
    def complete(r: Result[Unit]): Unit = r match {
      case ResultDone(()) => ()
      case ResultNeedPackage(dep, resume) =>
        complete(resume(darMap.get(dep)))
      case _ =>
        throw new RuntimeException(s"Unexpected engine result $r from attempt to add package.")
    }

    darMap foreach {
      case (pkgId, pkg) => complete(compiledPackages.addPackage(pkgId, pkg))
    }
  }

  // Add a dar to compiledPackages (in memory) and to persistent storage if using it.
  // Uploads of packages that already exist are considered harmless and are ignored.
  private def addDar(encodedDar: Dar[(PackageId, DamlLf.ArchivePayload)]): Either[String, Unit] = {
    addPackagesInMemory(encodedDar)
    triggerDao.persistPackages(encodedDar)
  }

  private def logTriggerStatus(triggerInstance: UUID, msg: String): Unit = {
    val entry = (LocalDateTime.now, msg)
    triggerLog += triggerInstance -> (getTriggerStatus(triggerInstance) :+ entry)
  }

  private def getTriggerStatus(uuid: UUID): Vector[(LocalDateTime, String)] = {
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
      initialDar: Option[Dar[(PackageId, DamlLf.ArchivePayload)]],
      jdbcConfig: Option[JdbcConfig],
      noSecretKey: Boolean,
  ): Behavior[Message] = Behaviors.setup { ctx =>
    val triggerDao: RunningTriggerDao =
      jdbcConfig match {
        case None => InMemoryTriggerDao()
        case Some(c) => DbTriggerDao(c)(ctx.system.executionContext)
      }

    val key: SecretKey =
      sys.env.get("TRIGGER_SERVICE_SECRET_KEY") match {
        case Some(key) => SecretKey(key)
        case None =>
          ctx.log.warn(
            "The environment variable 'TRIGGER_SERVICE_SECRET_KEY' is not defined. It is highly recommended that a non-empty value for this variable be set. If the service startup parameters do not include the '--no-secret-key' option, the service will now terminate.")
          if (noSecretKey) {
            SecretKey("secret key") // Provided for testing.
          } else {
            sys.exit(1)
          }
      }

    val server = new Server(triggerDao)

    initialDar foreach { dar =>
      server.addDar(dar) match {
        case Left(err) =>
          ctx.log.error("Failed to upload provided DAR.\n" ++ err)
          sys.exit(1)
        case Right(()) =>
      }
    }

    // http doesn't know about akka typed so provide untyped system
    implicit val untypedSystem: akka.actor.ActorSystem = ctx.system.toClassic
    implicit val materializer: Materializer = Materializer(untypedSystem)
    implicit val esf: ExecutionSequencerFactory =
      new AkkaExecutionSequencerPool("TriggerService")(untypedSystem)
    implicit val dateTimeFormat: RootJsonFormat[LocalDateTime] = LocalDateTimeJsonFormat

    def triggerRunnerName(triggerInstance: UUID): String = triggerInstance.toString ++ "-monitor"

    def getRunner(triggerInstance: UUID): Option[ActorRef[TriggerRunner.Message]] =
      ctx
        .child(triggerRunnerName(triggerInstance))
        .asInstanceOf[Option[ActorRef[TriggerRunner.Message]]]

    def startTrigger(
        credentials: UserCredentials,
        triggerName: Identifier): Either[String, JsValue] = {
      for {
        trigger <- Trigger.fromIdentifier(server.compiledPackages, triggerName)
        party = TokenManagement.decodeCredentials(key, credentials)._1
        triggerInstance = UUID.randomUUID
        _ = ctx.spawn(
          TriggerRunner(
            new TriggerRunner.Config(
              ctx.self,
              triggerInstance,
              triggerName,
              credentials,
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
          triggerRunnerName(triggerInstance)
        )
      } yield JsObject(("triggerId", triggerInstance.toString.toJson))
    }

    def stopTrigger(uuid: UUID, credentials: UserCredentials): Either[String, Option[JsValue]] = {
      //TODO(SF, 2020-05-20): At least check that the provided token
      //is the same as the one used to start the trigger and fail with
      //'Unauthorized' if not (expect we'll be able to do better than
      //this).
      triggerDao.removeRunningTrigger(uuid) map {
        case false => None
        case true =>
          getRunner(uuid) foreach { runner =>
            runner ! TriggerRunner.Stop
          }
          // If we couldn't find the runner then there is nothing to stop anyway,
          // so pretend everything went normally.
          server.logTriggerStatus(uuid, "stopped: by user request")
          Some(JsObject(("triggerId", uuid.toString.toJson)))
      }
    }

    def listTriggers(credentials: UserCredentials): Either[String, JsValue] = {
      triggerDao.listRunningTriggers(credentials) map { triggerInstances =>
        JsObject(("triggerIds", triggerInstances.map(_.toString).toJson))
      }
    }

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
                      .findCredentials(key, request)
                      .fold(
                        message => complete(errorResponse(StatusCodes.Unauthorized, message)),
                        credentials =>
                          startTrigger(credentials, params.triggerName) match {
                            case Left(err) =>
                              complete(errorResponse(StatusCodes.UnprocessableEntity, err))
                            case Right(triggerInstance) =>
                              complete(successResponse(triggerInstance))
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
                      case Success(dar) =>
                        try {
                          server.addDar(dar) match {
                            case Left(err) =>
                              complete(errorResponse(StatusCodes.InternalServerError, err))
                            case Right(()) =>
                              val mainPackageId =
                                JsObject(("mainPackageId", dar.main._1.name.toJson))
                              complete(successResponse(mainPackageId))
                          }
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
                  .findCredentials(key, request)
                  .fold(
                    message => complete(errorResponse(StatusCodes.Unauthorized, message)),
                    credentials =>
                      listTriggers(credentials) match {
                        case Left(err) =>
                          complete(errorResponse(StatusCodes.InternalServerError, err))
                        case Right(triggerInstances) => complete(successResponse(triggerInstances))
                    }
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
                  .findCredentials(key, request)
                  .fold(
                    message => complete(errorResponse(StatusCodes.Unauthorized, message)),
                    credentials =>
                      stopTrigger(uuid, credentials) match {
                        case Left(err) =>
                          complete(errorResponse(StatusCodes.InternalServerError, err))
                        case Right(None) =>
                          val err = s"No trigger running with id $uuid"
                          complete(errorResponse(StatusCodes.NotFound, err))
                        case Right(Some(stoppedTriggerId)) =>
                          complete(successResponse(stoppedTriggerId))
                    }
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
            server.logTriggerStatus(runningTrigger.triggerInstance, "starting")
            Behaviors.same
          case TriggerStarted(runningTrigger) =>
            // The trigger has successfully started. Update the
            // running triggers tables.
            server.logTriggerStatus(runningTrigger.triggerInstance, "running")
            triggerDao.addRunningTrigger(runningTrigger) match {
              case Left(err) =>
                // The trigger has just advised it's in the running
                // state but updating the running trigger table has
                // failed. This error condition is exogenous to the
                // runner. We therefore need to tell it explicitly to
                // stop.
                server.logTriggerStatus(
                  runningTrigger.triggerInstance,
                  "stopped: initialization failure (db write failure)")
                runningTrigger.runner ! TriggerRunner.Stop
                Behaviors.same
              case Right(()) => Behaviors.same
            }
          case TriggerInitializationFailure(runningTrigger, cause) =>
            // The trigger has failed to start. No need to update the
            // running triggers tables since this trigger never made
            // it there.
            server
              .logTriggerStatus(runningTrigger.triggerInstance, "stopped: initialization failure")
            // Don't send any messages to the runner here (it's under
            // the management of a supervision strategy).
            Behaviors.same
          case TriggerRuntimeFailure(runningTrigger, cause) =>
            // The trigger has failed. Remove it from the running triggers tables.
            server.logTriggerStatus(runningTrigger.triggerInstance, "stopped: runtime failure")
            // Ignore the result of the deletion as we don't have a sensible way
            // to handle a failure here at the moment.
            val _ = triggerDao.removeRunningTrigger(runningTrigger.triggerInstance)
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

  // Timeout for serving binding
  implicit val timeout: Timeout = 30.seconds

  // Used by the test fixture
  def startServer(
      host: String,
      port: Int,
      ledgerConfig: LedgerConfig,
      maxInboundMessageSize: Int,
      maxFailureNumberOfRetries: Int,
      failureRetryTimeRange: Duration,
      encodedDar: Option[Dar[(PackageId, DamlLf.ArchivePayload)]],
      jdbcConfig: Option[JdbcConfig],
      noSecretKey: Boolean,
  ): Future[(ServerBinding, ActorSystem[Server.Message])] = {

    val system: ActorSystem[Server.Message] =
      ActorSystem(
        Server(
          host,
          port,
          ledgerConfig,
          maxInboundMessageSize,
          maxFailureNumberOfRetries,
          failureRetryTimeRange,
          encodedDar,
          jdbcConfig,
          noSecretKey,
        ),
        "TriggerService"
      )

    implicit val scheduler: Scheduler = system.scheduler
    implicit val ec: ExecutionContext = system.executionContext

    val bindingFuture = system.ask((ref: ActorRef[ServerBinding]) => Server.GetServerBinding(ref))
    bindingFuture.map(server => (server, system))
  }

  def main(args: Array[String]): Unit = {
    ServiceConfig.parse(args) match {
      case None => sys.exit(1)
      case Some(config) =>
        val encodedDar: Option[Dar[(PackageId, DamlLf.ArchivePayload)]] =
          config.darPath.map { darPath =>
            DarReader().readArchiveFromFile(darPath.toFile) match {
              case Failure(err) => sys.error(s"Failed to read archive: $err")
              case Success(dar) => dar
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
              encodedDar,
              config.jdbcConfig,
              config.noSecretKey
            ),
            "TriggerService"
          )

        implicit val scheduler: Scheduler = system.scheduler
        implicit val ec: ExecutionContext = system.executionContext

        (config.init, config.jdbcConfig) match {
          case (true, None) =>
            system.log.error("No JDBC configuration for database initialization.")
            sys.exit(1)
          case (true, Some(jdbcConfig)) =>
            DbTriggerDao(jdbcConfig).initialize match {
              case Left(err) =>
                system.log.error(err)
                sys.exit(1)
              case Right(()) =>
                system.log.info("Successfully initialized database.")
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
