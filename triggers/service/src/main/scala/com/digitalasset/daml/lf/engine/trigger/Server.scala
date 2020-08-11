// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.PostStop
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.scaladsl.adapter._
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.model._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.Materializer
import akka.util.ByteString
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
import com.daml.lf.engine.trigger.dao._
import com.daml.lf.engine.trigger.Request.StartParams
import com.daml.lf.engine.trigger.Response._
import com.daml.daml_lf_dev.DamlLf
import com.daml.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.daml.scalautil.Statement.discard

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}
import java.io.ByteArrayInputStream
import java.util.UUID
import java.util.zip.ZipInputStream
import java.time.LocalDateTime

class Server(
    ledgerConfig: LedgerConfig,
    restartConfig: TriggerRestartConfig,
    secretKey: SecretKey,
    triggerDao: RunningTriggerDao)(
    implicit ctx: ActorContext[Message],
    materializer: Materializer,
    esf: ExecutionSequencerFactory) {

  import java.util.{concurrent => jconc}

  private val triggerLog: jconc.ConcurrentMap[UUID, Vector[(LocalDateTime, String)]] =
    new jconc.ConcurrentHashMap

  // We keep the compiled packages in memory as it is required to construct a trigger Runner.
  // When running with a persistent store we also write the encoded packages so we can recover
  // our state after the service shuts down or crashes.
  val compiledPackages: MutableCompiledPackages = ConcurrentCompiledPackages()

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
      case (pkgId, pkg) => complete(compiledPackages.addPackage(pkgId, pkg))
    }
  }

  // Add a dar to compiledPackages (in memory) and to persistent storage if using it.
  // Uploads of packages that already exist are considered harmless and are ignored.
  private def addDar(encodedDar: Dar[(PackageId, DamlLf.ArchivePayload)]): Either[String, Unit] = {
    addPackagesInMemory(encodedDar.all)
    triggerDao.persistPackages(encodedDar)
  }

  private def restartTriggers(triggers: Vector[RunningTrigger]): Either[String, Unit] = {
    import cats.implicits._ // needed for traverse
    triggers.traverse_(t => startTrigger(t.credentials, t.triggerName, Some(t.triggerInstance)))
  }

  private def triggerRunnerName(triggerInstance: UUID): String =
    triggerInstance.toString ++ "-monitor"

  private def getRunner(triggerInstance: UUID): Option[ActorRef[TriggerRunner.Message]] =
    ctx
      .child(triggerRunnerName(triggerInstance))
      .asInstanceOf[Option[ActorRef[TriggerRunner.Message]]]

  private def startTrigger(
      credentials: UserCredentials,
      triggerName: Identifier,
      existingInstance: Option[UUID] = None): Either[String, JsValue] = {
    for {
      trigger <- Trigger.fromIdentifier(compiledPackages, triggerName)
      triggerInstance <- existingInstance match {
        case None =>
          val newInstance = UUID.randomUUID
          triggerDao
            .addRunningTrigger(RunningTrigger(newInstance, triggerName, credentials))
            .map(_ => newInstance)
        case Some(instance) => Right(instance)
      }
      party = TokenManagement.decodeCredentials(secretKey, credentials)._1
      _ = ctx.spawn(
        TriggerRunner(
          new TriggerRunner.Config(
            ctx.self,
            triggerInstance,
            credentials,
            compiledPackages,
            trigger,
            ledgerConfig,
            restartConfig,
            party
          ),
          triggerInstance.toString
        ),
        triggerRunnerName(triggerInstance)
      )
    } yield JsObject(("triggerId", triggerInstance.toString.toJson))
  }

  private def stopTrigger(uuid: UUID): Either[String, Option[JsValue]] = {
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
        logTriggerStatus(uuid, "stopped: by user request")
        Some(JsObject(("triggerId", uuid.toString.toJson)))
    }
  }

  private def listTriggers(credentials: UserCredentials): Either[String, JsValue] = {
    triggerDao.listRunningTriggers(credentials) map { triggerInstances =>
      JsObject(("triggerIds", triggerInstances.map(_.toString).toJson))
    }
  }

  private def logTriggerStatus(triggerInstance: UUID, msg: String): Unit = {
    val entry = (LocalDateTime.now, msg)
    discard(triggerLog.merge(triggerInstance, Vector(entry), _ ++ _))
  }

  private def getTriggerStatus(uuid: UUID): Vector[(LocalDateTime, String)] =
    triggerLog.getOrDefault(uuid, Vector.empty)

  private val route = concat(
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
                    .findCredentials(secretKey, request)
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
                      try {
                        addDar(dar) match {
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
                .findCredentials(secretKey, request)
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
          implicit val dateTimeFormat: RootJsonFormat[LocalDateTime] = LocalDateTimeJsonFormat
          complete(successResponse(JsObject(("logs", getTriggerStatus(uuid).toJson))))
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
                .findCredentials(secretKey, request)
                .fold(
                  message => complete(errorResponse(StatusCodes.Unauthorized, message)),
                  _ =>
                    stopTrigger(uuid) match {
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
}

object Server {

  def apply(
      host: String,
      port: Int,
      ledgerConfig: LedgerConfig,
      restartConfig: TriggerRestartConfig,
      initialDar: Option[Dar[(PackageId, DamlLf.ArchivePayload)]],
      jdbcConfig: Option[JdbcConfig],
      initDb: Boolean,
      noSecretKey: Boolean,
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

    val secretKey: SecretKey =
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

    if (initDb) jdbcConfig match {
      case None =>
        ctx.log.error("No JDBC configuration for database initialization.")
        sys.exit(1)
      case Some(c) =>
        DbTriggerDao(c).initialize match {
          case Left(err) =>
            ctx.log.error(err)
            sys.exit(1)
          case Right(()) =>
            ctx.log.info("Successfully initialized database.")
            sys.exit(0)
        }
    }

    val (_, server): (RunningTriggerDao, Server) = jdbcConfig match {
      case None =>
        val dao = InMemoryTriggerDao()
        val server = new Server(ledgerConfig, restartConfig, secretKey, dao)
        (dao, server)
      case Some(c) =>
        val dao = DbTriggerDao(c)
        val server = new Server(ledgerConfig, restartConfig, secretKey, dao)
        val recovery: Either[String, Unit] = for {
          packages <- dao.readPackages
          _ = server.addPackagesInMemory(packages)
          triggers <- dao.readRunningTriggers
          _ <- server.restartTriggers(triggers)
        } yield ()
        recovery match {
          case Left(err) =>
            ctx.log.error("Failed to recover state from database.\n" + err)
            sys.exit(1)
          case Right(()) =>
            ctx.log.info("Successfully recovered state from database.")
            (dao, server)
        }
    }

    initialDar foreach { dar =>
      server.addDar(dar) match {
        case Left(err) =>
          ctx.log.error("Failed to upload provided DAR.\n" ++ err)
          sys.exit(1)
        case Right(()) =>
      }
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
            server
              .logTriggerStatus(triggerInstance, "stopped: initialization failure")
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
            Behaviors.stopped // Automatically stops all actors.
        }
        .receiveSignal {
          case (_, PostStop) =>
            // TODO SC until this future returns, connections may still be accepted. Consider
            // coordinating this future with the actor in some way, or use addToCoordinatedShutdown
            // (though I have a feeling it will not work out so neatly)
            discard[Future[akka.Done]](binding.unbind())
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
    val serverBinding = Http().bindAndHandle(Route.handlerFlow(server.route), host, port)
    ctx.pipeToSelf(serverBinding) {
      case Success(binding) => Started(binding)
      case Failure(ex) => StartFailed(ex)
    }

    starting(wasStopped = false, req = None)
  }
}
