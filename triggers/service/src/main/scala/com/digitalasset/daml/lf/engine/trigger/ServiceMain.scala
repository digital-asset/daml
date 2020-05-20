// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import akka.actor.typed.{ActorRef, ActorSystem, Behavior, PostStop, Scheduler}
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
import akka.stream.{Materializer}
import akka.stream.scaladsl.{Source}
import akka.util.{ByteString, Timeout}

import spray.json.DefaultJsonProtocol._
import spray.json._

import com.daml.lf.archive.{Dar, DarReader, Decode}
import com.daml.lf.archive.Reader.ParseError
import com.daml.lf.data.Ref.PackageId
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

import scala.concurrent.{ExecutionContext, Future, Await}
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import scala.sys.ShutdownHookThread

import java.io.ByteArrayInputStream
import java.time.Duration
import java.util.UUID
import java.util.zip.ZipInputStream

case class LedgerConfig(
    host: String,
    port: Int,
    timeProvider: TimeProviderType,
    commandTtl: Duration,
)

object Server {

  sealed trait Message
  private final case class StartFailed(cause: Throwable) extends Message
  private final case class Started(binding: ServerBinding) extends Message
  final case class GetServerBinding(replyTo: ActorRef[ServerBinding]) extends Message
  final case object Stop extends Message

  private def addDar(compiledPackages: MutableCompiledPackages, dar: Dar[(PackageId, Package)]) = {
    val darMap = dar.all.toMap
    darMap.foreach {
      case (pkgId, pkg) =>
        // If packages are not in topological order, we will get back ResultNeedPackage.
        // The way the code is structured here we will still call addPackage even if we
        // already fed the package via the callback but this is harmless and not expensive.
        def go(r: Result[Unit]): Unit = r match {
          case ResultDone(()) => ()
          case ResultNeedPackage(pkgId, resume) =>
            go(resume(darMap.get(pkgId)))
          case _ => throw new RuntimeException(s"Unexpected engine result $r")
        }
        go(compiledPackages.addPackage(pkgId, pkg))
    }
  }

  case class TriggerRunnerWithToken(
      ref: ActorRef[TriggerRunner.Message],
      token: Jwt,
  )

  private var triggers: Map[UUID, TriggerRunnerWithToken] = Map.empty
  private var triggersByToken: Map[Jwt, Set[UUID]] = Map.empty
  private val compiledPackages: MutableCompiledPackages = ConcurrentCompiledPackages()

  private def startTrigger(
      ctx: ActorContext[Server.Message],
      token: (Jwt, JwtPayload),
      trigger: Trigger,
      params: StartParams,
      ledgerConfig: LedgerConfig,
      maxInboundMessageSize: Int)(
      implicit esf: ExecutionSequencerFactory,
      mat: Materializer): JsValue = {
    val jwt: Jwt = token._1
    val jwtPayload: JwtPayload = token._2
    val party: Party = Party(jwtPayload.party);
    val uuid = UUID.randomUUID
    val ident = uuid.toString
    val ref = ctx.spawn(
      TriggerRunner(
        new TriggerRunner.Config(
          compiledPackages,
          trigger,
          ledgerConfig,
          maxInboundMessageSize,
          party),
        ident),
      ident + "-monitor")
    triggers = triggers + (uuid -> TriggerRunnerWithToken(ref, jwt))
    val newTriggerSet = triggersByToken.getOrElse(jwt, Set()) + uuid
    triggersByToken = triggersByToken + (jwt -> newTriggerSet)
    val triggerIdResult = JsObject(("triggerId", uuid.toString.toJson))
    triggerIdResult
  }

  private def stopTrigger(uuid: UUID, token: (Jwt, JwtPayload))(
      implicit esf: ExecutionSequencerFactory,
      mat: Materializer): JsValue = {
    //TODO(SF, 2020-05-20): Check that the provided token
    //is the same as the one used to start the trigger and
    //fail with 'Unauthorized' if not.
    val actorWithToken = triggers.get(uuid).get
    actorWithToken.ref ! TriggerRunner.Stop
    triggers = triggers - uuid
    val token = actorWithToken.token
    val newTriggerSet = triggersByToken.get(token).get - uuid
    triggersByToken = triggersByToken + (token -> newTriggerSet)
    val stoppedTriggerId = JsObject(("triggerId", uuid.toString.toJson))
    stoppedTriggerId
  }

  private def listTriggers(token: (Jwt, JwtPayload))(
      implicit esf: ExecutionSequencerFactory,
      mat: Materializer): JsValue = {
    val jwt: Jwt = token._1
    val triggerList = triggersByToken.getOrElse(jwt, Set()).map(_.toString).toList
    JsObject(("triggerIds", triggerList.toJson))
  }

  def apply(
      host: String,
      port: Int,
      ledgerConfig: LedgerConfig,
      maxInboundMessageSize: Int,
      dar: Option[Dar[(PackageId, Package)]],
  ): Behavior[Message] = Behaviors.setup { ctx =>
    // http doesn't know about akka typed so provide untyped system
    implicit val untypedSystem: akka.actor.ActorSystem = ctx.system.toClassic
    implicit val materializer: Materializer = Materializer(untypedSystem)
    implicit val esf: ExecutionSequencerFactory =
      new AkkaExecutionSequencerPool("TriggerService")(untypedSystem)

    dar.foreach(addDar(compiledPackages, _))

    // 'fileUpload' triggers this warning we don't have a fix right
    // now so we disable it.
    @SuppressWarnings(Array("org.wartremover.warts.Any"))
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
                          Trigger.fromIdentifier(compiledPackages, params.identifier) match {
                            case Left(err) =>
                              complete(errorResponse(StatusCodes.UnprocessableEntity, err))
                            case Right(trigger) => {
                              val triggerId = startTrigger(
                                ctx,
                                token,
                                trigger,
                                params,
                                ledgerConfig,
                                maxInboundMessageSize)
                              complete(successResponse(triggerId))
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
                          addDar(compiledPackages, dar)
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
                    triggerIds => complete(successResponse(triggerIds))
                  )
            }
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
                    triggerId => complete(successResponse(triggerId))
                  )
            }
        }
      },
    )

    val serverBinding = Http().bindAndHandle(Route.handlerFlow(route), host, port)
    ctx.pipeToSelf(serverBinding) {
      case Success(binding) => Started(binding)
      case Failure(ex) => StartFailed(ex)
    }

    def running(binding: ServerBinding): Behavior[Message] =
      Behaviors
        .receiveMessage[Message] {
          case StartFailed(_) => Behaviors.unhandled
          case Started(_) => Behaviors.unhandled
          case GetServerBinding(replyTo) =>
            replyTo ! binding
            Behaviors.same
          case Stop =>
            ctx.log.info(
              "Stopping server http://{}:{}/",
              binding.localAddress.getHostString,
              binding.localAddress.getPort,
            )
            Behaviors.stopped
        }
        .receiveSignal {
          case (_, PostStop) =>
            binding.unbind()
            Behaviors.same
        }

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
        case GetServerBinding(replyTo) => starting(wasStopped, Some(replyTo))
        case Stop =>
          // we got a stop message but haven't completed starting yet,
          // we cannot stop until starting has completed
          starting(wasStopped = true, req = None)
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
      dar: Option[Dar[(PackageId, Package)]])
    : Future[(ServerBinding, ActorSystem[Server.Message])] = {
    val system: ActorSystem[Server.Message] =
      ActorSystem(Server(host, port, ledgerConfig, maxInboundMessageSize, dar), "TriggerService")
    // timeout chosen at random, change freely if you see issues
    implicit val timeout: Timeout = 15.seconds
    implicit val scheduler: Scheduler = system.scheduler
    implicit val ec: ExecutionContext = system.executionContext
    val bindingFuture = system.ask((ref: ActorRef[ServerBinding]) => Server.GetServerBinding(ref))
    bindingFuture.map(server => (server, system))
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
            Server("localhost", config.httpPort, ledgerConfig, config.maxInboundMessageSize, dar),
            "TriggerService")
        // Timeout chosen at random, change freely if you see issues.
        implicit val timeout: Timeout = 15.seconds
        implicit val scheduler: Scheduler = system.scheduler
        implicit val ec: ExecutionContext = system.executionContext

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
