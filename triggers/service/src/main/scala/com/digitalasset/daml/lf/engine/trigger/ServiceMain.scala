// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import akka.actor.typed.{ActorRef, ActorSystem, Behavior, PostStop, Scheduler}
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.scaladsl.{Behaviors}
import akka.actor.typed.scaladsl.adapter._
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.directives.FileInfo
import akka.http.scaladsl.server.Route
import akka.stream.{KillSwitch, KillSwitches, Materializer}
import akka.stream.scaladsl.{Source}
import akka.util.{ByteString, Timeout}
import java.io.ByteArrayInputStream
import java.time.Duration
import java.util.UUID
import java.util.zip.ZipInputStream
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.io.StdIn
import scala.util.{Success, Failure}
import scalaz.syntax.tag._
import scalaz.syntax.traverse._
import spray.json._
import spray.json.DefaultJsonProtocol._

import com.daml.lf.CompiledPackages
import com.daml.lf.archive.{Dar, DarReader, Decode}
import com.daml.lf.archive.Reader.ParseError
import com.daml.lf.data.Ref._
import com.daml.lf.engine.{
  ConcurrentCompiledPackages,
  MutableCompiledPackages,
  Result,
  ResultDone,
  ResultNeedPackage
}
import com.daml.lf.language.Ast._
import com.daml.daml_lf_dev.DamlLf
import com.daml.grpc.adapter.AkkaExecutionSequencerPool
import com.daml.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.daml.ledger.api.refinements.ApiTypes.ApplicationId
import com.daml.ledger.api.v1.event.{CreatedEvent}
import com.daml.ledger.api.v1.ledger_offset.{LedgerOffset}
import com.daml.ledger.client.LedgerClient
import com.daml.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement,
}
import com.daml.platform.services.time.TimeProviderType

case class LedgerConfig(
    host: String,
    port: Int,
    timeProvider: TimeProviderType,
    commandTtl: Duration,
)

object TriggerActor {
  sealed trait Message
  final case object Stop extends Message
  final case class Failed(error: Throwable) extends Message
  final case class QueryACSFailed(cause: Throwable) extends Message
  final case class QueriedACS(
      runner: Runner,
      acs: Seq[CreatedEvent],
      offset: LedgerOffset,
  ) extends Message

  case class Config(
      compiledPackages: CompiledPackages,
      trigger: Trigger,
      ledgerConfig: LedgerConfig,
      party: String,
  )

  def apply(
      config: Config,
  )(implicit esf: ExecutionSequencerFactory, mat: Materializer): Behavior[Message] =
    Behaviors.setup { context =>
      implicit val ec: ExecutionContext = context.executionContext
      val name = context.self.path.name
      val appId = ApplicationId(name)
      val clientConfig = LedgerClientConfiguration(
        applicationId = appId.unwrap,
        ledgerIdRequirement = LedgerIdRequirement("", enabled = false),
        commandClient = CommandClientConfiguration.default.copy(
          defaultDeduplicationTime = config.ledgerConfig.commandTtl),
        sslContext = None,
      )

      // Waiting for the ACS query to finish so we can build the initial state.
      // TODO We should handle being stopped while querying the ACS.
      def queryingACS() = Behaviors.receiveMessagePartial[Message] {
        case QueryACSFailed(cause) => throw new RuntimeException("ACS query failed", cause)
        case QueriedACS(runner, acs, offset) =>
          val (killSwitch, trigger) = runner.runWithACS(
            acs,
            offset,
            msgFlow = KillSwitches.single[TriggerMsg],
          )
          // TODO If we are stopped we will end up causing the future to complete which will trigger
          // a message that is sent to a now terminated actor. We should fix this somehow™.
          context.pipeToSelf(trigger) {
            case Success(_) => Failed(new RuntimeException("Trigger exited unexpectedly"))
            case Failure(cause) => Failed(cause)
          }
          running(killSwitch)
      }

      // Trigger loop is started, wait until we should stop.
      def running(killSwitch: KillSwitch) =
        Behaviors
          .receiveMessagePartial[Message] {
            case Stop =>
              Behaviors.stopped
          }
          .receiveSignal {
            case (_, PostStop) =>
              killSwitch.shutdown
              Behaviors.same
          }

      val acsQuery =
        LedgerClient
          .singleHost(config.ledgerConfig.host, config.ledgerConfig.port, clientConfig)
          .flatMap { client =>
            val runner = new Runner(
              config.compiledPackages,
              config.trigger,
              client,
              config.ledgerConfig.timeProvider,
              appId,
              config.party)
            runner
              .queryACS()
              .map({
                case (acs, offset) =>
                  QueriedACS(runner, acs, offset)
              })
          }
      context.pipeToSelf(acsQuery) {
        case Success(msg) => msg
        case Failure(cause) => QueryACSFailed(cause)
      }
      queryingACS()
    }
}

object Server {

  sealed trait Message
  private final case class StartFailed(cause: Throwable) extends Message
  private final case class Started(binding: ServerBinding) extends Message
  final case class GetServerBinding(replyTo: ActorRef[ServerBinding]) extends Message
  final case object Stop extends Message

  case class TriggerParams(identifier: Identifier, party: String)
  implicit object IdentifierFormat extends JsonFormat[Identifier] {
    def read(value: JsValue) = value match {
      case JsString(s) => {
        val components = s.split(":")
        if (components.length == 3) {
          val parsed = for {
            pkgId <- PackageId.fromString(components(0))
            mod <- DottedName.fromString(components(1))
            entity <- DottedName.fromString(components(2))
          } yield Identifier(pkgId, QualifiedName(mod, entity))
          parsed match {
            case Left(e) => deserializationError(e)
            case Right(id) => id
          }
        } else {
          deserializationError(s"Expected trigger identifier of the form pkgid:mod:name but got $s")
        }
      }
      case _ => deserializationError("Expected trigger identifier of the form pkgid:mod:name")
    }
    def write(id: Identifier) = JsString(s"${id.packageId}:${id.qualifiedName}")
  }
  implicit val triggerParamsFormat = jsonFormat2(TriggerParams)

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

  def apply(
      host: String,
      port: Int,
      ledgerConfig: LedgerConfig,
      dar: Option[Dar[(PackageId, Package)]],
  ): Behavior[Message] = Behaviors.setup { ctx =>
    // http doesn't know about akka typed so provide untyped system
    implicit val untypedSystem: akka.actor.ActorSystem = ctx.system.toClassic
    implicit val materializer: Materializer = Materializer(untypedSystem)
    implicit val esf: ExecutionSequencerFactory =
      new AkkaExecutionSequencerPool("TriggerService")(untypedSystem)

    var triggers: Map[UUID, ActorRef[TriggerActor.Message]] = Map.empty
    // Mutable in preparation for dynamic package upload.
    val compiledPackages: MutableCompiledPackages = ConcurrentCompiledPackages()
    dar.foreach(addDar(compiledPackages, _))

    // fileUpload seems to trigger that warning and I couldn't find
    // a way to fix it so we disable the warning.
    @SuppressWarnings(Array("org.wartremover.warts.Any"))
    val route = concat(
      post {
        concat(
          // Start a new trigger given its identifier and the party it should be running as.
          // Returns a UUID for the newly started trigger.
          path("start") {
            entity(as[TriggerParams]) {
              params =>
                Trigger.fromIdentifier(compiledPackages, params.identifier) match {
                  case Left(err) =>
                    complete((StatusCodes.UnprocessableEntity, err))
                  case Right(trigger) =>
                    val uuid = UUID.randomUUID
                    val ref = ctx.spawn(
                      TriggerActor(
                        TriggerActor.Config(compiledPackages, trigger, ledgerConfig, params.party)),
                      uuid.toString,
                    )
                    triggers = triggers + (uuid -> ref)
                    complete(uuid.toString)

                }
            }
          },
          // upload a DAR as a multi-part form request with a single field called
          // "dar".
          path("upload_dar") {
            fileUpload("dar") {
              case (metadata: FileInfo, byteSource: Source[ByteString, Any]) =>
                val byteStringF: Future[ByteString] = byteSource.runFold(ByteString(""))(_ ++ _)
                onSuccess(byteStringF) {
                  byteString =>
                    val inputStream = new ByteArrayInputStream(byteString.toArray)
                    DarReader()
                      .readArchive("package-upload", new ZipInputStream(inputStream)) match {
                      case Failure(err) => complete((StatusCodes.UnprocessableEntity, err))
                      case Success(encodedDar) =>
                        try {
                          val dar = encodedDar.map {
                            case (pkgId, payload) => Decode.readArchivePayload(pkgId, payload)
                          }
                          addDar(compiledPackages, dar)
                          complete(s"DAR uploaded, main package id: ${dar.main._1}")
                        } catch {
                          case e: ParseError => complete((StatusCodes.UnprocessableEntity, e))
                        }
                    }
                }
            }
          }
        )
      },
      // Stop a trigger given its UUID
      delete {
        pathPrefix("stop" / JavaUUID) { id =>
          triggers.get(id).get ! TriggerActor.Stop
          triggers = triggers - id
          complete(s"Trigger $id has been stopped.")
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
          throw new RuntimeException("Server failed to start", cause)
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
      dar: Option[Dar[(PackageId, Package)]])
    : Future[(ServerBinding, ActorSystem[Server.Message])] = {
    val system: ActorSystem[Server.Message] =
      ActorSystem(Server(host, port, ledgerConfig, dar), "TriggerService")
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
          ActorSystem(Server("localhost", 8080, ledgerConfig, dar), "TriggerService")
        StdIn.readLine()
        system ! Server.Stop
    }
  }
}
