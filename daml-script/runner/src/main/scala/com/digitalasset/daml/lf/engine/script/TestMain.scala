// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine.script

import java.io.FileInputStream

import akka.actor.ActorSystem
import akka.stream._
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.io.Source
import scalaz.syntax.traverse._
import spray.json._
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.daml.lf.archive.{Dar, DarReader}
import com.digitalasset.daml.lf.archive.Decode
import com.digitalasset.daml.lf.data.Ref.{Identifier, PackageId, QualifiedName}
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml.lf.language.Ast.Package
import com.digitalasset.daml_lf_dev.DamlLf
import com.digitalasset.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.digitalasset.ledger.api.refinements.ApiTypes.ApplicationId
import com.digitalasset.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement
}
import com.digitalasset.ledger.client.services.commands.CommandUpdater
import com.digitalasset.platform.sandbox.SandboxServer
import com.digitalasset.platform.sandbox.config.SandboxConfig
import com.digitalasset.platform.services.time.TimeProviderType
import com.google.protobuf.ByteString

import scala.util.control.NonFatal
import scala.util.{Failure, Success}

object TestMain {

  def main(args: Array[String]): Unit = {

    TestConfig.parse(args) match {
      case None => sys.exit(1)
      case Some(config) => {
        val encodedDar: Dar[(PackageId, DamlLf.ArchivePayload)] =
          DarReader().readArchiveFromFile(config.darPath).get
        val dar: Dar[(PackageId, Package)] = encodedDar.map {
          case (pkgId, pkgArchive) => Decode.readArchivePayload(pkgId, pkgArchive)
        }

        val applicationId = ApplicationId("Script Test")
        val clientConfig = LedgerClientConfiguration(
          applicationId = ApplicationId.unwrap(applicationId),
          ledgerIdRequirement = LedgerIdRequirement("", enabled = false),
          commandClient = CommandClientConfiguration.default,
          sslContext = None
        )
        val timeProvider: TimeProvider =
          config.timeProviderType match {
            case TimeProviderType.Static => TimeProvider.Constant(Instant.EPOCH)
            case TimeProviderType.WallClock => TimeProvider.UTC
            case _ =>
              throw new RuntimeException(s"Unexpected TimeProviderType: $config.timeProviderType")
          }
        val commandUpdater = new CommandUpdater(
          timeProviderO = Some(timeProvider),
          ttl = config.commandTtl,
          overrideTtl = true)

        val system: ActorSystem = ActorSystem("ScriptTest")
        implicit val sequencer: ExecutionSequencerFactory =
          new AkkaExecutionSequencerPool("ScriptTestPool")(system)
        implicit val materializer: Materializer = Materializer(system)
        implicit val ec: ExecutionContext = system.dispatcher

        val runner = new Runner(dar, applicationId, commandUpdater)
        val (participantParams, participantCleanup) = config.participantConfig match {
          case Some(file) => {
            val source = Source.fromFile(file)
            val fileContent = try {
              source.mkString
            } finally {
              source.close
            }
            val jsVal = fileContent.parseJson
            import ParticipantsJsonProtocol._
            (jsVal.convertTo[Participants[ApiParameters]], () => ())
          }
          case None =>
            val (apiParameters, cleanup) = if (config.ledgerHost.isEmpty) {
              val sandboxConfig = SandboxConfig.default.copy(
                timeProviderType = config.timeProviderType
              )
              val sandbox = new SandboxServer(sandboxConfig)
              val sandboxClosed = new AtomicBoolean(false)

              def closeSandbox(): Unit = {
                if (sandboxClosed.compareAndSet(false, true)) sandbox.close()
              }

              try Runtime.getRuntime.addShutdownHook(new Thread(() => closeSandbox()))
              catch {
                case NonFatal(t) =>
                  //logger.error("Shutting down Sandbox application because of initialization error", t)
                  closeSandbox()
              }
              (ApiParameters("localhost", sandbox.port), () => closeSandbox())
            } else {
              (ApiParameters(config.ledgerHost.get, config.ledgerPort.get), () => ())
            }
            (
              Participants(
                default_participant = Some(apiParameters),
                participants = Map.empty,
                party_participants = Map.empty),
              cleanup)
        }

        val flow: Future[Unit] = for {
          clients <- Runner.connect(participantParams, clientConfig)
          _ <- clients.getParticipant(None) match {
            case Left(err) => throw new RuntimeException(err)
            case Right(client) =>
              client.packageManagementClient.uploadDarFile(
                ByteString.readFrom(new FileInputStream(config.darPath)))
          }
          _ <- Future.sequence {
            dar.main._2.modules.flatMap {
              case (moduleName, module) =>
                module.definitions.collect {
                  case (name, Ast.DValue(Ast.TApp(Ast.TTyCon(tycon), _), _, _, _))
                      if tycon == runner.scriptTyCon =>
                    val testRun: Future[Unit] = for {
                      _ <- runner.run(
                        clients,
                        Identifier(dar.main._1, QualifiedName(moduleName, name)),
                        None)
                    } yield ()
                    testRun.onComplete {
                      case Failure(exception) =>
                        println(s"$moduleName:$name FAILURE ($exception)")
                      case Success(_) =>
                        println(s"$moduleName:$name SUCCESS")
                    }
                    testRun
                }
            }
          }
        } yield ()

        flow.onComplete { _ =>
          participantCleanup()
          system.terminate()
        }
        Await.result(flow, Duration.Inf)
      }
    }
  }
}
