// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script

import java.io.FileInputStream
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.daml.api.util.TimeProvider
import com.daml.lf.PureCompiledPackages
import com.daml.lf.archive.{Dar, DarReader, Decode}
import com.daml.lf.data.Ref.{Identifier, PackageId, QualifiedName}
import com.daml.lf.language.Ast.Package
import com.daml.daml_lf_dev.DamlLf
import com.daml.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.daml.ledger.api.refinements.ApiTypes.ApplicationId
import com.daml.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement
}
import com.daml.platform.sandbox.SandboxServer
import com.daml.platform.sandbox.config.SandboxConfig
import com.daml.platform.services.time.TimeProviderType
import com.daml.ports.Port
import com.google.protobuf.ByteString
import com.typesafe.scalalogging.StrictLogging
import scalaz.syntax.traverse._
import spray.json._

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.io.Source
import scala.util.{Failure, Success}

object TestMain extends StrictLogging {

  def main(args: Array[String]): Unit = {

    TestConfig.parse(args) match {
      case None => sys.exit(1)
      case Some(config) =>
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

        val system: ActorSystem = ActorSystem("ScriptTest")
        implicit val sequencer: ExecutionSequencerFactory =
          new AkkaExecutionSequencerPool("ScriptTestPool")(system)
        implicit val materializer: Materializer = Materializer(system)
        implicit val ec: ExecutionContext = system.dispatcher

        val (participantParams, participantCleanup) = config.participantConfig match {
          case Some(file) =>
            val source = Source.fromFile(file)
            val fileContent = try {
              source.mkString
            } finally {
              source.close
            }
            val jsVal = fileContent.parseJson
            import ParticipantsJsonProtocol._
            (jsVal.convertTo[Participants[ApiParameters]], () => Future.successful(()))
          case None =>
            val (apiParameters, cleanup) = if (config.ledgerHost.isEmpty) {
              val sandboxConfig = SandboxConfig.default.copy(
                port = Port.Dynamic,
                timeProviderType = Some(config.timeProviderType),
              )
              val sandboxResource = SandboxServer.owner(sandboxConfig).acquire()
              val sandboxPort =
                Await.result(sandboxResource.asFuture.flatMap(_.portF).map(_.value), Duration.Inf)
              (ApiParameters("localhost", sandboxPort), () => sandboxResource.release())
            } else {
              (
                ApiParameters(config.ledgerHost.get, config.ledgerPort.get),
                () => Future.successful(()),
              )
            }
            (
              Participants(
                default_participant = Some(apiParameters),
                participants = Map.empty,
                party_participants = Map.empty),
              cleanup,
            )
        }

        val darMap = dar.all.toMap
        val compiledPackages = PureCompiledPackages(darMap).right.get
        val testScripts = dar.main._2.modules.flatMap {
          case (moduleName, module) => {
            module.definitions.collect(Function.unlift {
              case (name, defn) => {
                val id = Identifier(dar.main._1, QualifiedName(moduleName, name))
                Script.fromIdentifier(compiledPackages, id) match {
                  case Right(script: Script.Action) => Some((id, script))
                  case _ => None
                }
              }
            })
          }
        }

        val flow: Future[Boolean] = for {
          clients <- Runner.connect(participantParams, clientConfig, config.maxInboundMessageSize)
          _ <- clients.getParticipant(None) match {
            case Left(err) => throw new RuntimeException(err)
            case Right(client) =>
              client.grpcClient.packageManagementClient
                .uploadDarFile(ByteString.readFrom(new FileInputStream(config.darPath)))
          }
          success = new AtomicBoolean(true)
          _ <- Future.sequence {
            testScripts.map {
              case (id, script) => {
                val runner =
                  new Runner(compiledPackages, script, applicationId, timeProvider)
                val testRun: Future[Unit] = runner.runWithClients(clients).map(_ => ())
                // Print test result and remember failure.
                testRun.onComplete {
                  case Failure(exception) =>
                    success.set(false)
                    println(s"${id.qualifiedName} FAILURE ($exception)")
                  case Success(_) =>
                    println(s"${id.qualifiedName} SUCCESS")
                }
                // Do not abort in case of failure, but complete all test runs.
                testRun.recover {
                  case _ => ()
                }
              }
            }
          }
        } yield success.get()

        flow.onComplete { _ =>
          Await.result(participantCleanup(), Duration.Inf)
          system.terminate()
        }

        if (!Await.result(flow, Duration.Inf)) {
          sys.exit(1)
        }
    }
  }
}
