// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream._
import java.nio.file.Files
import java.time.Instant
import java.util.stream.Collectors
import scala.collection.JavaConverters._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.io.Source
import scalaz.\/-
import scalaz.syntax.traverse._
import spray.json._

import com.daml.api.util.TimeProvider
import com.daml.lf.archive.{Dar, DarReader}
import com.daml.lf.archive.Decode
import com.daml.lf.data.Ref.{Identifier, PackageId, QualifiedName}
import com.daml.lf.iface.EnvironmentInterface
import com.daml.lf.iface.reader.InterfaceReader
import com.daml.lf.language.Ast.Package
import com.daml.daml_lf_dev.DamlLf
import com.daml.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.daml.ledger.api.refinements.ApiTypes.ApplicationId
import com.daml.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement
}
import com.daml.platform.services.time.TimeProviderType
import com.daml.auth.TokenHolder

object RunnerMain {

  def main(args: Array[String]): Unit = {

    RunnerConfig.parse(args) match {
      case None => sys.exit(1)
      case Some(config) => {
        val encodedDar: Dar[(PackageId, DamlLf.ArchivePayload)] =
          DarReader().readArchiveFromFile(config.darPath).get
        val dar: Dar[(PackageId, Package)] = encodedDar.map {
          case (pkgId, pkgArchive) => Decode.readArchivePayload(pkgId, pkgArchive)
        }
        val scriptId: Identifier =
          Identifier(dar.main._1, QualifiedName.assertFromString(config.scriptIdentifier))

        val applicationId = ApplicationId("Script Runner")
        val timeProvider: TimeProvider =
          config.timeProviderType.getOrElse(RunnerConfig.DefaultTimeProviderType) match {
            case TimeProviderType.Static => TimeProvider.Constant(Instant.EPOCH)
            case TimeProviderType.WallClock => TimeProvider.UTC
            case _ =>
              throw new RuntimeException(s"Unexpected TimeProviderType: $config.timeProviderType")
          }

        implicit val system: ActorSystem = ActorSystem("ScriptRunner")
        implicit val sequencer: ExecutionSequencerFactory =
          new AkkaExecutionSequencerPool("ScriptRunnerPool")(system)
        implicit val ec: ExecutionContext = system.dispatcher
        implicit val materializer: Materializer = Materializer(system)

        val inputValue = config.inputFile.map(file => {
          val source = Source.fromFile(file)
          val fileContent = try {
            source.mkString
          } finally {
            source.close()
          }
          fileContent.parseJson
        })

        val participantParams = config.participantConfig match {
          case Some(file) => {
            val source = Source.fromFile(file)
            val fileContent = try {
              source.mkString
            } finally {
              source.close
            }
            val jsVal = fileContent.parseJson
            import ParticipantsJsonProtocol._
            jsVal.convertTo[Participants[ApiParameters]]
          }
          case None =>
            Participants(
              default_participant =
                Some(ApiParameters(config.ledgerHost.get, config.ledgerPort.get)),
              participants = Map.empty,
              party_participants = Map.empty)
        }
        val flow: Future[Unit] = for {

          clients <- if (config.jsonApi) {
            // We fail during config parsing if this does not exist.
            val tokenFile = config.accessTokenFile.get
            val token = Files.readAllLines(tokenFile).stream.collect(Collectors.joining("\n"))
            val ifaceDar = dar.map(pkg => InterfaceReader.readInterface(() => \/-(pkg))._2)
            val envIface = EnvironmentInterface.fromReaderInterfaces(ifaceDar)
            Runner.jsonClients(participantParams, token, envIface)
          } else {
            // Note (MK): For now, we only support using a single-token for everything.
            // We might want to extend this to allow for multiple tokens, e.g., one token per party +
            // one admin token for allocating parties.
            val tokenHolder = config.accessTokenFile.map(new TokenHolder(_))
            val clientConfig = LedgerClientConfiguration(
              applicationId = ApplicationId.unwrap(applicationId),
              ledgerIdRequirement = LedgerIdRequirement("", enabled = false),
              commandClient = CommandClientConfiguration.default,
              sslContext = config.tlsConfig.flatMap(_.client),
              token = tokenHolder.flatMap(_.token),
            )
            Runner.connect(participantParams, clientConfig, config.maxInboundMessageSize)
          }
          result <- Runner.run(dar, scriptId, inputValue, clients, applicationId, timeProvider)
          _ <- Future {
            config.outputFile.foreach { outputFile =>
              val jsVal = LfValueCodec.apiValueToJsValue(
                result.toValue.assertNoRelCid(rcoid => s"Unexpected relative contract id $rcoid"))
              Files.write(outputFile.toPath, Seq(jsVal.prettyPrint).asJava)
            }
          }
        } yield ()

        flow.onComplete(_ =>
          if (config.jsonApi) {
            Http().shutdownAllConnectionPools().flatMap { case () => system.terminate() }
          } else {
            system.terminate()
        })
        Await.result(flow, Duration.Inf)
      }
    }
  }
}
