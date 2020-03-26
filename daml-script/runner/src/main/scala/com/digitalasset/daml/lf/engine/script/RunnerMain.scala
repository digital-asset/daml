// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine.script

import akka.actor.ActorSystem
import akka.stream._
import java.time.Instant
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.io.Source
import scalaz.syntax.traverse._
import spray.json._

import com.digitalasset.api.util.TimeProvider
import com.digitalasset.daml.lf.archive.{Dar, DarReader}
import com.digitalasset.daml.lf.archive.Decode
import com.digitalasset.daml.lf.data.Ref.{Identifier, PackageId, QualifiedName}
import com.digitalasset.daml.lf.language.Ast.Package
import com.digitalasset.daml_lf_dev.DamlLf
import com.digitalasset.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.digitalasset.ledger.api.refinements.ApiTypes.ApplicationId
import com.digitalasset.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement
}
import com.digitalasset.platform.services.time.TimeProviderType
import com.digitalasset.auth.TokenHolder

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
        val timeProvider: TimeProvider =
          config.timeProviderType match {
            case TimeProviderType.Static => TimeProvider.Constant(Instant.EPOCH)
            case TimeProviderType.WallClock => TimeProvider.UTC
            case _ =>
              throw new RuntimeException(s"Unexpected TimeProviderType: $config.timeProviderType")
          }

        val system: ActorSystem = ActorSystem("ScriptRunner")
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
          clients <- Runner.connect(participantParams, clientConfig)
          _ <- Runner.run(dar, scriptId, inputValue, clients, applicationId, timeProvider)
        } yield ()

        flow.onComplete(_ => system.terminate())
        Await.result(flow, Duration.Inf)
      }
    }
  }
}
