// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream._
import java.nio.file.Files
import scala.collection.JavaConverters._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.io.Source
import scalaz.\/-
import scalaz.syntax.traverse._
import spray.json._

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
        val timeMode: ScriptTimeMode = config.timeMode.getOrElse(RunnerConfig.DefaultTimeMode)

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
            // To avoid a breaking change, we allow specifying
            // --access-token-file and --participant-config
            // together and use the token file as the default for all participants
            // that do not specify an explicit token.
            val source = Source.fromFile(file)
            val fileContent = try {
              source.mkString
            } finally {
              source.close
            }
            val jsVal = fileContent.parseJson
            val token = config.accessTokenFile.map(new TokenHolder(_)).flatMap(_.token)
            import ParticipantsJsonProtocol._
            jsVal
              .convertTo[Participants[ApiParameters]]
              .map(params => params.copy(access_token = params.access_token.orElse(token)))
          }
          case None =>
            val tokenHolder = config.accessTokenFile.map(new TokenHolder(_))
            Participants(
              default_participant = Some(
                ApiParameters(
                  config.ledgerHost.get,
                  config.ledgerPort.get,
                  tokenHolder.flatMap(_.token))),
              participants = Map.empty,
              party_participants = Map.empty
            )
        }
        val flow: Future[Unit] = for {

          clients <- if (config.jsonApi) {
            val ifaceDar = dar.map(pkg => InterfaceReader.readInterface(() => \/-(pkg))._2)
            val envIface = EnvironmentInterface.fromReaderInterfaces(ifaceDar)
            Runner.jsonClients(participantParams, envIface)
          } else {
            // Note (MK): For now, we only support using a single-token for everything.
            // We might want to extend this to allow for multiple tokens, e.g., one token per party +
            // one admin token for allocating parties.
            val tokenHolder = config.accessTokenFile.map(new TokenHolder(_))
            val clientConfig = LedgerClientConfiguration(
              applicationId = ApplicationId.unwrap(applicationId),
              ledgerIdRequirement = LedgerIdRequirement.none,
              commandClient = CommandClientConfiguration.default,
              sslContext = config.tlsConfig.flatMap(_.client),
              token = tokenHolder.flatMap(_.token),
            )
            Runner.connect(
              participantParams,
              applicationId,
              config.tlsConfig,
              config.maxInboundMessageSize)
          }
          result <- Runner.run(dar, scriptId, inputValue, clients, applicationId, timeMode)
          _ <- Future {
            config.outputFile.foreach { outputFile =>
              val jsVal = LfValueCodec.apiValueToJsValue(result.toValue)
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
