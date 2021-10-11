// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream._
import java.nio.file.Files
import scala.jdk.CollectionConverters._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.io.Source
import scalaz.\/-
import scalaz.syntax.traverse._
import spray.json._
import com.daml.lf.archive.{Dar, DarDecoder}
import com.daml.lf.data.Ref.{Identifier, PackageId, QualifiedName}
import com.daml.lf.engine.script.ledgerinteraction.ScriptTimeMode
import com.daml.lf.iface.EnvironmentInterface
import com.daml.lf.iface.reader.InterfaceReader
import com.daml.lf.language.Ast.Package
import com.daml.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.daml.auth.TokenHolder

object RunnerMain {

  def main(args: Array[String]): Unit = {
    RunnerConfig.parse(args) match {
      case None => sys.exit(1)
      case Some(config) => main(config)
    }
  }

  def main(config: RunnerConfig): Unit = {
    val dar: Dar[(PackageId, Package)] = DarDecoder.assertReadArchiveFromFile(config.darPath)
    val scriptId: Identifier =
      Identifier(dar.main._1, QualifiedName.assertFromString(config.scriptIdentifier))

    val timeMode: ScriptTimeMode = config.timeMode.getOrElse(RunnerConfig.DefaultTimeMode)

    implicit val system: ActorSystem = ActorSystem("ScriptRunner")
    implicit val sequencer: ExecutionSequencerFactory =
      new AkkaExecutionSequencerPool("ScriptRunnerPool")(system)
    implicit val ec: ExecutionContext = system.dispatcher
    implicit val materializer: Materializer = Materializer(system)

    val inputValue = config.inputFile.map(file => {
      val source = Source.fromFile(file)
      val fileContent =
        try {
          source.mkString
        } finally {
          source.close()
        }
      fileContent.parseJson
    })

    val participantParams = config.participantConfig match {
      case Some(file) => {
        // We allow specifying --access-token-file/--application-id together with
        // --participant-config and use the values as the default for
        // all participants that do not specify an explicit token.
        val source = Source.fromFile(file)
        val fileContent =
          try {
            source.mkString
          } finally {
            source.close
          }
        val jsVal = fileContent.parseJson
        val token = config.accessTokenFile.map(new TokenHolder(_)).flatMap(_.token)
        import ParticipantsJsonProtocol._
        jsVal
          .convertTo[Participants[ApiParameters]]
          .map(params =>
            params.copy(
              access_token = params.access_token.orElse(token),
              application_id = params.application_id.orElse(config.applicationId),
            )
          )
      }
      case None =>
        val tokenHolder = config.accessTokenFile.map(new TokenHolder(_))
        Participants(
          default_participant = Some(
            ApiParameters(
              config.ledgerHost.get,
              config.ledgerPort.get,
              tokenHolder.flatMap(_.token),
              config.applicationId,
            )
          ),
          participants = Map.empty,
          party_participants = Map.empty,
        )
    }
    val flow: Future[Unit] = for {

      clients <-
        if (config.jsonApi) {
          val ifaceDar = dar.map(pkg => InterfaceReader.readInterface(() => \/-(pkg))._2)
          val envIface = EnvironmentInterface.fromReaderInterfaces(ifaceDar)
          Runner.jsonClients(participantParams, envIface)
        } else {
          Runner.connect(participantParams, config.tlsConfig, config.maxInboundMessageSize)
        }
      result <- Runner.run(dar, scriptId, inputValue, clients, timeMode)
      _ <- Future {
        config.outputFile.foreach { outputFile =>
          val jsVal = LfValueCodec.apiValueToJsValue(result.toUnnormalizedValue)
          val outDir = outputFile.getParentFile()
          if (outDir != null) {
            val _ = Files.createDirectories(outDir.toPath())
          }
          Files.write(outputFile.toPath, Seq(jsVal.prettyPrint).asJava)
        }
      }
    } yield ()

    flow.onComplete(_ =>
      if (config.jsonApi) {
        Http().shutdownAllConnectionPools().flatMap { case () => system.terminate() }
      } else {
        system.terminate()
      }
    )
    Await.result(flow, Duration.Inf)
  }
}
