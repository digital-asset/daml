// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import com.daml.lf.language.Ast.Package
import com.daml.lf.typesig.EnvironmentSignature
import com.daml.lf.typesig.reader.SignatureReader
import com.daml.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.daml.auth.TokenHolder
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.lf.data.NoCopy
import com.daml.lf.engine.script.ledgerinteraction.ScriptTimeMode
import com.daml.ledger.api.refinements.ApiTypes.ApplicationId

import java.io.File
import scala.util.Try

object RunnerMain {

  def main(runnerConfig: RunnerCliConfig): Unit = {

    implicit val system: ActorSystem = ActorSystem("ScriptRunner")
    implicit val sequencer: ExecutionSequencerFactory =
      new AkkaExecutionSequencerPool("ScriptRunnerPool")(system)
    implicit val ec: ExecutionContext = system.dispatcher
    implicit val materializer: Materializer = Materializer(system)

    val flow: Future[Unit] = for {
      config <- Future.fromTry(RunnerConfig(runnerConfig))
      clients <-
        if (config.jsonApi) {
          val ifaceDar =
            config.dar.map(pkg => SignatureReader.readPackageSignature(() => \/-(pkg))._2)
          val envSig = EnvironmentSignature.fromPackageSignatures(ifaceDar)
          // TODO (#13973) resolve envSig, or not, depending on whether inherited choices are needed
          Runner.jsonClients(config.participantParams, envSig)
        } else {
          Runner.connect(config.participantParams, config.tlsConfig, config.maxInboundMessageSize)
        }
      result <- Runner.run(config.dar, config.scriptId, config.inputValue, clients, config.timeMode)
      _ <- Future {
        config.outputFile.foreach { outputFile =>
          val jsVal = LfValueCodec.apiValueToJsValue(result.toUnnormalizedValue)
          val outDir = outputFile.getParentFile
          if (outDir != null) {
            val _ = Files.createDirectories(outDir.toPath)
          }
          Files.write(outputFile.toPath, Seq(jsVal.prettyPrint).asJava)
        }
      }
    } yield ()

    flow.onComplete(_ =>
      if (runnerConfig.jsonApi) {
        Http().shutdownAllConnectionPools().flatMap { case () => system.terminate() }
      } else {
        system.terminate()
      }
    )
    Await.result(flow, Duration.Inf)
  }

  final case class RunnerConfig private (
      dar: Dar[(PackageId, Package)],
      scriptId: Identifier,
      participantParams: Participants[ApiParameters],
      timeMode: ScriptTimeMode,
      inputValue: Option[JsValue],
      outputFile: Option[File],
      token: Option[String],
      tlsConfig: TlsConfiguration,
      jsonApi: Boolean,
      maxInboundMessageSize: Int,
      applicationId: Option[ApplicationId],
  ) extends NoCopy

  object RunnerConfig {
    private[script] def apply(config: RunnerCliConfig): Try[RunnerConfig] = Try {
      val dar: Dar[(PackageId, Package)] = DarDecoder.assertReadArchiveFromFile(config.darPath)
      val scriptId: Identifier =
        Identifier(dar.main._1, QualifiedName.assertFromString(config.scriptIdentifier))
      val token = config.accessTokenFile.map(new TokenHolder(_)).flatMap(_.token)
      val inputValue: Option[JsValue] = config.inputFile.map(file => {
        val source = Source.fromFile(file)
        val fileContent =
          try {
            source.mkString
          } finally {
            source.close()
          }
        fileContent.parseJson
      })
      val participantParams: Participants[ApiParameters] = config.participantConfig match {
        case Some(file) =>
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
          import ParticipantsJsonProtocol._
          jsVal
            .convertTo[Participants[ApiParameters]]
            .map(params =>
              params.copy(
                access_token = params.access_token.orElse(token),
                application_id = params.application_id.orElse(config.applicationId),
              )
            )

        case None =>
          Participants(
            default_participant = Some(
              ApiParameters(
                config.ledgerHost.get,
                config.ledgerPort.get,
                token,
                config.applicationId,
              )
            ),
            participants = Map.empty,
            party_participants = Map.empty,
          )
      }

      new RunnerConfig(
        dar,
        scriptId,
        participantParams,
        config.timeMode,
        inputValue,
        config.outputFile,
        token,
        config.tlsConfig,
        config.jsonApi,
        config.maxInboundMessageSize,
        config.applicationId,
      )
    }
  }
}
