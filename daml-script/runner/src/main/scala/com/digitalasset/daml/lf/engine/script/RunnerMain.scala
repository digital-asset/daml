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
import scalaz.\/-
import scalaz.syntax.traverse._
import spray.json._
import com.daml.lf.PureCompiledPackages
import com.daml.lf.speedy.{SValue, Speedy, TraceLog, WarningLog}
import com.daml.lf.archive.{Dar, DarDecoder}
import com.daml.lf.data.Ref.{Identifier, PackageId, QualifiedName}
import com.daml.lf.language.Ast.Package
import com.daml.lf.language.Ast.Type
import com.daml.lf.typesig.EnvironmentSignature
import com.daml.lf.typesig.reader.SignatureReader
import com.daml.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.daml.auth.TokenHolder
import com.daml.lf.engine.script.ledgerinteraction.{ScriptLedgerClient, GrpcLedgerClient}
import java.io.FileInputStream
import com.google.protobuf.ByteString
import java.util.concurrent.atomic.AtomicBoolean
import java.io.File
import scala.util.{Failure, Success}

// We have our own type for time modes since TimeProviderType
// allows for more stuff that doesn’t make sense in Daml Script.
sealed trait ScriptTimeMode

object ScriptTimeMode {
  final case object Static extends ScriptTimeMode
  final case object WallClock extends ScriptTimeMode
}

object RunnerMain {
  def main(config: RunnerMainConfig): Unit = {
    implicit val system: ActorSystem = ActorSystem("RunnerMain")
    implicit val sequencer: ExecutionSequencerFactory =
      new AkkaExecutionSequencerPool("ScriptCliRunnerPool")(system)
    implicit val ec: ExecutionContext = system.dispatcher
    implicit val materializer: Materializer = Materializer(system)

    val flow = run(config)

    flow.onComplete(_ =>
      if (config.jsonApi) {
        Http().shutdownAllConnectionPools().flatMap { case () => system.terminate() }
      } else {
        system.terminate()
      }
    )

    if (!Await.result(flow, Duration.Inf)) {
      sys.exit(1)
    }
  }

  // We run tests sequentially for now. While tests that
  // only access per-party state and access only state of freshly allocated parties
  // can in principal be run in parallel that runs into resource limits at some point
  // and doesn’t work for tests that access things like listKnownParties.
  // Once we have a mechanism to mark tests as exclusive and control the concurrency
  // limit we can think about running tests in parallel again.
  def sequentialTraverse[A, B](
      seq: Seq[A]
  )(f: A => Future[B])(implicit ec: ExecutionContext): Future[Seq[B]] =
    seq.foldLeft(Future.successful(Seq.empty[B])) { case (acc, nxt) =>
      acc.flatMap(bs => f(nxt).map(b => bs :+ b))
    }

  def run(config: RunnerMainConfig)(implicit
      system: ActorSystem,
      sequencer: ExecutionSequencerFactory,
      ec: ExecutionContext,
      materializer: Materializer,
  ): Future[Boolean] =
    for {
      _ <- Future.successful(())
      traceLog = Speedy.Machine.newTraceLog
      warningLog = Speedy.Machine.newWarningLog

      dar: Dar[(PackageId, Package)] = DarDecoder.assertReadArchiveFromFile(config.darPath)

      majorVersion = dar.main._2.languageVersion.major
      compiledPackages = PureCompiledPackages.assertBuild(
        dar.all.toMap,
        Runner.compilerConfig(majorVersion),
      )
      ifaceDar =
        dar.map(pkg => SignatureReader.readPackageSignature(() => \/-(pkg))._2)
      envIface = EnvironmentSignature.fromPackageSignatures(ifaceDar)

      clients <- connectToParticipants(config, compiledPackages, envIface, traceLog, warningLog)

      _ <- (clients.getParticipant(None), config.uploadDar) match {
        case (Left(err), _) => throw new RuntimeException(err)
        // RunnerMainConfig ensures uploadDar cannot be true when not using Grpc.
        case (Right(client: GrpcLedgerClient), true) =>
          client.grpcClient.packageManagementClient
            .uploadDarFile(ByteString.readFrom(new FileInputStream(config.darPath)))
        case _ => Future.unit
      }

      runScript = (
          scriptId: Identifier,
          inputFile: Option[File],
          outputFile: Option[File],
          convertInputValue: Option[(JsValue, Type) => Either[String, SValue]],
      ) =>
        for {
          result <- Runner
            .run(
              compiledPackages,
              scriptId,
              convertInputValue,
              inputFile.map(file => java.nio.file.Files.readString(file.toPath).parseJson),
              clients,
              config.timeMode,
              traceLog,
              warningLog,
              enableContractUpgrading = config.enableContractUpgrading,
            )
          _ <- Future {
            outputFile.foreach { outputFile =>
              val jsVal = LfValueCodec.apiValueToJsValue(result.toUnnormalizedValue)
              val outDir = outputFile.getParentFile
              if (outDir != null) {
                val _ = Files.createDirectories(outDir.toPath)
              }
              Files.write(outputFile.toPath, Seq(jsVal.prettyPrint).asJava)
            }
          }
        } yield ()

      success <- config.runMode match {
        case RunnerMainConfig.RunMode.RunAll => {
          val success = new AtomicBoolean(true)
          val testScripts: Seq[Identifier] = dar.main._2.modules.flatMap {
            case (moduleName, module) =>
              module.definitions.collect(Function.unlift { case (name, _) =>
                val id = Identifier(dar.main._1, QualifiedName(moduleName, name))
                Script.fromIdentifier(compiledPackages, id) match {
                  // We exclude generated identifiers starting with `$`.
                  case Right(_: Script.Action) if !name.dottedName.startsWith("$") =>
                    Some(id)
                  case _ => None
                }
              })
          }.toSeq

          sequentialTraverse(testScripts.sorted) { id =>
            runScript(id, None, None, None)
              .andThen {
                case Failure(exception) =>
                  success.set(false)
                  println(s"${id.qualifiedName} FAILURE ($exception)")
                case Success(_) =>
                  println(s"${id.qualifiedName} SUCCESS")
              }
              // Do not abort in case of failure, but complete all test runs.
              .recover { case _ => () }
          }.map { case _ => success.get() }
        }
        case RunnerMainConfig.RunMode.RunOne(scriptName, inputFile, outputFile) => {
          val scriptId: Identifier =
            Identifier(dar.main._1, QualifiedName.assertFromString(scriptName))
          val converter = (json: JsValue, typ: Type) =>
            Converter(majorVersion).fromJsonValue(
              scriptId.qualifiedName,
              envIface,
              compiledPackages,
              typ,
              json,
            )
          runScript(scriptId, inputFile, outputFile, Some(converter)).map(_ => true)
        }

      }
    } yield success

  def connectToParticipants(
      config: RunnerMainConfig,
      compiledPackages: PureCompiledPackages,
      envIface: EnvironmentSignature,
      traceLog: TraceLog,
      warningLog: WarningLog,
  )(implicit
      system: ActorSystem,
      sequencer: ExecutionSequencerFactory,
      ec: ExecutionContext,
  ): Future[Participants[ScriptLedgerClient]] = {
    val token = config.accessTokenFile.map(new TokenHolder(_)).flatMap(_.token)
    config.participantMode match {
      case ParticipantMode.RemoteParticipantConfig(file) =>
        val jsVal = java.nio.file.Files.readString(file.toPath).parseJson
        import ParticipantsJsonProtocol._
        val params =
          jsVal
            .convertTo[Participants[ApiParameters]]
            .map(params =>
              params.copy(
                access_token = params.access_token.orElse(token),
                application_id = params.application_id.orElse(config.applicationId),
              )
            )
        connectApiParameters(config, params, envIface)
      case ParticipantMode.RemoteParticipantHost(host, port, oAdminPort) =>
        val params =
          Participants(
            default_participant =
              Some(ApiParameters(host, port, token, config.applicationId, oAdminPort)),
            participants = Map.empty,
            party_participants = Map.empty,
          )
        connectApiParameters(config, params, envIface)
      case ParticipantMode.IdeLedgerParticipant() =>
        Runner.ideLedgerClient(compiledPackages, traceLog, warningLog)
    }
  }

  def connectApiParameters(
      config: RunnerMainConfig,
      participantParams: Participants[ApiParameters],
      envIface: EnvironmentSignature,
  )(implicit
      system: ActorSystem,
      sequencer: ExecutionSequencerFactory,
      ec: ExecutionContext,
  ): Future[Participants[ScriptLedgerClient]] =
    if (config.jsonApi) {
      // TODO (#13973) resolve envIface, or not, depending on whether inherited choices are needed
      Runner.jsonClients(participantParams, envIface)
    } else {
      Runner.connect(participantParams, config.tlsConfig, config.maxInboundMessageSize)
    }
}
