// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine.script

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream._
import java.nio.file.Files

import scala.jdk.CollectionConverters._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scalaz.\/-
import scalaz.syntax.traverse._
import spray.json._
import com.digitalasset.daml.lf.archive.{Dar, DarDecoder}
import com.digitalasset.daml.lf.data.Ref.{Identifier, PackageId, QualifiedName}
import com.digitalasset.daml.lf.engine.preprocessing.ValueTranslator
import com.digitalasset.daml.lf.engine.ScriptEngine.{
  TraceLog,
  WarningLog,
  newTraceLog,
  newWarningLog,
  defaultCompilerConfig,
}
import com.digitalasset.daml.lf.language.Ast.{Package, Type}
import com.digitalasset.daml.lf.PureCompiledPackages
import com.digitalasset.daml.lf.typesig.EnvironmentSignature
import com.digitalasset.daml.lf.typesig.reader.SignatureReader
import com.digitalasset.daml.lf.value._
import com.daml.grpc.adapter.{ExecutionSequencerFactory, PekkoExecutionSequencerPool}
import com.daml.auth.TokenHolder
import com.digitalasset.daml.lf.engine.script.ledgerinteraction.{
  GrpcLedgerClient,
  ScriptLedgerClient,
}
import java.io.FileInputStream

import com.google.protobuf.ByteString
import java.io.File

import com.digitalasset.canton.tracing.TraceContext

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
      new PekkoExecutionSequencerPool("ScriptCliRunnerPool")(system)
    implicit val ec: ExecutionContext = system.dispatcher
    implicit val materializer: Materializer = Materializer(system)
    implicit val traceContext: TraceContext = TraceContext.empty

    val flow = run(config)

    flow.onComplete(_ => system.terminate())

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
      sequencer: ExecutionSequencerFactory,
      ec: ExecutionContext,
      materializer: Materializer,
      traceContext: TraceContext,
  ): Future[Boolean] =
    for {
      _ <- Future.successful(())
      traceLog = newTraceLog
      warningLog = newWarningLog

      dar: Dar[(PackageId, Package)] = DarDecoder.assertReadArchiveFromFile(config.darPath)

      majorVersion = dar.main._2.languageVersion.major
      compiledPackages = PureCompiledPackages.assertBuild(
        dar.all.toMap,
        defaultCompilerConfig,
      )
      ifaceDar =
        dar.map { case (pkgId, _) =>
          SignatureReader
            .readPackageSignature(() => \/-(pkgId -> compiledPackages.signatures(pkgId)))
            ._2
        }
      envIface = EnvironmentSignature.fromPackageSignatures(ifaceDar)

      clients <- connectToParticipants(config, compiledPackages, traceLog, warningLog)

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
          convertInputValue: Option[(JsValue, Type) => Either[String, Value]],
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
            )
          result <- Future {
            outputFile.foreach { outputFile =>
              val pureResult = Value
                .castExtendedValue(result)
                .fold(throw _, identity)
              val jsVal = LfValueCodec.apiValueToJsValue(pureResult)
              val outDir = outputFile.getParentFile
              if (outDir != null) {
                val _ = Files.createDirectories(outDir.toPath)
              }
              Files.write(outputFile.toPath, Seq(jsVal.prettyPrint).asJava)
            }
            result
          }
        } yield result

      runManyTests = (testScripts: Seq[Identifier]) => {
        sequentialTraverse(testScripts.sorted) { id =>
          runScript(id, None, None, None)
            .transform {
              case Failure(exception) => Success((id, Left(exception)))
              case Success(result) => Success((id, Right(result)))
            }
        }.map { case results =>
          config.resultMode match {
            case RunnerMainConfig.ResultMode.Text =>
              results.foreach {
                case (id, Left(exception)) => println(s"${id.qualifiedName} FAILURE ($exception)")
                case (id, Right(_)) => println(s"${id.qualifiedName} SUCCESS")
              }
            case RunnerMainConfig.ResultMode.Json(path) =>
              val pathParent = path.getParentFile
              if (pathParent != null) {
                val _ = Files.createDirectories(pathParent.toPath)
              }
              val jsString = JsObject(Map.from(results).map {
                case (id, Left(exception)) =>
                  (
                    id.qualifiedName.toString,
                    JsObject(Map(("error", JsString(exception.toString)))),
                  )
                case (id, Right(result)) =>
                  val pureResult = Value
                    .castExtendedValue(result)
                    .fold(throw _, identity)
                  (
                    id.qualifiedName.toString,
                    JsObject(Map(("result", LfValueCodec.apiValueToJsValue(pureResult)))),
                  )
              }).prettyPrint
              Files.write(path.toPath, Seq(jsString).asJava)
          }

          !results.exists(_._2.isLeft)
        }
      }

      success <- config.runMode match {
        case RunnerMainConfig.RunMode.RunExcluding(excludes) => {
          val testScripts: Seq[Identifier] = dar.main._2.modules.flatMap {
            case (moduleName, module) =>
              module.definitions.collect(Function.unlift { case (name, _) =>
                val id = Identifier(dar.main._1, QualifiedName(moduleName, name))
                ScriptAction.fromIdentifier(compiledPackages, id) match {
                  // We exclude generated identifiers starting with `$`.
                  case Right(_: ScriptAction.NoParam)
                      if !name.dottedName.startsWith("$") &&
                        !excludes.contains(id.qualifiedName.toString) =>
                    Some(id)
                  case _ => None
                }
              })
          }.toSeq

          runManyTests(testScripts)
        }
        case RunnerMainConfig.RunMode.RunIncluding(ids) =>
          runManyTests(
            ids.map(scriptName =>
              Identifier(dar.main._1, QualifiedName.assertFromString(scriptName))
            )
          )
        case RunnerMainConfig.RunMode.RunSingle(scriptName, inputFile, outputFile) => {
          val scriptId: Identifier =
            Identifier(dar.main._1, QualifiedName.assertFromString(scriptName))
          val converter = (json: JsValue, typ: Type) =>
            Converter(majorVersion)
              .fromJsonValue(
                scriptId.qualifiedName,
                envIface,
                typ,
                json,
              )
              .flatMap(value =>
                // Use translator only to verify type, do not use translated value
                new ValueTranslator(
                  compiledPackages.pkgInterface,
                  forbidLocalContractIds = true,
                ).translateValue(typ, value).fold(err => Left(err.getMessage), _ => Right(value))
              )
          runScript(scriptId, inputFile, outputFile, Some(converter)).map(_ => true)
        }

      }
    } yield success

  def connectToParticipants(
      config: RunnerMainConfig,
      compiledPackages: PureCompiledPackages,
      traceLog: TraceLog,
      warningLog: WarningLog,
  )(implicit
      sequencer: ExecutionSequencerFactory,
      ec: ExecutionContext,
      traceContext: TraceContext,
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
                user_id = params.user_id.orElse(config.userId),
              )
            )
        connectApiParameters(config, params)
      case ParticipantMode.RemoteParticipantHost(host, port, oAdminPort) =>
        val params =
          Participants(
            default_participant = Some(ApiParameters(host, port, token, config.userId, oAdminPort)),
            participants = Map.empty,
            party_participants = Map.empty,
          )
        connectApiParameters(config, params)
      case ParticipantMode.IdeLedgerParticipant() =>
        Runner.ideLedgerClient(compiledPackages, traceLog, warningLog)
    }
  }

  def connectApiParameters(
      config: RunnerMainConfig,
      participantParams: Participants[ApiParameters],
  )(implicit
      sequencer: ExecutionSequencerFactory,
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): Future[Participants[ScriptLedgerClient]] =
    Runner.connect(participantParams, config.tlsConfig, config.maxInboundMessageSize)
}
