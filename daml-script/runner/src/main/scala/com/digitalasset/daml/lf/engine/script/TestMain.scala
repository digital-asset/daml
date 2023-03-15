// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script

import java.io.FileInputStream
import java.util.concurrent.atomic.AtomicBoolean

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.daml.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.lf.PureCompiledPackages
import com.daml.lf.archive.{Dar, DarDecoder}
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.{Identifier, PackageId, QualifiedName}
import com.daml.lf.language.Ast.Package
import com.google.protobuf.ByteString
import com.typesafe.scalalogging.StrictLogging
import spray.json._

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.io.Source
import scala.util.{Failure, Success}
import com.daml.lf.speedy.{Speedy, TraceLog, WarningLog}
import com.daml.lf.engine.script.ledgerinteraction.IdeLedgerClient
import com.daml.lf.engine.script.ledgerinteraction.GrpcLedgerClient
import com.daml.lf.engine.script.ledgerinteraction.ScriptLedgerClient

object TestMain extends StrictLogging {

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

  def connectApiParticipant(
      config: TestConfig,
      participantParams: Participants[ApiParameters],
  )(implicit
      executionContext: ExecutionContext,
      sequencer: ExecutionSequencerFactory,
  ): Future[Participants[GrpcLedgerClient]] = {
    for {
      clients <- Runner.connect(
        participantParams,
        TlsConfiguration(false, None, None, None),
        config.maxInboundMessageSize,
      )
      _ <- clients.getParticipant(None) match {
        case Left(err) => throw new RuntimeException(err)
        case Right(client) =>
          client.grpcClient.packageManagementClient
            .uploadDarFile(ByteString.readFrom(new FileInputStream(config.darPath)))
      }
    } yield clients
  }

  def main(config: TestConfig): Unit = {

    val dar: Dar[(PackageId, Package)] = DarDecoder.assertReadArchiveFromFile(config.darPath)

    val system: ActorSystem = ActorSystem("ScriptTest")
    implicit val sequencer: ExecutionSequencerFactory =
      new AkkaExecutionSequencerPool("ScriptTestPool")(system)
    implicit val materializer: Materializer = Materializer(system)
    implicit val executionContext: ExecutionContext = system.dispatcher

    val darMap = dar.all.toMap
    val compiledPackages = PureCompiledPackages.assertBuild(darMap, Runner.compilerConfig)
    val traceLog: TraceLog = Speedy.Machine.newTraceLog
    val warningLog: WarningLog = Speedy.Machine.newWarningLog

    val eParticipantParams: Either[Participants[ApiParameters], Participants[IdeLedgerClient]] =
      config.ledgerMode match {
        case LedgerMode.ParticipantConfig(file) =>
          val source = Source.fromFile(file)
          val fileContent =
            try {
              source.mkString
            } finally {
              source.close
            }
          val jsVal = fileContent.parseJson
          import ParticipantsJsonProtocol._
          Left(jsVal.convertTo[Participants[ApiParameters]])
        case LedgerMode.LedgerAddress(host, port) =>
          Left(
            Participants(
              default_participant = Some(ApiParameters(host, port, None, None)),
              participants = Map.empty,
              party_participants = Map.empty,
            )
          )
        case LedgerMode.IdeLedger() =>
          Right(
            Participants(
              default_participant =
                Some(new IdeLedgerClient(compiledPackages, traceLog, warningLog, () => false)),
              participants = Map.empty,
              party_participants = Map.empty,
            )
          )
      }

    val testScripts: Map[Ref.Identifier, Script.Action] = dar.main._2.modules.flatMap {
      case (moduleName, module) =>
        module.definitions.collect(Function.unlift { case (name, _) =>
          val id = Identifier(dar.main._1, QualifiedName(moduleName, name))
          Script.fromIdentifier(compiledPackages, id) match {
            // We exclude generated identifiers starting with `$`.
            case Right(script: Script.Action) if !name.dottedName.startsWith("$") =>
              Some((id, script))
            case _ => None
          }
        })
    }

    val flow: Future[Boolean] = for {
      clients: Participants[ScriptLedgerClient] <- (eParticipantParams match {
        case Left(participantParams) => connectApiParticipant(config, participantParams)
        case Right(ideLedgerParticipant) => Future.successful(ideLedgerParticipant)
      })

      success = new AtomicBoolean(true)
      // Sort in case scripts depend on each other.
      _ <- sequentialTraverse(testScripts.toList.sortBy({ case (id, _) => id })) {
        case (id, script) =>
          val runner =
            new Runner(compiledPackages, script, config.timeMode)
          val testRun: Future[Unit] =
            runner.runWithClients(clients, traceLog, warningLog)._2.map(_ => ())
          // Print test result and remember failure.
          testRun
            .andThen {
              case Failure(exception) =>
                success.set(false)
                println(s"${id.qualifiedName} FAILURE ($exception)")
              case Success(_) =>
                println(s"${id.qualifiedName} SUCCESS")
            }
            .recover { case _ =>
              // Do not abort in case of failure, but complete all test runs.
              ()
            }
      }
    } yield success.get()

    flow.onComplete { _ =>
      system.terminate()
    }

    if (!Await.result(flow, Duration.Inf)) {
      sys.exit(1)
    }
  }
}
