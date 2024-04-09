// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package model
package test

import com.daml.bazeltools.BazelRunfiles.rlocation
import com.daml.grpc.adapter.{ExecutionSequencerFactory, PekkoExecutionSequencerPool}
import com.daml.lf.archive.{Dar, UniversalArchiveDecoder}
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.engine.script.v2.ledgerinteraction.IdeLedgerClient
import com.daml.lf.engine.script.v2.ledgerinteraction.ScriptLedgerClient.SubmitFailure
import com.daml.lf.engine.script.v2.ledgerinteraction.grpcLedgerClient.{
  AdminLedgerClient,
  GrpcLedgerClient,
}
import com.daml.lf.language.{Ast, LanguageMajorVersion}
import com.daml.lf.model.test.Ledgers.Ledger
import com.daml.lf.speedy.{Compiler, RingBufferTraceLog, WarningLog}
import com.daml.logging.ContextualizedLogger
import com.digitalasset.canton.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientChannelConfiguration,
  LedgerClientConfiguration,
}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.google.protobuf.ByteString
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.scalacheck.Gen

import java.io.{File, FileInputStream}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

object Demo {

  private val universalDarPath: String = rlocation("daml-lf/model-test-lib/universal.dar")

  private val universalTemplateDar: Dar[(PackageId, Ast.Package)] =
    UniversalArchiveDecoder
      .assertReadFile(new File(universalDarPath))

  private val universalTemplatePkgId: Ref.PackageId =
    universalTemplateDar.main._1

  private val compiledPkgs: PureCompiledPackages =
    PureCompiledPackages.assertBuild(
      universalTemplateDar.all.toMap,
      Compiler.Config.Default(LanguageMajorVersion.V2),
    )

  private val ideLedgerClient: IdeLedgerClient = new IdeLedgerClient(
    originalCompiledPackages = compiledPkgs,
    traceLog = new RingBufferTraceLog(ContextualizedLogger.createFor("model.test.trace"), 1000),
    warningLog = new WarningLog(ContextualizedLogger.createFor("model.test.warnings")),
    canceled = () => false,
    namedLoggerFactory = NamedLoggerFactory("model.test", "model-based testing profile"),
  )

  private def makeGrpcLedgerClient()(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
  ): Future[GrpcLedgerClient] = {
    val clientChannelConfig = LedgerClientChannelConfiguration(
      sslContext = None
    )
    for {
      grpcClient <- com.digitalasset.canton.ledger.client.LedgerClient
        .singleHost(
          "localhost",
          5011,
          LedgerClientConfiguration(
            applicationId = "model-based-testing",
            commandClient = CommandClientConfiguration.default,
          ),
          clientChannelConfig,
          NamedLoggerFactory("model.test", ""),
        )
      _ <- Future.successful(
        grpcClient.packageManagementClient
          .uploadDarFile(ByteString.readFrom(new FileInputStream(universalDarPath)))
      )
    } yield new GrpcLedgerClient(
      grpcClient = grpcClient,
      applicationId = Some(Ref.ApplicationId.assertFromString("model-based-testing")),
      oAdminClient =
        Some(AdminLedgerClient.singleHost("localhost", 5012, None, clientChannelConfig)),
      enableContractUpgrading = false,
      compiledPackages = compiledPkgs,
    )
  }

  private def execute(interpreter: Interpreter, l: Ledger)(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Unit = {
    try {
      val result = Await.result(interpreter.runLedger(l), Duration.Inf)
      result match {
        case Right(value) =>
          println("SUCCESS")
          println(value)
        case Left(SubmitFailure(statusError, _)) =>
          println("ERROR")
          statusError match {
            case e: com.daml.lf.scenario.Error =>
              println(com.daml.lf.scenario.Pretty.prettyError(e).render(80))
            case _ => println(statusError)
          }
      }
    } catch {
      case e: Throwable =>
        println("EXCEPTION")
        e.printStackTrace(System.out)
    }
  }

  def main(args: Array[String]): Unit = {

    implicit val system: ActorSystem = ActorSystem("RunnerMain")
    implicit val ec: ExecutionContext = system.dispatcher
    implicit val materializer: Materializer = Materializer(system)
    implicit val sequencer: ExecutionSequencerFactory =
      new PekkoExecutionSequencerPool("ModelBasedTestingRunnerPool")(system)

    val grpcLedgerClient = Await.result(makeGrpcLedgerClient(), Duration.Inf)
    val ideInterpreter = new Interpreter(universalTemplatePkgId, ideLedgerClient)
    val cantonInterpreter = new Interpreter(universalTemplatePkgId, grpcLedgerClient)

    while (true) {
      Gen
        .resize(5, new Generators(3).ledgerGen)
        .sample
        .foreach(ledger => {
          println("==== ledger ====")
          println(Pretty.prettyLedger(ledger))
          println("==== IDE result ====")
          execute(ideInterpreter, ledger)
          println("==== Canton result ====")
          execute(cantonInterpreter, ledger)
        })
    }

    val _ = Await.ready(system.terminate(), Duration.Inf)
  }
}
