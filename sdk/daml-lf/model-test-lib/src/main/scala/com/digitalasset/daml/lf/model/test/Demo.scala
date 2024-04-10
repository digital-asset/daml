// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package model
package test

import com.daml.bazeltools.BazelRunfiles.rlocation
import com.daml.grpc.adapter.{ExecutionSequencerFactory, PekkoExecutionSequencerPool}
import com.daml.ledger.api.v2.{TransactionFilterOuterClass => proto}
import com.daml.ledger.javaapi
import com.daml.ledger.rxjava.DamlLedgerClient
import com.daml.lf.archive.{Dar, UniversalArchiveDecoder}
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.engine.script.v2.ledgerinteraction.IdeLedgerClient
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
import scala.jdk.CollectionConverters._

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

  private val ledgerClientForProjections: DamlLedgerClient =
    DamlLedgerClient.newBuilder("localhost", 5011).build()

  private def execute(interpreter: Interpreter, l: Ledger)(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Unit = {
    val (partyIds, result) = Await.result(interpreter.runLedger(l), Duration.Inf)
    result match {
      case Right(contractIds) =>
        // println(contractIds)
        for ((party, partyId) <- partyIds.toList) {
          println(s"Projection for party $party")
          // println(fetchEvents(partyId))
          println(
            Pretty.prettyProjection(
              ToProjection.convertFromCanton(
                partyIds.map(_.swap),
                contractIds.map(_.swap),
                fetchEvents(partyId),
              )
            )
          )
        }
      case Left(error) =>
        println("ERROR")
        println(error.pretty)
    }
  }

  def fetchEvents(party: Ref.Party): List[javaapi.data.TransactionTree] = {
    ledgerClientForProjections.getTransactionsClient
      .getTransactionsTrees(
        javaapi.data.ParticipantOffset.ParticipantBegin.getInstance(),
        javaapi.data.TransactionFilter.fromProto(
          proto.TransactionFilter
            .newBuilder()
            .putFiltersByParty(party, proto.Filters.getDefaultInstance)
            .build()
        ),
        true,
      )
      .blockingIterable
      .asScala
      .toList
  }

  def main(args: Array[String]): Unit = {

    implicit val system: ActorSystem = ActorSystem("RunnerMain")
    implicit val ec: ExecutionContext = system.dispatcher
    implicit val materializer: Materializer = Materializer(system)
    implicit val sequencer: ExecutionSequencerFactory =
      new PekkoExecutionSequencerPool("ModelBasedTestingRunnerPool")(system)

    val grpcLedgerClient = Await.result(makeGrpcLedgerClient(), Duration.Inf)
    val ideInterpreter = new Interpreter(universalTemplatePkgId, ideLedgerClient)
    print(ideInterpreter)
    val cantonInterpreter = new Interpreter(universalTemplatePkgId, grpcLedgerClient)
    ledgerClientForProjections.connect()

    val test: Ledgers.Ledger =
      List(
        Ledgers.Commands(
          actAs = Set(1, 2),
          actions = List(
            Ledgers.Create(contractId = 1, signatories = Set(1), observers = Set()),
            Ledgers.Create(contractId = 2, signatories = Set(2), observers = Set()),
          ),
        ),
        Ledgers.Commands(
          actAs = Set(2),
          actions = List(
            Ledgers.Create(contractId = 3, signatories = Set(2), observers = Set()),
            Ledgers.Exercise(
              contractId = 2,
              kind = Ledgers.NonConsuming,
              controllers = Set(2),
              choiceObservers = Set(),
              subTransaction = List(
                Ledgers.Exercise(
                  contractId = 1,
                  kind = Ledgers.NonConsuming,
                  controllers = Set(2),
                  choiceObservers = Set(),
                  subTransaction = List(),
                )
              ),
            ),
            Ledgers.Exercise(
              contractId = 2,
              kind = Ledgers.Consuming,
              controllers = Set(2),
              choiceObservers = Set(),
              subTransaction = List(
                Ledgers.Exercise(
                  contractId = 1,
                  kind = Ledgers.Consuming,
                  controllers = Set(2),
                  choiceObservers = Set(),
                  subTransaction = List(),
                )
              ),
            ),
          ),
        ),
      )

    while (true) {
      Gen
        .resize(5, new Generators(3).ledgerGen)
        .sample
      Some(test)
        .foreach(ledger => {
          if (ledger.nonEmpty) {
            Await.result(ideInterpreter.runLedger(ledger), Duration.Inf)._2 match {
              case Left(_) =>
                print(".")
              case Right(_) =>
                println("\n==== ledger ====")
                println(Pretty.prettyLedger(ledger))
                println("==== ide ledger ====")
                println(speedy.Pretty.prettyTransactions(ideLedgerClient.ledger).render(80))
                println("==== canton ====")
                execute(cantonInterpreter, ledger)
            }
          }
        })
    }

    ledgerClientForProjections.close()
    val _ = Await.ready(system.terminate(), Duration.Inf)
  }
}
