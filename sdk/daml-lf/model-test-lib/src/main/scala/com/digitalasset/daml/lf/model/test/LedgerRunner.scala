// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package model
package test

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.v2.{TransactionFilterOuterClass => proto}
import com.daml.ledger.javaapi
import com.daml.ledger.rxjava.DamlLedgerClient
import com.daml.lf.archive.{Dar, UniversalArchiveDecoder}
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.{PackageId, Party}
import com.daml.lf.engine.script.v2.ledgerinteraction.grpcLedgerClient.{
  AdminLedgerClient,
  GrpcLedgerClient,
}
import com.daml.lf.engine.script.v2.ledgerinteraction.{IdeLedgerClient, ScriptLedgerClient}
import com.daml.lf.language.{Ast, LanguageMajorVersion}
import com.daml.lf.model.test.Ledgers.Ledger
import com.daml.lf.model.test.Projections.{PartyId, Projection}
import com.daml.lf.model.test.ToProjection.{ContractIdReverseMapping, PartyIdReverseMapping}
import com.daml.lf.speedy.{Compiler, RingBufferTraceLog, WarningLog}
import com.daml.logging.ContextualizedLogger
import com.digitalasset.canton.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientChannelConfiguration,
  LedgerClientConfiguration,
}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.google.protobuf.ByteString
import org.apache.pekko.stream.Materializer

import java.io.{File, FileInputStream}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.CollectionConverters._

trait LedgerRunner {
  def runAndProject(
      ledger: Ledgers.Ledger
  ): Either[Interpreter.InterpreterError, Map[Projections.PartyId, Projections.Projection]]

  def close(): Unit
}

object LedgerRunner {
  def forCantonLedger(
      universalDarPath: String,
      host: String,
      ledgerApiPort: Int,
      adminApiPort: Int,
  )(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      materializer: Materializer,
  ): LedgerRunner =
    new CantonLedgerRunner(universalDarPath, host, ledgerApiPort, adminApiPort)

  def forIdeLedger(
      universalDarPath: String
  )(implicit ec: ExecutionContext, materializer: Materializer): LedgerRunner =
    new IdeLedgerRunner(universalDarPath)
}

private abstract class AbstractLedgerRunner(universalDarPath: String)(implicit
    ec: ExecutionContext,
    materializer: Materializer,
) extends LedgerRunner {

  // abstract members

  def ledgerClient: ScriptLedgerClient
  def project(
      reversePartyIds: ToProjection.PartyIdReverseMapping,
      reverseContractIds: ToProjection.ContractIdReverseMapping,
      party: Ref.Party,
  ): Projection

  // definitions

  private val universalTemplateDar: Dar[(PackageId, Ast.Package)] =
    UniversalArchiveDecoder
      .assertReadFile(new File(universalDarPath))

  protected val universalTemplatePkgId: Ref.PackageId =
    universalTemplateDar.main._1

  protected val compiledPkgs: PureCompiledPackages =
    PureCompiledPackages.assertBuild(
      universalTemplateDar.all.toMap,
      Compiler.Config.Default(LanguageMajorVersion.V2),
    )

  private lazy val interpreter = new Interpreter(universalTemplatePkgId, ledgerClient)

  // partial implementation of the parent trait

  override def runAndProject(
      ledger: Ledger
  ): Either[Interpreter.InterpreterError, Map[PartyId, Projection]] = {
    val (partyIds, result) = Await.result(interpreter.runLedger(ledger), Duration.Inf)
    result.map(contractIds => {
      val reversePartyIds = partyIds.map(_.swap)
      val reverseContractIds = contractIds.map(_.swap)
      for {
        (partyId, party) <- partyIds
      } yield partyId ->
        project(reversePartyIds, reverseContractIds, party)
    })
  }
}

private class CantonLedgerRunner(
    universalDarPath: String,
    host: String,
    ledgerApiPort: Int,
    adminApiPort: Int,
)(implicit
    ec: ExecutionContext,
    esf: ExecutionSequencerFactory,
    materializer: Materializer,
) extends AbstractLedgerRunner(universalDarPath) {

  override val ledgerClient: ScriptLedgerClient = Await.result(makeGprcLedgerClient(), Duration.Inf)
  private val ledgerClientForProjections: DamlLedgerClient =
    DamlLedgerClient.newBuilder("localhost", 5011).build()

  ledgerClientForProjections.connect()

  private def makeGprcLedgerClient(): Future[GrpcLedgerClient] = {
    val clientChannelConfig = LedgerClientChannelConfiguration(
      sslContext = None
    )
    for {
      grpcClient <- com.digitalasset.canton.ledger.client.LedgerClient
        .singleHost(
          host,
          ledgerApiPort,
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
    } yield {
      new GrpcLedgerClient(
        grpcClient = grpcClient,
        applicationId = Some(Ref.ApplicationId.assertFromString("model-based-testing")),
        oAdminClient =
          Some(AdminLedgerClient.singleHost(host, adminApiPort, None, clientChannelConfig)),
        enableContractUpgrading = false,
        compiledPackages = compiledPkgs,
      )
    }
  }

  private def fetchEvents(party: Ref.Party): List[javaapi.data.TransactionTree] = {
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

  override def project(
      reversePartyIds: ToProjection.PartyIdReverseMapping,
      reverseContractIds: ToProjection.ContractIdReverseMapping,
      party: Party,
  ): Projection = {
    ToProjection.convertFromCantonProjection(
      reversePartyIds,
      reverseContractIds,
      fetchEvents(party),
    )
  }

  override def close(): Unit =
    ledgerClientForProjections.close()
}

private class IdeLedgerRunner(universalDarPath: String)(implicit
    ec: ExecutionContext,
    materializer: Materializer,
) extends AbstractLedgerRunner(universalDarPath) {

  override val ledgerClient: IdeLedgerClient = {
    new IdeLedgerClient(
      originalCompiledPackages = compiledPkgs,
      traceLog = new RingBufferTraceLog(ContextualizedLogger.createFor("model.test.trace"), 1000),
      warningLog = new WarningLog(ContextualizedLogger.createFor("model.test.warnings")),
      canceled = () => false,
      namedLoggerFactory = NamedLoggerFactory("model.test", "model-based testing profile"),
    )
  }

  override def project(
      reversePartyIds: PartyIdReverseMapping,
      reverseContractIds: ContractIdReverseMapping,
      party: Party,
  ): Projection =
    ToProjection.projectFromScenarioLedger(
      reversePartyIds,
      reverseContractIds,
      ledgerClient.ledger,
      party,
    )

  override def close(): Unit = ()
}
