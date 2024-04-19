// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package model
package test

import cats.implicits.toTraverseOps
import cats.instances.all._
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
import com.daml.lf.model.test.LedgerImplicits._
import com.daml.lf.model.test.LedgerRunner.ApiPorts
import com.daml.lf.model.test.Ledgers.{ParticipantId, Scenario}
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
      scenario: Scenario
  ): Either[Interpreter.InterpreterError, Map[Projections.PartyId, Projections.Projection]]

  def close(): Unit
}

object LedgerRunner {

  case class ApiPorts(ledgerApiPort: Int, adminApiPort: Int)

  def forCantonLedger(
      universalDarPath: String,
      host: String,
      apiPorts: Iterable[ApiPorts],
  )(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      materializer: Materializer,
  ): LedgerRunner =
    new CantonLedgerRunner(universalDarPath, host, apiPorts)

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

  def ledgerClients: PartialFunction[ParticipantId, ScriptLedgerClient]

  def replicateParties(
      replications: Map[Ref.Party, (ParticipantId, Set[ParticipantId])]
  ): Future[Unit]

  def project(
      reversePartyIds: ToProjection.PartyIdReverseMapping,
      reverseContractIds: ToProjection.ContractIdReverseMapping,
      participantId: ParticipantId,
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

  private lazy val interpreter =
    new Interpreter(universalTemplatePkgId, ledgerClients, replicateParties)

  // partial implementation of the parent trait

  override def runAndProject(
      scenario: Scenario
  ): Either[Interpreter.InterpreterError, Map[PartyId, Projection]] = {
    val (partyIds, result) = Await.result(interpreter.runLedger(scenario), Duration.Inf)
    result.map(contractIds => {
      val reversePartyIds = partyIds.map(_.swap)
      val reverseContractIds = contractIds.map(_.swap)
      // we for now assume there is exactly one participant per party
      val reverseParticipantIds =
        scenario.topology.groupedByPartyId.view.mapValues(_.head.participantId)
      for {
        (partyId, party) <- partyIds
      } yield partyId ->
        project(reversePartyIds, reverseContractIds, reverseParticipantIds(partyId), party)
    })
  }
}

private class CantonLedgerRunner(
    universalDarPath: String,
    host: String,
    apiPorts: Iterable[LedgerRunner.ApiPorts],
)(implicit
    ec: ExecutionContext,
    esf: ExecutionSequencerFactory,
    materializer: Materializer,
) extends AbstractLedgerRunner(universalDarPath) {

  override def ledgerClients: Seq[ScriptLedgerClient] =
    Await.result(apiPorts.toSeq.traverse(makeGprcLedgerClient), Duration.Inf)

  private val ledgerClientsForProjections: Seq[DamlLedgerClient] =
    apiPorts
      .map(ports => DamlLedgerClient.newBuilder("localhost", ports.ledgerApiPort).build())
      .toSeq

  ledgerClientsForProjections.foreach(_.connect())

  private def makeGprcLedgerClient(apiPorts: ApiPorts): Future[GrpcLedgerClient] = {
    val clientChannelConfig = LedgerClientChannelConfiguration(
      sslContext = None
    )
    for {
      grpcClient <- com.digitalasset.canton.ledger.client.LedgerClient
        .singleHost(
          host,
          apiPorts.ledgerApiPort,
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
        oAdminClient = Some(
          AdminLedgerClient.singleHost(host, apiPorts.adminApiPort, None, clientChannelConfig)
        ),
        enableContractUpgrading = false,
        compiledPackages = compiledPkgs,
      )
    }
  }

  private def fetchEvents(
      participantId: ParticipantId,
      party: Ref.Party,
  ): List[javaapi.data.TransactionTree] = {
    ledgerClientsForProjections(participantId).getTransactionsClient
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
      participantId: ParticipantId,
      party: Party,
  ): Projection = {
    ToProjection.convertFromCantonProjection(
      reversePartyIds,
      reverseContractIds,
      fetchEvents(participantId, party),
    )
  }

  private lazy val cantonConfig = {
    def participant(ports: ApiPorts, n: Int): String =
      s"""
         |participant$n {
         |    ledger-api {
         |        port = ${ports.ledgerApiPort}
         |    }
         |    admin-api {
         |        port = ${ports.adminApiPort}
         |    }
         |}
         |""".stripMargin

    s"""
      |canton {
      |    remote-participants {
      |        ${apiPorts.zipWithIndex.map((participant _).tupled).mkString("\n")}
      |    }
      |}
      |""".stripMargin
  }

  override def replicateParties(
      replications: Map[Party, (ParticipantId, Set[ParticipantId])]
  ): Future[Unit] = {

    def partyLiteral(party: Party): String =
      s"""PartyId.tryFromProtoPrimitive("${party}")"""

    def participantLiteral(participantId: ParticipantId): String =
      s"participant${participantId}"

    def participantSetLiteral(participantIds: Set[ParticipantId]): String =
      participantIds.map(participantLiteral).mkString("Set(", ", ", ")")

    def entryLiteral(entry: (Party, (ParticipantId, Set[ParticipantId]))): String = {
      val (party, (participantFrom, participantTo)) = entry
      s"${partyLiteral(party)} -> (${participantLiteral(participantFrom)}, ${participantSetLiteral(participantTo)})"
    }

    val replicationsLiteral = replications.map(entryLiteral).mkString("Map(", ", ", ")")

    val script =
      s"""
         |import com.digitalasset.canton.console.ParticipantReference
         |
         |val domainId = participant1.domains.list_connected.head.domainId
         |val replications = $replicationsLiteral
         |
         |def replicate(party: PartyId, participantFrom: ParticipantReference, participantTo: ParticipantReference) = {
         |  println("replicating " + party + " from " + participantFrom + " to " + participantTo)
         |  Seq(participantFrom, participantTo).foreach(p =>
         |    p.topology.party_to_participant_mappings
         |      .propose_delta(
         |        party,
         |        adds = List((participantTo.id, ParticipantPermission.Submission)),
         |        store = domainId.filterString,
         |      )
         |  )
         |}
         |
         |replications.foreach { case (party, (participantFrom, participantsTo)) =>
         |  participantsTo.foreach { participantTo =>
         |    replicate(party, participantFrom, participantTo)
         |  }
         |}
         |""".stripMargin

    Future.successful(
      CantonConsole.run(cantonConfig, script, debug = true)
    )
  }

  override def close(): Unit =
    ledgerClientsForProjections.foreach(_.close())
}

private class IdeLedgerRunner(universalDarPath: String)(implicit
    ec: ExecutionContext,
    materializer: Materializer,
) extends AbstractLedgerRunner(universalDarPath) {

  private val ledgerClient: IdeLedgerClient = {
    new IdeLedgerClient(
      originalCompiledPackages = compiledPkgs,
      traceLog = new RingBufferTraceLog(ContextualizedLogger.createFor("model.test.trace"), 1000),
      warningLog = new WarningLog(ContextualizedLogger.createFor("model.test.warnings")),
      canceled = () => false,
      namedLoggerFactory = NamedLoggerFactory("model.test", "model-based testing profile"),
    )
  }

  override def ledgerClients: PartialFunction[ParticipantId, ScriptLedgerClient] = { case _ =>
    ledgerClient
  }

  override def project(
      reversePartyIds: PartyIdReverseMapping,
      reverseContractIds: ContractIdReverseMapping,
      participantId: ParticipantId,
      party: Party,
  ): Projection =
    ToProjection.projectFromScenarioLedger(
      reversePartyIds,
      reverseContractIds,
      ledgerClient.ledger,
      party,
    )

  override def replicateParties(
      replications: Map[Party, (ParticipantId, Set[ParticipantId])]
  ): Future[Unit] =
    Future.successful(())

  override def close(): Unit = ()
}
