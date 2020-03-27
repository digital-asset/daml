// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine.trigger.test

import java.io.File
import java.util.UUID

import akka.stream.scaladsl.{Flow, Sink}
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.daml.bazeltools.BazelRunfiles._
import com.digitalasset.daml.lf.PureCompiledPackages
import com.digitalasset.daml.lf.archive.{DarReader, Decode}
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.speedy.SExpr
import com.digitalasset.daml.lf.speedy.SExpr._
import com.digitalasset.daml.lf.speedy.SValue._
import com.digitalasset.daml.lf.value.Value.AbsoluteContractId
import com.digitalasset.dec.DirectExecutionContext
import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.testing.utils.{MockMessages, SuiteResourceManagementAroundAll}
import com.digitalasset.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.digitalasset.ledger.api.v1.commands._
import com.digitalasset.ledger.api.v1.commands.{Command, CreateCommand, ExerciseCommand}
import com.digitalasset.ledger.api.v1.event.CreatedEvent
import com.digitalasset.ledger.api.refinements.ApiTypes.ApplicationId
import com.digitalasset.ledger.api.v1.testing.time_service.TimeServiceGrpc
import com.digitalasset.ledger.api.v1.transaction_filter.{Filters, TransactionFilter}
import com.digitalasset.ledger.api.v1.{value => LedgerApi}
import com.digitalasset.ledger.client.LedgerClient
import com.digitalasset.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement
}
import com.digitalasset.ledger.client.services.testing.time.StaticTime
import com.digitalasset.platform.sandbox.services.{SandboxFixture, TestCommands}
import com.digitalasset.platform.services.time.TimeProviderType
import org.scalatest._
import scala.concurrent.Future
import scala.util.control.NonFatal
import scalaz.syntax.tag._
import scalaz.syntax.traverse._

import com.digitalasset.daml.lf.engine.trigger.{Runner, Trigger, TriggerMsg}

@SuppressWarnings(Array("org.wartremover.warts.Any"))
final class TriggerIt
    extends AsyncWordSpec
    with TestCommands
    with SandboxFixture
    with Matchers
    with SuiteResourceManagementAroundAll
    with TryValues {

  private val ledgerClientConfiguration =
    LedgerClientConfiguration(
      applicationId = MockMessages.applicationId,
      ledgerIdRequirement = LedgerIdRequirement("", enabled = false),
      commandClient = CommandClientConfiguration.default,
      sslContext = None,
      token = None
    )

  private def timeProvider(ledgerId: domain.LedgerId): Future[TimeProvider] = {
    StaticTime
      .updatedVia(TimeServiceGrpc.stub(channel), ledgerId.unwrap)
      .recover { case NonFatal(_) => TimeProvider.UTC }(DirectExecutionContext)
  }

  private def ledgerClient(): Future[LedgerClient] =
    LedgerClient.singleHost("localhost", serverPort.value, ledgerClientConfiguration)

  override protected def darFile = new File(rlocation("triggers/tests/acs.dar"))

  private def dar = DarReader().readArchiveFromFile(darFile).get.map {
    case (pkgId, archive) => Decode.readArchivePayload(pkgId, archive)
  }
  private def compiledPackages = PureCompiledPackages(dar.all.toMap).right.get

  private def getRunner(client: LedgerClient, name: QualifiedName, party: String): Runner = {
    val triggerId = Identifier(packageId, name)
    val trigger = Trigger.fromIdentifier(compiledPackages, triggerId).right.get
    new Runner(
      compiledPackages,
      trigger,
      client,
      TimeProviderType.WallClock,
      ApplicationId(MockMessages.applicationId),
      party)
  }

  private def allocateParty(client: LedgerClient): Future[String] =
    client.partyManagementClient.allocateParty(None, None).map(_.party)

  private def create(client: LedgerClient, party: String, cmd: CreateCommand): Future[String] = {
    val commands = Seq(Command().withCreate(cmd))
    val request = SubmitAndWaitRequest(
      Some(
        Commands(
          party = party,
          commands = commands,
          ledgerId = client.ledgerId.unwrap,
          applicationId = MockMessages.applicationId,
          commandId = UUID.randomUUID.toString
        )))
    for {
      response <- client.commandServiceClient.submitAndWaitForTransaction(request)
    } yield response.getTransaction.events.head.getCreated.contractId
  }

  private def archive(
      client: LedgerClient,
      party: String,
      templateId: LedgerApi.Identifier,
      contractId: String): Future[Unit] = {
    val commands = Seq(
      Command().withExercise(
        ExerciseCommand(
          templateId = Some(templateId),
          contractId = contractId,
          choice = "Archive",
          choiceArgument = Some(LedgerApi.Value().withRecord(LedgerApi.Record())))))
    val request = SubmitAndWaitRequest(
      Some(
        Commands(
          party = party,
          commands = commands,
          ledgerId = client.ledgerId.unwrap,
          applicationId = MockMessages.applicationId,
          commandId = UUID.randomUUID.toString
        )))
    for {
      response <- client.commandServiceClient.submitAndWaitForTransaction(request)
    } yield ()
  }

  private def queryACS(client: LedgerClient, party: String) = {
    val filter = TransactionFilter(List((party, Filters.defaultInstance)).toMap)
    val contractsF: Future[Seq[CreatedEvent]] = client.activeContractSetClient
      .getActiveContracts(filter, verbose = true)
      .runWith(Sink.seq)
      .map(_.flatMap(x => x.activeContracts))
    contractsF.map(contracts =>
      contracts.map(created => (created.getTemplateId, created.getCreateArguments)).groupBy(_._1))
  }

  "Trigger" can {
    "AcsTests" should {
      val assetId = LedgerApi.Identifier(packageId, "ACS", "Asset")
      val assetMirrorId = LedgerApi.Identifier(packageId, "ACS", "AssetMirror")
      def asset(party: String): CreateCommand =
        CreateCommand(
          templateId = Some(assetId),
          createArguments = Some(
            LedgerApi.Record(fields =
              Seq(LedgerApi.RecordField("issuer", Some(LedgerApi.Value().withParty(party)))))))

      final case class AssetResult(
          successfulCompletions: Long,
          failedCompletions: Long,
          activeAssets: Set[String])

      def toResult(expr: SExpr): AssetResult = {
        val fields = expr.asInstanceOf[SEValue].v.asInstanceOf[SRecord].values
        AssetResult(
          successfulCompletions = fields.get(1).asInstanceOf[SInt64].value,
          failedCompletions = fields.get(2).asInstanceOf[SInt64].value,
          activeAssets = fields
            .get(0)
            .asInstanceOf[SList]
            .list
            .map(x =>
              x.asInstanceOf[SContractId].value.asInstanceOf[AbsoluteContractId].coid.toString)
            .toSet
        )
      }

      "1 create" in {
        for {
          client <- ledgerClient()
          party <- allocateParty(client)
          runner = getRunner(client, QualifiedName.assertFromString("ACS:test"), party)
          (acs, offset) <- runner.queryACS()
          // Start the future here
          finalStateF = runner.runWithACS(acs, offset, msgFlow = Flow[TriggerMsg].take(6))._2
          // Execute commands
          contractId <- create(client, party, asset(party))
          // Wait for the trigger to terminate
          result <- finalStateF.map(toResult)
          acs <- queryACS(client, party)
        } yield {
          assert(result.activeAssets == Seq(contractId).toSet)
          assert(result.successfulCompletions == 2)
          assert(result.failedCompletions == 0)
          assert(acs(assetMirrorId).size == 1)
        }
      }

      "2 creates" in {
        for {
          client <- ledgerClient()
          party <- allocateParty(client)
          runner = getRunner(client, QualifiedName.assertFromString("ACS:test"), party)
          (acs, offset) <- runner.queryACS()

          finalStateF = runner.runWithACS(acs, offset, msgFlow = Flow[TriggerMsg].take(12))._2

          contractId1 <- create(client, party, asset(party))
          contractId2 <- create(client, party, asset(party))

          result <- finalStateF.map(toResult)
          acs <- queryACS(client, party)
        } yield {
          assert(result.activeAssets == Seq(contractId1, contractId2).toSet)
          assert(result.successfulCompletions == 4)
          assert(result.failedCompletions == 0)
          assert(acs(assetMirrorId).size == 2)
        }
      }

      "2 creates and 2 archives" in {
        for {
          client <- ledgerClient()
          party <- allocateParty(client)
          runner = getRunner(client, QualifiedName.assertFromString("ACS:test"), party)
          (acs, offset) <- runner.queryACS()

          finalStateF = runner.runWithACS(acs, offset, msgFlow = Flow[TriggerMsg].take(16))._2

          contractId1 <- create(client, party, asset(party))
          contractId2 <- create(client, party, asset(party))
          _ <- archive(client, party, assetId, contractId1)
          _ <- archive(client, party, assetId, contractId2)

          result <- finalStateF.map(toResult)
          acs <- queryACS(client, party)
        } yield {
          assert(result.activeAssets == Seq().toSet)
          assert(result.successfulCompletions == 4)
          assert(result.failedCompletions == 0)
          assert(acs(assetMirrorId).size == 2)
        }
      }
    }
  }
}
