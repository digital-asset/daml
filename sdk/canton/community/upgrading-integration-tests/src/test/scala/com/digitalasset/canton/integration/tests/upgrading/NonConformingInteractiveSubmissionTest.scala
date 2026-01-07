// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrading

import com.daml.ledger.api.v2.interactive.interactive_submission_service.PrepareSubmissionResponse
import com.daml.ledger.javaapi.data.*
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.damltests.nonconforming
import com.digitalasset.canton.damltests.nonconforming.v1.java.nonconforming.BankTransfer
import com.digitalasset.canton.integration.ConfigTransforms.disableUpgradeValidation
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.protocol.{
  AuthenticatedContractIdVersionV11,
  ContractInstance,
  ExampleContractFactory,
}
import com.digitalasset.canton.topology.ExternalParty
import com.digitalasset.canton.{HasExecutionContext, LfPackageId, LfTimestamp}
import com.digitalasset.daml.lf.transaction.CreationTime.CreatedAt
import org.scalatest.OptionValues
import org.scalatest.matchers.should.Matchers

import java.util.Optional
import scala.jdk.CollectionConverters.SeqHasAsJava

class NonConformingInteractiveSubmissionTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with OptionValues
    with HasExecutionContext
    with Matchers {

  private var payer: ExternalParty = _
  private var payee: ExternalParty = _
  private var bank: ExternalParty = _

  private val mainIdentifier = new Identifier("#NonConformingX", "NonConformingX", "Main")

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .addConfigTransform(disableUpgradeValidation)
      .withSetup { implicit env =>
        import env.*
        participant1.synchronizers.connect_local(sequencer1, alias = daName)
        participant1.dars.upload(UpgradingBaseTest.NonConformingX)
        payer = participant1.parties.testing.external.enable("Payer")
        payee = participant1.parties.testing.external.enable("Payee")
        bank = participant1.parties.testing.external.enable("Bank")
      }

  "Interactive submission" should {

    "Detect a submission with inconsistent enrichment of latest contract" in { implicit env =>
      import env.*

      val mainContract = buildMain(participant1)
      val transferContract = createBankTransfer(participant1)

      assertThrowsAndLogsCommandFailures(
        prepareMainGet(participant1, transferContract, mainContract),
        _.errorMessage should include regex raw"(?s)INVALID_ARGUMENT/INTERPRETATION_UPGRADE_ERROR_AUTHENTICATION_FAILED.*Error when authenticating contract.*${transferContract.contractId.get()}",
      )
    }

    "Detect a submission with inconsistent enrichment of V11 contract" in { implicit env =>
      import env.*

      val mainContract = buildMain(participant1)
      val transferContract: DisclosedContract = createV11BankTransfer(participant1)

      assertThrowsAndLogsCommandFailures(
        prepareMainGet(participant1, transferContract, mainContract),
        _.errorMessage should include regex raw"(?s)INVALID_ARGUMENT/FAILED_TO_PREPARE_TRANSACTION.*${transferContract.contractId.get()}.*enriches to different values",
      )
    }
  }

  private def buildMain(participant1: => LocalParticipantReference) = {
    val mainTxCmd = new CreateCommand(
      mainIdentifier,
      new DamlRecord(
        Seq(new DamlRecord.Field(new Party(bank.toProtoPrimitive))).asJava
      ),
    )

    val mainTx = participant1.ledger_api.javaapi.commands.submit(
      Seq(bank),
      Seq(
        mainTxCmd
      ),
      includeCreatedEventBlob = true,
    )

    val mainContract: DisclosedContract =
      JavaDecodeUtil.decodeDisclosedContracts(mainTx).loneElement
    mainContract
  }

  private def createBankTransfer(participant: LocalParticipantReference) = {
    val packageId =
      LfPackageId.assertFromString(nonconforming.v1.java.nonconforming.BankTransfer.PACKAGE_ID)
    val bankTransferTx = participant.ledger_api.javaapi.commands.submit(
      Seq(bank),
      Seq(
        new BankTransfer(
          bank.toProtoPrimitive,
          payee.toProtoPrimitive,
          payer.toProtoPrimitive,
          100,
        ).create.commands.loneElement
      ),
      includeCreatedEventBlob = true,
      userPackageSelectionPreference = Seq(packageId),
    )
    JavaDecodeUtil.decodeDisclosedContracts(bankTransferTx).loneElement
  }

  private def createV11BankTransfer(participant: LocalParticipantReference) = {
    val latest = createBankTransfer(participant)

    val inst = ContractInstance.decode(latest.createdEventBlob).value

    val v11inst = ExampleContractFactory.fromCreate(
      inst.inst.toCreateNode,
      createdAt = CreatedAt(LfTimestamp.now()),
      cantonContractIdVersion = AuthenticatedContractIdVersionV11,
    )

    new DisclosedContract(
      v11inst.encoded,
      latest.synchronizerId.get(),
      latest.templateId,
      Optional.of(v11inst.contractId.coid),
    )
  }

  private def prepareMainGet(
      participant: LocalParticipantReference,
      transferContract: DisclosedContract,
      mainContract: DisclosedContract,
  ): PrepareSubmissionResponse = {

    val transferCidField = new DamlRecord.Field(new ContractId(transferContract.contractId.get()))

    val exCmd = new ExerciseCommand(
      mainIdentifier,
      mainContract.contractId.get(),
      "Main_Get",
      new DamlRecord(Seq(transferCidField, transferCidField).asJava),
    )

    participant.ledger_api.javaapi.interactive_submission.prepare(
      Seq(bank.partyId),
      Seq(exCmd),
      disclosedContracts = Seq(mainContract, transferContract),
    )

  }
}
