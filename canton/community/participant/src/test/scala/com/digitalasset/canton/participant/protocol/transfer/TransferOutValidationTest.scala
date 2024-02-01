// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.transfer

import cats.data.EitherT
import com.digitalasset.canton.*
import com.digitalasset.canton.data.{CantonTimestamp, TransferSubmitterMetadata}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.protocol.submission.SeedGenerator
import com.digitalasset.canton.participant.protocol.transfer.TransferProcessingSteps.{
  StakeholdersMismatch,
  TemplateIdMismatch,
  TransferProcessorError,
}
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.sequencing.protocol.Recipients
import com.digitalasset.canton.time.TimeProofTestUtil
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.transaction.{ParticipantPermission, VettedPackages}
import com.digitalasset.canton.version.Transfer.{SourceProtocolVersion, TargetProtocolVersion}
import org.scalatest.wordspec.AsyncWordSpec

import java.util.UUID

class TransferOutValidationTest
    extends AsyncWordSpec
    with BaseTest
    with ProtocolVersionChecksAsyncWordSpec {
  private val sourceDomain = SourceDomainId(
    DomainId.tryFromString("domain::source")
  )
  private val sourceMediator = MediatorId(
    UniqueIdentifier.tryFromProtoPrimitive("mediator::source")
  )
  private val targetDomain = TargetDomainId(
    DomainId(UniqueIdentifier.tryFromProtoPrimitive("domain::target"))
  )

  private val submitterParty1: LfPartyId = LfPartyId.assertFromString("submitterParty::party")

  private val receiverParty2: LfPartyId = PartyId(
    UniqueIdentifier.tryFromProtoPrimitive("receiverParty2::party")
  ).toLf

  private val participant = ParticipantId.tryFromProtoPrimitive("PAR::bothdomains::participant")

  private val initialTransferCounter: TransferCounterO =
    Some(TransferCounter.Genesis)

  private def submitterInfo(submitter: LfPartyId): TransferSubmitterMetadata = {
    TransferSubmitterMetadata(
      submitter,
      participant,
      DefaultDamlValues.lfCommandId(),
      submissionId = None,
      DefaultDamlValues.lfApplicationId(),
      workflowId = None,
    )
  }

  val contractId = ExampleTransactionFactory.suffixedId(10, 0)

  val transferId = TransferId(sourceDomain, CantonTimestamp.Epoch)
  val uuid = new UUID(3L, 4L)
  private val pureCrypto = TestingIdentityFactory.pureCrypto()
  private val seedGenerator = new SeedGenerator(pureCrypto)
  val seed = seedGenerator.generateSaltSeed()

  private val templateId =
    LfTemplateId.assertFromString("transferoutvalidationtestpackage:template:id")

  val wrongTemplateId =
    LfTemplateId.assertFromString("transferoutvalidatoionpackage:wrongtemplate:id")

  val contract = ExampleTransactionFactory.asSerializable(
    contractId,
    contractInstance = ExampleTransactionFactory.contractInstance(templateId = templateId),
    metadata = ContractMetadata.tryCreate(
      signatories = Set(submitterParty1),
      stakeholders = Set(submitterParty1),
      maybeKeyWithMaintainers = None,
    ),
  )

  private val identityFactory = TestingTopology()
    .withDomains(sourceDomain.unwrap)
    .withReversedTopology(
      Map(
        participant -> Map(
          submitterParty1 -> ParticipantPermission.Submission,
          receiverParty2 -> ParticipantPermission.Submission,
        )
      )
    )
    .withSimpleParticipants(participant) // required such that `participant` gets a signing key
    .withPackages(
      Seq(VettedPackages(participant, Seq(templateId.packageId, wrongTemplateId.packageId)))
    )
    .build(loggerFactory)

  val stakeholders = Set(submitterParty1)
  val sourcePV = SourceProtocolVersion(testedProtocolVersion)
  val targetPV = TargetProtocolVersion(testedProtocolVersion)

  "transfer out validation" should {
    "succeed without errors" in {
      val validation = mkTransferOutValidation(
        stakeholders,
        sourcePV,
        templateId,
        initialTransferCounter,
      )
      for {
        _ <- validation.valueOrFailShutdown("validation failed")
      } yield succeed
    }
  }

  "detect stakeholders mismatch" in {
    // receiverParty2 is not a stakeholder on a contract, but it is listed as stakeholder here
    val validation = mkTransferOutValidation(
      stakeholders.union(Set(receiverParty2)),
      sourcePV,
      templateId,
      initialTransferCounter,
    )
    for {
      res <- validation.leftOrFailShutdown("couldn't get left from transfer out validation")
    } yield {
      res shouldBe StakeholdersMismatch(
        None,
        Set(submitterParty1, receiverParty2),
        None,
        Right(Set(submitterParty1)),
      )
    }
  }

  "detect template id mismatch" in {
    // template id does not match the one in the contract
    val validation =
      mkTransferOutValidation(stakeholders, sourcePV, wrongTemplateId, initialTransferCounter).value
    for {
      res <- validation.failOnShutdown
    } yield {
      res shouldBe Left(TemplateIdMismatch(templateId.leftSide, wrongTemplateId.leftSide))
    }
  }

  private def mkTransferOutValidation(
      newStakeholders: Set[LfPartyId],
      sourceProtocolVersion: SourceProtocolVersion,
      expectedTemplateId: LfTemplateId,
      transferCounter: TransferCounterO,
  ): EitherT[FutureUnlessShutdown, TransferProcessorError, Unit] = {
    val transferOutRequest = TransferOutRequest(
      submitterInfo(submitterParty1),
      // receiverParty2 is not a stakeholder on a contract, but it is listed as stakeholder here
      newStakeholders,
      Set(participant.adminParty.toLf),
      ExampleTransactionFactory.transactionId(0),
      contract,
      transferId.sourceDomain,
      sourceProtocolVersion,
      MediatorRef(sourceMediator),
      targetDomain,
      targetPV,
      TimeProofTestUtil.mkTimeProof(timestamp = CantonTimestamp.Epoch, targetDomain = targetDomain),
      transferCounter,
    )
    val fullTransferOutTree = transferOutRequest
      .toFullTransferOutTree(
        pureCrypto,
        pureCrypto,
        seed,
        uuid,
      )

    TransferOutValidation(
      fullTransferOutTree,
      stakeholders,
      expectedTemplateId,
      sourceProtocolVersion,
      identityFactory.topologySnapshot(),
      Some(identityFactory.topologySnapshot()),
      Recipients.cc(participant),
      logger,
    )
  }
}
