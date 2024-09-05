// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.transfer

import cats.data.EitherT
import com.digitalasset.canton.*
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.data.{CantonTimestamp, TransferSubmitterMetadata}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.protocol.submission.SeedGenerator
import com.digitalasset.canton.participant.protocol.transfer.ReassignmentProcessingSteps.{
  ReassignmentProcessorError,
  StakeholdersMismatch,
  TemplateIdMismatch,
}
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.sequencing.protocol.{MediatorGroupRecipient, Recipients}
import com.digitalasset.canton.time.TimeProofTestUtil
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.version.Transfer.{SourceProtocolVersion, TargetProtocolVersion}
import org.scalatest.wordspec.AsyncWordSpec

import java.util.UUID

// TODO(#21081) Check file names, test names and packages
class UnassignmentValidationTest
    extends AsyncWordSpec
    with BaseTest
    with ProtocolVersionChecksAsyncWordSpec {
  private val sourceDomain = SourceDomainId(
    DomainId.tryFromString("domain::source")
  )
  private val sourceMediator = MediatorGroupRecipient(MediatorGroupIndex.tryCreate(100))
  private val targetDomain = TargetDomainId(
    DomainId(UniqueIdentifier.tryFromProtoPrimitive("domain::target"))
  )

  private val submitterParty1: LfPartyId = LfPartyId.assertFromString("submitterParty::party")

  private val receiverParty2: LfPartyId = PartyId(
    UniqueIdentifier.tryFromProtoPrimitive("receiverParty2::party")
  ).toLf

  private val participant = ParticipantId.tryFromProtoPrimitive("PAR::bothdomains::participant")

  private val initialReassignmentCounter: ReassignmentCounter = ReassignmentCounter.Genesis

  private def submitterInfo(submitter: LfPartyId): TransferSubmitterMetadata =
    TransferSubmitterMetadata(
      submitter,
      participant,
      DefaultDamlValues.lfCommandId(),
      submissionId = None,
      DefaultDamlValues.lfApplicationId(),
      workflowId = None,
    )

  val contractId = ExampleTransactionFactory.suffixedId(10, 0)

  val reassignmentId = ReassignmentId(sourceDomain, CantonTimestamp.Epoch)
  val uuid = new UUID(3L, 4L)
  private val pureCrypto = new SymbolicPureCrypto
  private val seedGenerator = new SeedGenerator(pureCrypto)
  val seed = seedGenerator.generateSaltSeed()

  private val templateId =
    LfTemplateId.assertFromString("unassignmentvalidationtestpackage:template:id")

  val wrongTemplateId =
    LfTemplateId.assertFromString("unassignmentvalidatoionpackage:wrongtemplate:id")

  val contract = ExampleTransactionFactory.asSerializable(
    contractId,
    contractInstance = ExampleTransactionFactory.contractInstance(templateId = templateId),
    metadata = ContractMetadata.tryCreate(
      signatories = Set(submitterParty1),
      stakeholders = Set(submitterParty1),
      maybeKeyWithMaintainersVersioned = None,
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
      Map(participant -> Seq(templateId.packageId, wrongTemplateId.packageId))
    )
    .build(loggerFactory)

  val stakeholders = Set(submitterParty1)
  val sourcePV = SourceProtocolVersion(testedProtocolVersion)
  val targetPV = TargetProtocolVersion(testedProtocolVersion)

  "transfer out validation" should {
    "succeed without errors" in {
      val validation = mkUnassignmentValidation(
        stakeholders,
        sourcePV,
        templateId,
        initialReassignmentCounter,
      )
      for {
        _ <- validation.valueOrFailShutdown("validation failed")
      } yield succeed
    }
  }

  "detect stakeholders mismatch" in {
    // receiverParty2 is not a stakeholder on a contract, but it is listed as stakeholder here
    val validation = mkUnassignmentValidation(
      stakeholders.union(Set(receiverParty2)),
      sourcePV,
      templateId,
      initialReassignmentCounter,
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
      mkUnassignmentValidation(
        stakeholders,
        sourcePV,
        wrongTemplateId,
        initialReassignmentCounter,
      ).value
    for {
      res <- validation.failOnShutdown
    } yield {
      res shouldBe Left(TemplateIdMismatch(templateId.leftSide, wrongTemplateId.leftSide))
    }
  }

  private def mkUnassignmentValidation(
      newStakeholders: Set[LfPartyId],
      sourceProtocolVersion: SourceProtocolVersion,
      expectedTemplateId: LfTemplateId,
      reassignmentCounter: ReassignmentCounter,
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Unit] = {
    val unassignmentRequest = UnassignmentRequest(
      submitterInfo(submitterParty1),
      // receiverParty2 is not a stakeholder on a contract, but it is listed as stakeholder here
      newStakeholders,
      Set(participant.adminParty.toLf),
      ExampleTransactionFactory.transactionId(0),
      contract,
      reassignmentId.sourceDomain,
      sourceProtocolVersion,
      sourceMediator,
      targetDomain,
      targetPV,
      TimeProofTestUtil.mkTimeProof(timestamp = CantonTimestamp.Epoch, targetDomain = targetDomain),
      reassignmentCounter,
    )
    val fullUnassignmentTree = unassignmentRequest
      .toFullUnassignmentTree(
        pureCrypto,
        pureCrypto,
        seed,
        uuid,
      )

    UnassignmentValidation(
      fullUnassignmentTree,
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
