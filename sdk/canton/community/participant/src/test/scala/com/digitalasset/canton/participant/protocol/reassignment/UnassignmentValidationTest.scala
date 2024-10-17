// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.EitherT
import com.digitalasset.canton.*
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.{
  ReassignmentProcessorError,
  StakeholdersMismatch,
  TemplateIdMismatch,
}
import com.digitalasset.canton.participant.protocol.reassignment.UnassignmentProcessorError.{
  ReassigningParticipantsMismatch,
  UnassignmentSubmitterMustBeStakeholder,
}
import com.digitalasset.canton.participant.protocol.submission.SeedGenerator
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.sequencing.protocol.{MediatorGroupRecipient, Recipients}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.version.ProtocolVersion
import org.scalatest.wordspec.AnyWordSpec

import java.util.UUID

class UnassignmentValidationTest extends AnyWordSpec with BaseTest with HasExecutionContext {
  private val sourceDomain = Source(DomainId.tryFromString("domain::source"))
  private val sourceMediator = MediatorGroupRecipient(MediatorGroupIndex.tryCreate(100))
  private val targetDomain = Target(
    DomainId(UniqueIdentifier.tryFromProtoPrimitive("domain::target"))
  )

  private val submitterParty1: LfPartyId = LfPartyId.assertFromString("submitterParty::party")
  private val nonStakeholder: LfPartyId = LfPartyId.assertFromString("nonStakeholder::party")

  private val receiverParty2: LfPartyId = PartyId(
    UniqueIdentifier.tryFromProtoPrimitive("receiverParty2::party")
  ).toLf

  private val participant = ParticipantId.tryFromProtoPrimitive("PAR::bothdomains::participant")
  private val otherParticipant = ParticipantId.tryFromProtoPrimitive("PAR::domain::participant")

  private val contractId = ExampleTransactionFactory.suffixedId(10, 0)

  private val uuid = new UUID(3L, 4L)
  private val pureCrypto = new SymbolicPureCrypto
  private val seedGenerator = new SeedGenerator(pureCrypto)
  private val seed = seedGenerator.generateSaltSeed()

  private val templateId =
    LfTemplateId.assertFromString("unassignmentvalidationtestpackage:template:id")

  private val wrongTemplateId =
    LfTemplateId.assertFromString("unassignmentvalidatoionpackage:wrongtemplate:id")

  private val contract = ExampleTransactionFactory.asSerializable(
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

  private val stakeholders = Stakeholders.tryCreate(Set(submitterParty1))
  private val sourcePV = Source(testedProtocolVersion)

  "unassignment validation" should {
    "succeed without errors" in {
      val validation = mkUnassignmentValidation(
        stakeholders,
        sourcePV,
        templateId,
      )

      validation.valueOrFailShutdown("validation failed").futureValue shouldBe ()
    }
  }

  "detect stakeholders mismatch" in {
    // receiverParty2 is not a stakeholder on a contract, but it is listed as stakeholder here
    val incorrectStakeholders = Stakeholders.tryCreate(
      stakeholders = stakeholders.stakeholders + receiverParty2
    )

    val validation = mkUnassignmentValidation(
      incorrectStakeholders,
      sourcePV,
      templateId,
    )

    validation.futureValueUS.left.value shouldBe StakeholdersMismatch(
      None,
      Set(submitterParty1, receiverParty2),
      None,
      Right(Set(submitterParty1)),
    )
  }

  "detect template id mismatch" in {
    // template id does not match the one in the contract
    val validation = mkUnassignmentValidation(
      stakeholders,
      sourcePV,
      wrongTemplateId,
    )

    validation.futureValueUS.left.value shouldBe TemplateIdMismatch(
      templateId.leftSide,
      wrongTemplateId.leftSide,
    )
  }

  "detect non-stakeholder submitter" in {
    def unassignmentValidation(submitter: LfPartyId) = {
      val validation = mkUnassignmentValidation(
        stakeholders,
        sourcePV,
        templateId,
        submitter = submitter,
      )

      validation.futureValueUS
    }

    assert(!stakeholders.stakeholders.contains(nonStakeholder))

    unassignmentValidation(submitterParty1).value shouldBe ()
    unassignmentValidation(
      nonStakeholder
    ).left.value shouldBe UnassignmentSubmitterMustBeStakeholder(
      contractId,
      submittingParty = nonStakeholder,
      stakeholders = stakeholders.stakeholders,
    )
  }

  "detect reassigning participant mismatch" in {
    def unassignmentValidation(reassigningParticipants: Set[ParticipantId]) =
      mkUnassignmentValidation(
        stakeholders,
        sourcePV,
        templateId,
        reassigningParticipants = reassigningParticipants,
      ).futureValueUS

    // Happy path / control
    unassignmentValidation(reassigningParticipants = Set(participant)).value shouldBe ()

    unassignmentValidation(
      reassigningParticipants = Set(otherParticipant)
    ).left.value shouldBe ReassigningParticipantsMismatch(
      contractId,
      expected = Set(participant),
      declared = Set(otherParticipant),
    )

    unassignmentValidation(
      reassigningParticipants = Set()
    ).left.value shouldBe ReassigningParticipantsMismatch(
      contractId,
      expected = Set(participant),
      declared = Set(),
    )
  }

  private def mkUnassignmentValidation(
      newStakeholders: Stakeholders,
      sourceProtocolVersion: Source[ProtocolVersion],
      expectedTemplateId: LfTemplateId,
      reassigningParticipants: Set[ParticipantId] = Set(participant),
      submitter: LfPartyId = submitterParty1,
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Unit] = {

    val updatedContract = contract.copy(metadata =
      ContractMetadata.tryCreate(
        signatories = Set(),
        stakeholders = newStakeholders.stakeholders,
        maybeKeyWithMaintainersVersioned = None,
      )
    )

    val unassignmentRequest =
      new ReassignmentDataHelpers(updatedContract, sourceDomain, targetDomain, identityFactory)
        .unassignmentRequest(
          submitter,
          submittingParticipant = participant,
          sourceMediator = sourceMediator,
        )(reassigningParticipants)

    val fullUnassignmentTree = unassignmentRequest
      .toFullUnassignmentTree(
        pureCrypto,
        pureCrypto,
        seed,
        uuid,
      )

    UnassignmentValidation.perform(
      expectedStakeholders = stakeholders,
      expectedTemplateId = expectedTemplateId,
      sourceProtocolVersion,
      Source(identityFactory.topologySnapshot()),
      Some(Target(identityFactory.topologySnapshot())),
      Recipients.cc(participant),
    )(fullUnassignmentTree)
  }
}
