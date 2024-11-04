// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.EitherT
import com.digitalasset.canton.*
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.data.ReassigningParticipants
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.protocol.SerializableContractAuthenticator
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.{
  ContractError,
  ReassignmentProcessorError,
  StakeholdersMismatch,
  SubmitterMustBeStakeholder,
}
import com.digitalasset.canton.participant.protocol.reassignment.UnassignmentProcessorError.ReassigningParticipantsMismatch
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

  private val signatory: LfPartyId = LfPartyId.assertFromString("signatory::party")
  private val observer: LfPartyId = LfPartyId.assertFromString("observer::party")

  private val nonStakeholder: LfPartyId = LfPartyId.assertFromString("nonStakeholder::party")

  private val receiverParty2: LfPartyId = PartyId(
    UniqueIdentifier.tryFromProtoPrimitive("receiverParty2::party")
  ).toLf

  private val confirmingParticipant =
    ParticipantId.tryFromProtoPrimitive("PAR::bothdomains::confirmingParticipant")
  private val observingParticipant =
    ParticipantId.tryFromProtoPrimitive("PAR::bothdomains::observingParticipant")
  private val otherParticipant = ParticipantId.tryFromProtoPrimitive("PAR::domain::participant")

  private val uuid = new UUID(3L, 4L)
  private val pureCrypto = new SymbolicPureCrypto
  private val seedGenerator = new SeedGenerator(pureCrypto)
  private val seed = seedGenerator.generateSaltSeed()

  private val wrongTemplateId =
    LfTemplateId.assertFromString("unassignmentvalidatoionpackage:wrongtemplate:id")

  private val metadata = ContractMetadata.tryCreate(
    signatories = Set(signatory),
    stakeholders = Set(signatory, observer),
    maybeKeyWithMaintainersVersioned = None,
  )
  private val stakeholders: Stakeholders = Stakeholders(metadata)

  private val contract = ExampleTransactionFactory.authenticatedSerializableContract(
    metadata = metadata
  )

  private val reassigningParticipants = ReassigningParticipants.tryCreate(
    confirming = Set(confirmingParticipant),
    observing = Set(confirmingParticipant, observingParticipant),
  )

  private val identityFactory: TestingIdentityFactory = TestingTopology()
    .withDomains(sourceDomain.unwrap)
    .withReversedTopology(
      Map(
        confirmingParticipant -> Map(
          signatory -> ParticipantPermission.Submission,
          receiverParty2 -> ParticipantPermission.Submission,
        ),
        observingParticipant -> Map(observer -> ParticipantPermission.Observation),
      )
    )
    .withSimpleParticipants(
      confirmingParticipant
    ) // required such that `participant` gets a signing key
    .withPackages(
      Map(
        confirmingParticipant -> Seq(
          ExampleTransactionFactory.packageId,
          wrongTemplateId.packageId,
        ),
        observingParticipant -> Seq(ExampleTransactionFactory.packageId, wrongTemplateId.packageId),
      )
    )
    .build(loggerFactory)

  private val sourcePV = Source(testedProtocolVersion)

  "unassignment validation" should {
    "succeed without errors" in {
      val validation = mkUnassignmentValidation()

      validation.valueOrFailShutdown("validation failed").futureValue shouldBe ()
    }

    "detect stakeholders mismatch" in {
      // receiverParty2 is not a stakeholder on a contract, but it is listed as stakeholder here
      val incorrectStakeholders = Stakeholders.tryCreate(
        stakeholders = stakeholders.all + receiverParty2,
        signatories = stakeholders.signatories,
      )

      val updatedContract = contract.copy(metadata =
        ContractMetadata.tryCreate(
          signatories = incorrectStakeholders.signatories,
          stakeholders = incorrectStakeholders.all,
          maybeKeyWithMaintainersVersioned = None,
        )
      )

      val validation = mkUnassignmentValidation(updatedContract)

      validation.futureValueUS.left.value shouldBe StakeholdersMismatch(
        None,
        incorrectStakeholders,
        None,
        Right(Stakeholders(contract.metadata)),
      )
    }

    "detect template id mismatch" in {
      // template id does not match the one in the contract
      val validation = mkUnassignmentValidation(expectedTemplateId = wrongTemplateId)

      validation.futureValueUS.left.value shouldBe ContractError.templateIdMismatch(
        ExampleTransactionFactory.templateId.leftSide,
        wrongTemplateId.leftSide,
      )
    }

    "detect invalid contract id" in {
      def validate(cid: LfContractId) = {
        val updatedContract = contract.copy(contractId = cid)

        val validation = mkUnassignmentValidation(updatedContract)

        validation.futureValueUS
      }

      val unauthenticatedContractId = ExampleTransactionFactory
        .authenticatedSerializableContract(
          metadata = ContractMetadata.tryCreate(
            signatories = Set(signatory),
            stakeholders = Set(signatory, nonStakeholder),
            maybeKeyWithMaintainersVersioned = None,
          )
        )
        .contractId

      validate(contract.contractId).value shouldBe ()

      inside(validate(unauthenticatedContractId).left.value) {
        case ContractError(msg) if msg.contains("Mismatching contract id suffixes.") => ()
      }
    }

    "detect non-stakeholder submitter" in {
      def unassignmentValidation(submitter: LfPartyId) = {
        val validation = mkUnassignmentValidation(submitter = submitter)

        validation.futureValueUS
      }

      assert(!stakeholders.all.contains(nonStakeholder))

      unassignmentValidation(signatory).value shouldBe ()
      unassignmentValidation(
        nonStakeholder
      ).left.value shouldBe SubmitterMustBeStakeholder(
        ReassignmentRef(contract.contractId),
        submittingParty = nonStakeholder,
        stakeholders = stakeholders.all,
      )
    }

    "detect reassigning participant mismatch" in {
      def unassignmentValidation(reassigningParticipants: ReassigningParticipants) =
        mkUnassignmentValidation(
          reassigningParticipantsOverride = reassigningParticipants
        ).futureValueUS

      // Happy path / control
      unassignmentValidation(reassigningParticipants = reassigningParticipants).value shouldBe ()

      // Additional/extra observing reassigning participant
      val additionalObservingReassigningParticipant = ReassigningParticipants.tryCreate(
        confirming = reassigningParticipants.confirming,
        observing = reassigningParticipants.observing + otherParticipant,
      )
      unassignmentValidation(
        reassigningParticipants = additionalObservingReassigningParticipant
      ).left.value shouldBe ReassigningParticipantsMismatch(
        contract.contractId,
        expected = reassigningParticipants,
        declared = additionalObservingReassigningParticipant,
      )

      // Additional/extra observing reassigning participant
      val additionalConfirmingReassigningParticipant = ReassigningParticipants.tryCreate(
        confirming = reassigningParticipants.confirming + otherParticipant,
        observing = reassigningParticipants.observing + otherParticipant,
      )
      unassignmentValidation(
        reassigningParticipants = additionalConfirmingReassigningParticipant
      ).left.value shouldBe ReassigningParticipantsMismatch(
        contract.contractId,
        expected = reassigningParticipants,
        declared = additionalConfirmingReassigningParticipant,
      )

      // Missing reassigning participant
      val missingConfirmingReassigningParticipant = ReassigningParticipants.tryCreate(
        confirming = Set(),
        observing = reassigningParticipants.observing,
      )
      unassignmentValidation(
        reassigningParticipants = missingConfirmingReassigningParticipant
      ).left.value shouldBe ReassigningParticipantsMismatch(
        contract.contractId,
        expected = reassigningParticipants,
        declared = missingConfirmingReassigningParticipant,
      )

      // Empty set
      unassignmentValidation(
        reassigningParticipants = ReassigningParticipants.empty
      ).left.value shouldBe ReassigningParticipantsMismatch(
        contract.contractId,
        expected = reassigningParticipants,
        declared = ReassigningParticipants.empty,
      )
    }
  }

  private def mkUnassignmentValidation(
      contract: SerializableContract = contract,
      sourceProtocolVersion: Source[ProtocolVersion] = sourcePV,
      expectedTemplateId: LfTemplateId = ExampleTransactionFactory.templateId,
      reassigningParticipantsOverride: ReassigningParticipants = reassigningParticipants,
      submitter: LfPartyId = signatory,
      identityFactory: TestingIdentityFactory = identityFactory,
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Unit] = {

    val unassignmentRequest =
      new ReassignmentDataHelpers(contract, sourceDomain, targetDomain, identityFactory)
        .unassignmentRequest(
          submitter,
          submittingParticipant = confirmingParticipant,
          sourceMediator = sourceMediator,
        )(reassigningParticipantsOverride)

    val fullUnassignmentTree = unassignmentRequest
      .toFullUnassignmentTree(
        pureCrypto,
        pureCrypto,
        seed,
        uuid,
      )

    val recipients = Recipients.cc(
      reassigningParticipants.observing.toSeq.head,
      reassigningParticipants.observing.toSeq.tail*
    )

    UnassignmentValidation.perform(
      SerializableContractAuthenticator(pureCrypto),
      expectedStakeholders = stakeholders,
      expectedTemplateId = expectedTemplateId,
      sourceProtocolVersion,
      Source(identityFactory.topologySnapshot()),
      Some(Target(identityFactory.topologySnapshot())),
      recipients,
    )(fullUnassignmentTree)
  }
}
