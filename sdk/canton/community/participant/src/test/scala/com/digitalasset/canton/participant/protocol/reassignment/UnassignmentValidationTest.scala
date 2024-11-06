// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.EitherT
import com.digitalasset.canton.*
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.data.{
  FullUnassignmentTree,
  ReassigningParticipants,
  UnassignmentViewTree,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.protocol.EngineController.EngineAbortStatus
import com.digitalasset.canton.participant.protocol.SerializableContractAuthenticator
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.{
  ContractError,
  ContractMetadataMismatch,
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
      val validation = performValidation()

      validation.valueOrFailShutdown("validation failed").futureValue shouldBe ()
    }

    "fail when wrong metadata is given (stakeholders)" in {
      def test(metadata: ContractMetadata): Either[ReassignmentProcessorError, Unit] = {
        val updatedContract = contract.copy(metadata = metadata)
        performValidation(updatedContract).futureValueUS
      }

      val incorrectStakeholders = ContractMetadata.tryCreate(
        stakeholders = stakeholders.all + receiverParty2,
        signatories = stakeholders.signatories,
        maybeKeyWithMaintainersVersioned = None,
      )

      val incorrectSignatories = ContractMetadata.tryCreate(
        stakeholders = stakeholders.all + receiverParty2,
        signatories = stakeholders.signatories + receiverParty2,
        maybeKeyWithMaintainersVersioned = None,
      )

      test(contract.metadata).value shouldBe ()

      test(incorrectStakeholders).left.value shouldBe ContractMetadataMismatch(
        reassignmentId = None,
        declaredContractMetadata = incorrectStakeholders,
        expectedMetadata = contract.metadata,
      )

      test(incorrectSignatories).left.value shouldBe ContractMetadataMismatch(
        reassignmentId = None,
        declaredContractMetadata = incorrectSignatories,
        expectedMetadata = contract.metadata,
      )
    }

    "fail when wrong metadata is given (contract key)" in {
      def test(metadata: ContractMetadata): Either[ReassignmentProcessorError, Unit] = {
        val updatedContract = contract.copy(metadata = metadata)
        performValidation(updatedContract).futureValueUS
      }

      val incorrectKey = ExampleTransactionFactory.globalKeyWithMaintainers(
        ExampleTransactionFactory.defaultGlobalKey,
        Set(contract.metadata.stakeholders.head),
      )

      val incorrectMetadata = ContractMetadata.tryCreate(
        stakeholders = contract.metadata.stakeholders,
        signatories = contract.metadata.signatories,
        maybeKeyWithMaintainersVersioned = Some(incorrectKey),
      )

      test(contract.metadata).value shouldBe ()

      test(incorrectMetadata).left.value shouldBe ContractMetadataMismatch(
        reassignmentId = None,
        declaredContractMetadata = incorrectMetadata,
        expectedMetadata = contract.metadata,
      )
    }

    "fail when inconsistent stakeholders are given" in {
      /*
      We construct in this test an inconsistent `inconsistentTree: FullUnassignmentTree` :
      - inconsistentTree.tree.commonData.stakeholders is incorrect
      - inconsistentTree.view.contract.metadata is correct
       */

      def createUnassignmentTree(metadata: ContractMetadata): FullUnassignmentTree =
        ReassignmentDataHelpers(
          contract.copy(metadata = metadata),
          sourceDomain,
          targetDomain,
          identityFactory,
        ).unassignmentRequest(
          submitter = signatory,
          submittingParticipant = confirmingParticipant,
          sourceMediator = sourceMediator,
        )(reassigningParticipants)
          .toFullUnassignmentTree(
            pureCrypto,
            pureCrypto,
            seed,
            uuid,
          )

      def test(
          viewContractMetadata: ContractMetadata,
          commonDataContractMetadata: ContractMetadata,
      ): Either[ReassignmentProcessorError, Unit] = {
        val view = createUnassignmentTree(viewContractMetadata).tree.view
        val commonData = createUnassignmentTree(commonDataContractMetadata).tree.commonData

        validateUnassignmentTree(
          FullUnassignmentTree(
            UnassignmentViewTree(commonData, view, Source(testedProtocolVersion), pureCrypto)
          )
        ).futureValueUS
      }

      val incorrectMetadata = ContractMetadata.tryCreate(
        stakeholders = stakeholders.all + receiverParty2,
        signatories = stakeholders.signatories,
        maybeKeyWithMaintainersVersioned = None,
      )

      val correctMetadata = contract.metadata

      test(correctMetadata, correctMetadata).value shouldBe ()
      test(correctMetadata, incorrectMetadata).left.value shouldBe StakeholdersMismatch(
        reassignmentId = None,
        declaredViewStakeholders = Stakeholders(incorrectMetadata),
        declaredContractStakeholders = Some(Stakeholders(correctMetadata)),
        expectedStakeholders = Right(Stakeholders(correctMetadata)),
      )
    }

    "detect invalid contract id" in {
      def validate(cid: LfContractId) = {
        val updatedContract = contract.copy(contractId = cid)

        val validation = performValidation(updatedContract)

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
        val validation = performValidation(submitter = submitter)

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
        performValidation(
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

  private def validateUnassignmentTree(
      fullUnassignmentTree: FullUnassignmentTree,
      sourceProtocolVersion: Source[ProtocolVersion] = sourcePV,
      identityFactory: TestingIdentityFactory = identityFactory,
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Unit] = {
    val recipients = Recipients.cc(
      reassigningParticipants.observing.toSeq.head,
      reassigningParticipants.observing.toSeq.tail*
    )

    val damle = DAMLeTestInstance(
      reassigningParticipants.confirming.head,
      stakeholders.signatories,
      stakeholders.all,
    )(loggerFactory)

    UnassignmentValidation.perform(
      SerializableContractAuthenticator(pureCrypto),
      sourceProtocolVersion,
      Source(identityFactory.topologySnapshot()),
      Some(Target(identityFactory.topologySnapshot())),
      recipients,
      damle,
      () => EngineAbortStatus.notAborted,
      loggerFactory,
    )(fullUnassignmentTree)
  }

  private def performValidation(
      contract: SerializableContract = contract,
      sourceProtocolVersion: Source[ProtocolVersion] = sourcePV,
      reassigningParticipantsOverride: ReassigningParticipants = reassigningParticipants,
      submitter: LfPartyId = signatory,
      identityFactory: TestingIdentityFactory = identityFactory,
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Unit] = {
    val unassignmentRequest =
      ReassignmentDataHelpers(contract, sourceDomain, targetDomain, identityFactory)
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

    validateUnassignmentTree(
      fullUnassignmentTree,
      sourceProtocolVersion,
      identityFactory,
    )
  }
}
