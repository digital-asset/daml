// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.EitherT
import com.digitalasset.canton.*
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.crypto.{Signature, SigningKeyUsage}
import com.digitalasset.canton.data.{
  CantonTimestamp,
  FullUnassignmentTree,
  ReassignmentRef,
  UnassignmentViewTree,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.protocol.EngineController
import com.digitalasset.canton.participant.protocol.conflictdetection.ConflictDetectionHelpers.mkActivenessResult
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.{
  ParsedReassignmentRequest,
  ReassignmentProcessorError,
}
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentValidationError.{
  ContractMetadataMismatch,
  ReassigningParticipantsMismatch,
  StakeholdersMismatch,
  SubmitterMustBeStakeholder,
}
import com.digitalasset.canton.participant.protocol.submission.SeedGenerator
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.sequencing.protocol.{MediatorGroupRecipient, Recipients}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import org.scalatest.wordspec.AnyWordSpec

import java.util.UUID

class UnassignmentValidationTest extends AnyWordSpec with BaseTest with HasExecutionContext {
  private val sourceSynchronizer = Source(SynchronizerId.tryFromString("synchronizer::source"))
  private val sourceMediator = MediatorGroupRecipient(MediatorGroupIndex.tryCreate(100))
  private val targetSynchronizer = Target(
    SynchronizerId(UniqueIdentifier.tryFromProtoPrimitive("synchronizer::target"))
  )

  private val signatory: LfPartyId = LfPartyId.assertFromString("signatory::party")
  private val observer: LfPartyId = LfPartyId.assertFromString("observer::party")

  private val nonStakeholder: LfPartyId = LfPartyId.assertFromString("nonStakeholder::party")

  private val receiverParty2: LfPartyId = PartyId(
    UniqueIdentifier.tryFromProtoPrimitive("receiverParty2::party")
  ).toLf

  private val confirmingParticipant =
    ParticipantId.tryFromProtoPrimitive("PAR::bothsynchronizers::confirmingParticipant")
  private val observingParticipant =
    ParticipantId.tryFromProtoPrimitive("PAR::bothsynchronizers::observingParticipant")
  private val otherParticipant = ParticipantId.tryFromProtoPrimitive("PAR::sync::participant")

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

  private val reassigningParticipants: Set[ParticipantId] =
    Set(confirmingParticipant, observingParticipant)

  private val identityFactory: TestingIdentityFactory = TestingTopology()
    .withSynchronizers(sourceSynchronizer.unwrap)
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

  "unassignment validation" should {
    "succeed without errors" in {
      val validation = performValidation().futureValueUS.value

      validation.isSuccessfulF.futureValueUS shouldBe true
    }

    "fail when wrong metadata is given (stakeholders)" in {
      def test(
          metadata: ContractMetadata
      ): Either[ReassignmentValidationError, Unit] = {
        val updatedContract = contract.copy(metadata = metadata)
        performValidation(updatedContract).futureValueUS.value.metadataResultET.futureValueUS
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

      test(contract.metadata).isRight shouldBe true

      test(incorrectStakeholders).left.value shouldBe
        ContractMetadataMismatch(
          reassignmentRef = ReassignmentRef(contract.contractId),
          declaredContractMetadata = incorrectStakeholders,
          expectedMetadata = contract.metadata,
        )

      test(incorrectSignatories).left.value shouldBe ContractMetadataMismatch(
        reassignmentRef = ReassignmentRef(contract.contractId),
        declaredContractMetadata = incorrectSignatories,
        expectedMetadata = contract.metadata,
      )
    }

    "fail when wrong metadata is given (contract key)" in {
      def test(
          metadata: ContractMetadata
      ): Either[ReassignmentValidationError, Unit] = {
        val updatedContract = contract.copy(metadata = metadata)
        performValidation(updatedContract).futureValueUS.value.metadataResultET.futureValueUS
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

      test(contract.metadata).isRight shouldBe true

      test(incorrectMetadata).left.value shouldBe ContractMetadataMismatch(
        reassignmentRef = ReassignmentRef(contract.contractId),
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
          sourceSynchronizer,
          targetSynchronizer,
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
      ): Either[ReassignmentValidationError, Unit] = {
        val view = createUnassignmentTree(viewContractMetadata).tree.view
        val commonData = createUnassignmentTree(commonDataContractMetadata).tree.commonData

        validateUnassignmentTree(
          FullUnassignmentTree(
            UnassignmentViewTree(commonData, view, Source(testedProtocolVersion), pureCrypto)
          )
        ).futureValueUS.value.metadataResultET.futureValueUS
      }

      val incorrectMetadata = ContractMetadata.tryCreate(
        stakeholders = stakeholders.all + receiverParty2,
        signatories = stakeholders.signatories,
        maybeKeyWithMaintainersVersioned = None,
      )

      val correctMetadata = contract.metadata

      test(correctMetadata, correctMetadata).isRight shouldBe true
      test(correctMetadata, incorrectMetadata).left.value shouldBe StakeholdersMismatch(
        reassignmentRef = ReassignmentRef(contract.contractId),
        declaredViewStakeholders = Stakeholders(incorrectMetadata),
        expectedStakeholders = Stakeholders(correctMetadata),
      )
    }

    "detect non-stakeholder submitter" in {
      def unassignmentValidation(submitter: LfPartyId) = {
        val validation = performValidation(submitter = submitter)

        validation.futureValueUS.value.validationErrors
      }

      assert(!stakeholders.all.contains(nonStakeholder))

      unassignmentValidation(signatory) shouldBe Nil
      unassignmentValidation(
        nonStakeholder
      ) shouldBe Seq(
        SubmitterMustBeStakeholder(
          ReassignmentRef(contract.contractId),
          submittingParty = nonStakeholder,
          stakeholders = stakeholders.all,
        )
      )
    }

    "detect reassigning participant mismatch" in {
      def unassignmentValidation(reassigningParticipants: Set[ParticipantId]) =
        performValidation(
          reassigningParticipantsOverride = reassigningParticipants
        ).futureValueUS.value.validationErrors

      // Happy path / control
      unassignmentValidation(reassigningParticipants = reassigningParticipants) shouldBe Seq()

      // Additional/extra reassigning participant
      val additionalReassigningParticipant = reassigningParticipants + otherParticipant

      unassignmentValidation(
        reassigningParticipants = additionalReassigningParticipant
      ) shouldBe Seq(
        ReassigningParticipantsMismatch(
          ReassignmentRef(contract.contractId),
          expected = reassigningParticipants,
          declared = additionalReassigningParticipant,
        )
      )

      // Additional/extra reassigning participant
      val additionalConfirmingReassigningParticipant = reassigningParticipants + otherParticipant

      unassignmentValidation(
        reassigningParticipants = additionalConfirmingReassigningParticipant
      ) shouldBe Seq(
        ReassigningParticipantsMismatch(
          ReassignmentRef(contract.contractId),
          expected = reassigningParticipants,
          declared = additionalConfirmingReassigningParticipant,
        )
      )

      // Missing reassigning participant
      val missingConfirmingReassigningParticipant = Set.empty[ParticipantId]
      unassignmentValidation(
        reassigningParticipants = missingConfirmingReassigningParticipant
      ) shouldBe Seq(
        ReassigningParticipantsMismatch(
          ReassignmentRef(contract.contractId),
          expected = reassigningParticipants,
          declared = missingConfirmingReassigningParticipant,
        )
      )

      // Empty set
      unassignmentValidation(
        reassigningParticipants = Set.empty
      ) shouldBe Seq(
        ReassigningParticipantsMismatch(
          ReassignmentRef(contract.contractId),
          expected = reassigningParticipants,
          declared = Set.empty,
        )
      )
    }
  }

  private val cryptoSnapshot = identityFactory
    .forOwnerAndSynchronizer(confirmingParticipant, sourceSynchronizer.unwrap)
    .currentSnapshotApproximation

  private def mkParsedRequest(
      view: FullUnassignmentTree,
      recipients: Recipients,
      signatureO: Option[Signature],
  ): ParsedReassignmentRequest[FullUnassignmentTree] = ParsedReassignmentRequest(
    RequestCounter(1),
    CantonTimestamp.Epoch,
    SequencerCounter(1),
    view,
    recipients,
    signatureO,
    None,
    isFreshOwnTimelyRequest = true,
    Seq.empty,
    sourceMediator,
    cryptoSnapshot,
    cryptoSnapshot.ipsSnapshot.findDynamicSynchronizerParameters().futureValueUS.value,
  )

  private def validateUnassignmentTree(
      fullUnassignmentTree: FullUnassignmentTree,
      identityFactory: TestingIdentityFactory = identityFactory,
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, UnassignmentValidationResult] = {
    val recipients = Recipients.cc(
      reassigningParticipants.toSeq.head,
      reassigningParticipants.toSeq.tail*
    )
    val signature = cryptoSnapshot
      .sign(fullUnassignmentTree.rootHash.unwrap, SigningKeyUsage.ProtocolOnly)
      .futureValueUS
      .value
    val parsed = mkParsedRequest(fullUnassignmentTree, recipients, Some(signature))

    val damle = DAMLeTestInstance(
      confirmingParticipant,
      stakeholders.signatories,
      stakeholders.all,
    )(loggerFactory)

    val unassignmentValidation = new UnassignmentValidation(confirmingParticipant, damle)

    val engineController =
      EngineController(confirmingParticipant, RequestId(CantonTimestamp.Epoch), loggerFactory)
    unassignmentValidation.perform(
      sourceTopology = Source(identityFactory.topologySnapshot()),
      targetTopology = Some(Target(identityFactory.topologySnapshot())),
      activenessF = FutureUnlessShutdown.pure(mkActivenessResult()),
      engineController = engineController,
    )(parsedRequest = parsed)

  }

  private def performValidation(
      contract: SerializableContract = contract,
      reassigningParticipantsOverride: Set[ParticipantId] = reassigningParticipants,
      submitter: LfPartyId = signatory,
      identityFactory: TestingIdentityFactory = identityFactory,
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, UnassignmentValidationResult] = {
    val unassignmentRequest =
      ReassignmentDataHelpers(contract, sourceSynchronizer, targetSynchronizer, identityFactory)
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
      identityFactory,
    )
  }
}
