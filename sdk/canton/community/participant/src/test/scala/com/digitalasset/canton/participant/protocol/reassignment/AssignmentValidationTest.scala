// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import com.digitalasset.canton.*
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.data.{
  CantonTimestamp,
  FullAssignmentTree,
  ReassignmentRef,
  ReassignmentSubmitterMetadata,
}
import com.digitalasset.canton.participant.protocol.SerializableContractAuthenticator
import com.digitalasset.canton.participant.protocol.reassignment.AssignmentValidation.*
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.{
  ContractError,
  ReassigningParticipantsMismatch,
  SubmitterMustBeStakeholder,
}
import com.digitalasset.canton.participant.protocol.submission.SeedGenerator
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.protocol.MediatorGroupRecipient
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import org.scalatest.wordspec.AsyncWordSpec

import java.util.UUID
import scala.concurrent.{Future, Promise}

class AssignmentValidationTest
    extends AsyncWordSpec
    with BaseTest
    with ProtocolVersionChecksAsyncWordSpec
    with HasActorSystem
    with HasExecutionContext
    with FailOnShutdown {
  private val sourceDomain = Source(
    DomainId(UniqueIdentifier.tryFromProtoPrimitive("domain::source"))
  )
  private val sourceMediator = MediatorGroupRecipient(MediatorGroupIndex.tryCreate(0))
  private val targetDomain = Target(
    DomainId(UniqueIdentifier.tryFromProtoPrimitive("domain::target"))
  )
  private val targetMediator = MediatorGroupRecipient(MediatorGroupIndex.tryCreate(0))

  private val signatory: LfPartyId = PartyId(
    UniqueIdentifier.tryFromProtoPrimitive("signatory::party")
  ).toLf
  private val observer: LfPartyId = PartyId(
    UniqueIdentifier.tryFromProtoPrimitive("observer::party")
  ).toLf

  private val otherParty: LfPartyId = PartyId(
    UniqueIdentifier.tryFromProtoPrimitive("otherParty::party")
  ).toLf

  private val submittingParticipant = ParticipantId(
    UniqueIdentifier.tryFromProtoPrimitive("bothdomains::participant")
  )
  private val submittingAdminParty = submittingParticipant.adminParty.toLf
  private val observingParticipant = ParticipantId(
    UniqueIdentifier.tryFromProtoPrimitive("bothdomains::observingParticipant")
  )

  private val otherParticipant = ParticipantId(
    UniqueIdentifier.tryFromProtoPrimitive("domain::participant")
  )

  private def submitterInfo(submitter: LfPartyId): ReassignmentSubmitterMetadata =
    ReassignmentSubmitterMetadata(
      submitter,
      submittingParticipant,
      LedgerCommandId.assertFromString("assignment-validation-command-id"),
      submissionId = None,
      LedgerApplicationId.assertFromString("tests"),
      workflowId = None,
    )

  private val identityFactory = TestingTopology()
    .withDomains(sourceDomain.unwrap)
    .withReversedTopology(
      Map(
        submittingParticipant -> Map(signatory -> ParticipantPermission.Submission),
        observingParticipant -> Map(observer -> ParticipantPermission.Observation),
      )
    )
    // required such that `participant` gets a signing key
    .withSimpleParticipants(submittingParticipant)
    .build(loggerFactory)

  private lazy val reassigningParticipants = Set(submittingParticipant, observingParticipant)

  private val cryptoSnapshot =
    identityFactory
      .forOwnerAndDomain(submittingParticipant, sourceDomain.unwrap)
      .currentSnapshotApproximation

  private val pureCrypto = new SymbolicPureCrypto

  private val seedGenerator = new SeedGenerator(pureCrypto)

  private val assignmentValidation = testInstance(targetDomain, cryptoSnapshot, None)

  "validateAssignmentRequest" should {
    val contract = ExampleTransactionFactory.authenticatedSerializableContract(
      ContractMetadata.tryCreate(
        signatories = Set(signatory),
        stakeholders = Set(signatory, observer),
        None,
      )
    )

    val reassignmentId = ReassignmentId(sourceDomain, CantonTimestamp.Epoch)

    val reassignmentDataHelpers = ReassignmentDataHelpers(
      contract,
      reassignmentId.sourceDomain,
      targetDomain,
      identityFactory,
    )

    val unassignmentRequest =
      reassignmentDataHelpers.unassignmentRequest(
        signatory,
        submittingParticipant,
        sourceMediator,
      )(reassigningParticipants = reassigningParticipants)

    val reassignmentData =
      reassignmentDataHelpers.reassignmentData(reassignmentId, unassignmentRequest)()

    val unassignmentResult = reassignmentDataHelpers
      .unassignmentResult(reassignmentData)
      .futureValue

    val assignmentRequest = makeFullAssignmentTree(
      contract,
      unassignmentResult,
    )

    "succeed without errors in the basic case (no reassignment data)" in {
      assignmentValidation
        .validateAssignmentRequest(
          CantonTimestamp.Epoch,
          assignmentRequest,
          reassignmentDataO = None,
          Target(cryptoSnapshot),
          isConfirming = false,
        )
        .futureValueUS
        .value shouldBe None
    }

    "succeed without errors when reassignment data is valid" in {
      def validate(
          isConfirmingReassigningParticipant: Boolean
      ): Option[AssignmentValidationResult] = assignmentValidation
        .validateAssignmentRequest(
          CantonTimestamp.Epoch,
          assignmentRequest,
          reassignmentDataO = Some(reassignmentData),
          Target(cryptoSnapshot),
          isConfirming = isConfirmingReassigningParticipant,
        )
        .futureValueUS
        .value

      validate(isConfirmingReassigningParticipant = true).value.confirmingParties shouldBe
        Set(signatory, submittingAdminParty)

      validate(isConfirmingReassigningParticipant = false) shouldBe None
    }

    "wait for the topology state to be available" in {
      val promise: Promise[Unit] = Promise()
      val assignmentProcessingSteps2 =
        testInstance(
          targetDomain,
          cryptoSnapshot,
          Some(promise.future), // Topology state is not available
        )

      val inValidated = assignmentProcessingSteps2
        .validateAssignmentRequest(
          CantonTimestamp.Epoch,
          assignmentRequest,
          Some(reassignmentData),
          Target(cryptoSnapshot),
          isConfirming = true,
        )
        .value

      always() {
        inValidated.isCompleted shouldBe false
      }

      promise.completeWith(Future.unit)
      for {
        _ <- inValidated
      } yield { succeed }
    }

    "complain about inconsistent reassignment counters" in {
      val assignmentTreeWrongCounter = makeFullAssignmentTree(
        contract,
        unassignmentResult,
        reassignmentCounter = reassignmentData.reassignmentCounter + 1,
      )

      assignmentValidation
        .validateAssignmentRequest(
          CantonTimestamp.Epoch,
          assignmentTreeWrongCounter,
          Some(reassignmentData),
          Target(cryptoSnapshot),
          isConfirming = true,
        )
        .value
        .futureValueUS
        .left
        .value shouldBe InconsistentReassignmentCounter(
        reassignmentId,
        assignmentTreeWrongCounter.reassignmentCounter,
        reassignmentData.reassignmentCounter,
      )
    }

    "detect inconsistent contract data" in {
      def validate(cid: LfContractId) = {
        val updatedContract = contract.copy(contractId = cid)

        val assignmentRequest = makeFullAssignmentTree(
          updatedContract,
          unassignmentResult,
        )

        assignmentValidation
          .validateAssignmentRequest(
            CantonTimestamp.Epoch,
            assignmentRequest,
            Some(reassignmentData),
            Target(cryptoSnapshot),
            isConfirming = true,
          )
          .value
          .futureValueUS
      }

      val unauthenticatedContractId = ExampleTransactionFactory
        .authenticatedSerializableContract(
          metadata = ContractMetadata
            .tryCreate(
              signatories = Set(signatory),
              stakeholders = Set(signatory, otherParty),
              None,
            )
        )
        .contractId

      validate(contract.contractId).value.value shouldBe a[AssignmentValidationResult]

      // The data differs from the one stored locally in ReassignmentData
      validate(unauthenticatedContractId).left.value shouldBe a[ContractDataMismatch]
    }

    "detect invalid contract id" in {
      def validate(cid: LfContractId, reassignmentDataDefined: Boolean) = {
        val updatedContract = contract.copy(contractId = cid)

        val reassignmentDataHelpers = ReassignmentDataHelpers(
          updatedContract,
          reassignmentId.sourceDomain,
          targetDomain,
          identityFactory,
        )

        val unassignmentRequest =
          reassignmentDataHelpers.unassignmentRequest(
            signatory,
            submittingParticipant,
            sourceMediator,
          )(reassigningParticipants)

        val reassignmentData =
          reassignmentDataHelpers.reassignmentData(reassignmentId, unassignmentRequest)()

        val unassignmentResult = reassignmentDataHelpers
          .unassignmentResult(reassignmentData)
          .futureValue

        val assignmentRequest = makeFullAssignmentTree(
          updatedContract,
          unassignmentResult,
        )

        assignmentValidation
          .validateAssignmentRequest(
            CantonTimestamp.Epoch,
            assignmentRequest,
            reassignmentDataO = Option.when(reassignmentDataDefined)(reassignmentData),
            Target(cryptoSnapshot),
            isConfirming = true,
          )
          .value
          .futureValueUS
      }

      val unauthenticatedContractId = ExampleTransactionFactory
        .authenticatedSerializableContract(
          metadata = ContractMetadata
            .tryCreate(
              signatories = Set(signatory),
              stakeholders = Set(signatory, otherParty),
              None,
            )
        )
        .contractId

      validate(contract.contractId, reassignmentDataDefined = true).value.value shouldBe
        a[AssignmentValidationResult]
      validate(contract.contractId, reassignmentDataDefined = false).value.value shouldBe
        a[AssignmentValidationResult]

      inside(validate(unauthenticatedContractId, reassignmentDataDefined = true).left.value) {
        case ContractError(msg) if msg.contains("Mismatching contract id suffixes.") => succeed
      }

      inside(validate(unauthenticatedContractId, reassignmentDataDefined = false).left.value) {
        case ContractError(msg) if msg.contains("Mismatching contract id suffixes.") => succeed
      }
    }

    "detect reassigning participant mismatch" in {
      def validate(reassigningParticipants: Set[ParticipantId]) = {
        val assignmentTree = makeFullAssignmentTree(
          contract,
          unassignmentResult,
          reassigningParticipants = reassigningParticipants,
        )

        assignmentValidation
          .validateAssignmentRequest(
            CantonTimestamp.Epoch,
            assignmentTree,
            Some(reassignmentData),
            Target(cryptoSnapshot),
            isConfirming = true,
          )
          .value
          .futureValueUS
      }

      // Happy path / control
      validate(reassigningParticipants).value.value.confirmingParties shouldBe Set(
        signatory,
        submittingAdminParty,
      )

      // Additional observing participant
      val additionalObservingParticipant = reassigningParticipants + otherParticipant

      validate(
        additionalObservingParticipant
      ).left.value shouldBe ReassigningParticipantsMismatch(
        ReassignmentRef(unassignmentResult.reassignmentId),
        expected = reassigningParticipants,
        declared = additionalObservingParticipant,
      )

      // Additional confirming participant
      val additionalConfirmingParticipant = reassigningParticipants + otherParticipant

      validate(
        additionalConfirmingParticipant
      ).left.value shouldBe ReassigningParticipantsMismatch(
        ReassignmentRef(unassignmentResult.reassignmentId),
        expected = reassigningParticipants,
        declared = additionalConfirmingParticipant,
      )

      // Empty reassigning participants
      validate(Set.empty).left.value shouldBe ReassigningParticipantsMismatch(
        ReassignmentRef(unassignmentResult.reassignmentId),
        expected = reassigningParticipants,
        declared = Set.empty,
      )
    }

    "detect non-stakeholder submitter" in {
      def validate(submitter: LfPartyId) = {
        val assignmentRequest = makeFullAssignmentTree(
          contract,
          unassignmentResult,
          submitter = submitter,
        )

        assignmentValidation
          .validateAssignmentRequest(
            CantonTimestamp.Epoch,
            assignmentRequest,
            Some(reassignmentData),
            Target(cryptoSnapshot),
            isConfirming = true,
          )
          .value
          .futureValueUS
      }

      // Happy path / control
      validate(signatory).value.value.confirmingParties shouldBe Set(
        signatory,
        submittingAdminParty,
      )

      validate(otherParty).left.value shouldBe SubmitterMustBeStakeholder(
        ReassignmentRef(unassignmentResult.reassignmentId),
        submittingParty = otherParty,
        stakeholders = Set(signatory, observer),
      )
    }
  }

  private def testInstance(
      domainId: Target[DomainId],
      snapshotOverride: DomainSnapshotSyncCryptoApi,
      awaitTimestampOverride: Option[Future[Unit]],
  ): AssignmentValidation =
    new AssignmentValidation(
      domainId,
      SerializableContractAuthenticator(pureCrypto),
      Target(defaultStaticDomainParameters),
      submittingParticipant,
      TestReassignmentCoordination.apply(
        Set(),
        CantonTimestamp.Epoch,
        Some(snapshotOverride),
        Some(awaitTimestampOverride),
        loggerFactory,
      ),
      loggerFactory = loggerFactory,
    )

  private def makeFullAssignmentTree(
      contract: SerializableContract,
      unassignmentResult: DeliveredUnassignmentResult,
      submitter: LfPartyId = signatory,
      uuid: UUID = new UUID(4L, 5L),
      targetDomain: Target[DomainId] = targetDomain,
      targetMediator: MediatorGroupRecipient = targetMediator,
      reassignmentCounter: ReassignmentCounter = ReassignmentCounter(1),
      reassigningParticipants: Set[ParticipantId] = reassigningParticipants,
  ): FullAssignmentTree = {
    val seed = seedGenerator.generateSaltSeed()
    valueOrFail(
      AssignmentProcessingSteps.makeFullAssignmentTree(
        pureCrypto,
        seed,
        submitterInfo(submitter),
        contract,
        reassignmentCounter,
        targetDomain,
        targetMediator,
        unassignmentResult,
        uuid,
        Source(testedProtocolVersion),
        Target(testedProtocolVersion),
        reassigningParticipants = reassigningParticipants,
      )
    )("Failed to create FullAssignmentTree")
  }

}
