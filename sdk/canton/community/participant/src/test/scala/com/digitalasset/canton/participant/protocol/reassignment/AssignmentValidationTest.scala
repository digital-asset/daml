// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import com.digitalasset.canton.*
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.data.{
  CantonTimestamp,
  FullAssignmentTree,
  ReassignmentSubmitterMetadata,
}
import com.digitalasset.canton.participant.protocol.SerializableContractAuthenticator
import com.digitalasset.canton.participant.protocol.reassignment.AssignmentValidation.*
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.{
  AssignmentSubmitterMustBeStakeholder,
  ContractError,
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
    with HasExecutionContext {
  private val sourceDomain = Source(
    DomainId(UniqueIdentifier.tryFromProtoPrimitive("domain::source"))
  )
  private val sourceMediator = MediatorGroupRecipient(MediatorGroupIndex.tryCreate(0))
  private val targetDomain = Target(
    DomainId(UniqueIdentifier.tryFromProtoPrimitive("domain::target"))
  )
  private val targetMediator = MediatorGroupRecipient(MediatorGroupIndex.tryCreate(0))

  private val party1: LfPartyId = PartyId(
    UniqueIdentifier.tryFromProtoPrimitive("party1::party")
  ).toLf

  private val party2: LfPartyId = PartyId(
    UniqueIdentifier.tryFromProtoPrimitive("party2::party")
  ).toLf

  private val submittingParticipant = ParticipantId(
    UniqueIdentifier.tryFromProtoPrimitive("bothdomains::participant")
  )

  private val otherParticipant = ParticipantId(
    UniqueIdentifier.tryFromProtoPrimitive("domain::participant")
  )

  private val initialReassignmentCounter: ReassignmentCounter = ReassignmentCounter.Genesis

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
      Map(submittingParticipant -> Map(party1 -> ParticipantPermission.Submission))
    )
    .withSimpleParticipants(
      submittingParticipant
    ) // required such that `participant` gets a signing key
    .build(loggerFactory)

  private val cryptoSnapshot =
    identityFactory
      .forOwnerAndDomain(submittingParticipant, sourceDomain.unwrap)
      .currentSnapshotApproximation

  private val pureCrypto = new SymbolicPureCrypto

  private val seedGenerator = new SeedGenerator(pureCrypto)

  private val assignmentValidation =
    testInstance(targetDomain, Set(party1), Set(party1), cryptoSnapshot, None)

  "validateAssignmentRequest" should {
    val contract = ExampleTransactionFactory.authenticatedSerializableContract(
      ContractMetadata.tryCreate(signatories = Set(party1), stakeholders = Set(party1), None)
    )

    val reassignmentId = ReassignmentId(sourceDomain, CantonTimestamp.Epoch)

    val reassignmentDataHelpers = new ReassignmentDataHelpers(
      contract,
      reassignmentId.sourceDomain,
      targetDomain,
      identityFactory,
    )

    val unassignmentRequest =
      reassignmentDataHelpers.unassignmentRequest(party1, submittingParticipant, sourceMediator)()

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
          isReassigningParticipant = false,
        )
        .futureValue shouldBe None
    }

    "succeed without errors when reassignment data is valid" in {
      assignmentValidation
        .validateAssignmentRequest(
          CantonTimestamp.Epoch,
          assignmentRequest,
          reassignmentDataO = Some(reassignmentData),
          Target(cryptoSnapshot),
          isReassigningParticipant = false,
        )
        .futureValue
        .value
        .confirmingParties shouldBe Set(party1)
    }

    "wait for the topology state to be available " in {
      val promise: Promise[Unit] = Promise()
      val assignmentProcessingSteps2 =
        testInstance(
          targetDomain,
          Set(party1),
          Set(party1),
          cryptoSnapshot,
          Some(promise.future), // Topology state is not available
        )

      val inValidated = assignmentProcessingSteps2
        .validateAssignmentRequest(
          CantonTimestamp.Epoch,
          assignmentRequest,
          Some(reassignmentData),
          Target(cryptoSnapshot),
          isReassigningParticipant = false,
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
        creatingTransactionId = ExampleTransactionFactory.transactionId(0),
      )

      assignmentValidation
        .validateAssignmentRequest(
          CantonTimestamp.Epoch,
          assignmentTreeWrongCounter,
          Some(reassignmentData),
          Target(cryptoSnapshot),
          isReassigningParticipant = true,
        )
        .value
        .futureValue
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
            isReassigningParticipant = false,
          )
          .value
          .futureValue
      }

      val unauthenticatedContractId = ExampleTransactionFactory
        .authenticatedSerializableContract(
          metadata = ContractMetadata
            .tryCreate(signatories = Set(party1), stakeholders = Set(party1, party2), None)
        )
        .contractId

      validate(contract.contractId).value.value shouldBe a[AssignmentValidationResult]

      // The data differs from the one stored locally in ReassignmentData
      validate(unauthenticatedContractId).left.value shouldBe a[ContractDataMismatch]
    }

    "detect invalid contract id" in {
      def validate(cid: LfContractId, reassignmentDataDefined: Boolean) = {
        val updatedContract = contract.copy(contractId = cid)

        val reassignmentDataHelpers = new ReassignmentDataHelpers(
          updatedContract,
          reassignmentId.sourceDomain,
          targetDomain,
          identityFactory,
        )

        val unassignmentRequest =
          reassignmentDataHelpers.unassignmentRequest(
            party1,
            submittingParticipant,
            sourceMediator,
          )()

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
            isReassigningParticipant = true,
          )
          .value
          .futureValue
      }

      val unauthenticatedContractId = ExampleTransactionFactory
        .authenticatedSerializableContract(
          metadata = ContractMetadata
            .tryCreate(signatories = Set(party1), stakeholders = Set(party1, party2), None)
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
          creatingTransactionId = ExampleTransactionFactory.transactionId(0),
        )

        assignmentValidation
          .validateAssignmentRequest(
            CantonTimestamp.Epoch,
            assignmentTree,
            Some(reassignmentData),
            Target(cryptoSnapshot),
            isReassigningParticipant = false,
          )
          .value
          .futureValue
      }

      // Happy path / control
      validate(Set(submittingParticipant)).value.value.confirmingParties shouldBe Set(party1)

      validate(Set(otherParticipant)).left.value shouldBe ReassigningParticipantsMismatch(
        unassignmentResult.reassignmentId,
        expected = Set(submittingParticipant),
        declared = Set(otherParticipant),
      )

      validate(Set()).left.value shouldBe ReassigningParticipantsMismatch(
        unassignmentResult.reassignmentId,
        expected = Set(submittingParticipant),
        declared = Set(),
      )
    }

    "detect non-stakeholder submitter" in {
      def validate(submitter: LfPartyId) = {
        val assignmentRequest = makeFullAssignmentTree(
          contract,
          unassignmentResult,
          submitter = submitter,
          creatingTransactionId = ExampleTransactionFactory.transactionId(0),
        )

        assignmentValidation
          .validateAssignmentRequest(
            CantonTimestamp.Epoch,
            assignmentRequest,
            Some(reassignmentData),
            Target(cryptoSnapshot),
            isReassigningParticipant = false,
          )
          .value
          .futureValue
      }

      // Happy path / control
      validate(party1).value.value.confirmingParties shouldBe Set(party1)

      validate(party2).left.value shouldBe AssignmentSubmitterMustBeStakeholder(
        unassignmentResult.reassignmentId,
        submittingParty = party2,
        stakeholders = Set(party1),
      )
    }
  }

  private def testInstance(
      domainId: Target[DomainId],
      signatories: Set[LfPartyId],
      stakeholders: Set[LfPartyId],
      snapshotOverride: DomainSnapshotSyncCryptoApi,
      awaitTimestampOverride: Option[Future[Unit]],
  ): AssignmentValidation = {
    val damle = DAMLeTestInstance(submittingParticipant, signatories, stakeholders)(loggerFactory)

    new AssignmentValidation(
      domainId,
      SerializableContractAuthenticator(pureCrypto),
      Target(defaultStaticDomainParameters),
      submittingParticipant,
      damle,
      TestReassignmentCoordination.apply(
        Set(),
        CantonTimestamp.Epoch,
        Some(snapshotOverride),
        Some(awaitTimestampOverride),
        loggerFactory,
      ),
      loggerFactory = loggerFactory,
    )
  }

  private def makeFullAssignmentTree(
      contract: SerializableContract,
      unassignmentResult: DeliveredUnassignmentResult,
      submitter: LfPartyId = party1,
      creatingTransactionId: TransactionId = ExampleTransactionFactory.transactionId(0),
      uuid: UUID = new UUID(4L, 5L),
      targetDomain: Target[DomainId] = targetDomain,
      targetMediator: MediatorGroupRecipient = targetMediator,
      reassignmentCounter: ReassignmentCounter = initialReassignmentCounter,
      reassigningParticipants: Set[ParticipantId] = Set(submittingParticipant),
  ): FullAssignmentTree = {
    val seed = seedGenerator.generateSaltSeed()
    valueOrFail(
      AssignmentProcessingSteps.makeFullAssignmentTree(
        pureCrypto,
        seed,
        submitterInfo(submitter),
        contract,
        reassignmentCounter,
        creatingTransactionId,
        targetDomain,
        targetMediator,
        unassignmentResult,
        uuid,
        Source(testedProtocolVersion),
        Target(testedProtocolVersion),
        confirmingReassigningParticipants = reassigningParticipants,
      )
    )("Failed to create FullAssignmentTree")
  }

}
