// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.syntax.functor.*
import com.digitalasset.canton.*
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.data.ReassignmentRef.ReassignmentIdRef
import com.digitalasset.canton.data.{
  CantonTimestamp,
  ContractsReassignmentBatch,
  FullAssignmentTree,
  ReassignmentRef,
  ReassignmentSubmitterMetadata,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.protocol.conflictdetection.ConflictDetectionHelpers.mkActivenessResult
import com.digitalasset.canton.participant.protocol.reassignment.AssignmentValidationError.{
  ContractDataMismatch,
  InconsistentReassignmentCounters,
  NonInitiatorSubmitsBeforeExclusivityTimeout,
}
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.ParsedReassignmentRequest
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentValidationError.{
  ReassigningParticipantsMismatch,
  SubmitterMustBeStakeholder,
}
import com.digitalasset.canton.participant.protocol.submission.SeedGenerator
import com.digitalasset.canton.participant.protocol.{ContractAuthenticator, EngineController}
import com.digitalasset.canton.participant.store.ReassignmentStore.UnknownReassignmentId
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.sequencing.protocol.{
  MediatorGroupRecipient,
  Recipients,
  RecipientsTest,
}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import org.scalatest.wordspec.AsyncWordSpec

import java.util.UUID
import scala.concurrent.Future

class AssignmentValidationTest
    extends AsyncWordSpec
    with BaseTest
    with ProtocolVersionChecksAsyncWordSpec
    with HasActorSystem
    with HasExecutionContext
    with FailOnShutdown {
  private val sourceSynchronizer = Source(
    SynchronizerId(UniqueIdentifier.tryFromProtoPrimitive("synchronizer::source")).toPhysical
  )
  private val sourceMediator = MediatorGroupRecipient(MediatorGroupIndex.tryCreate(0))
  private val targetSynchronizer = Target(
    SynchronizerId(UniqueIdentifier.tryFromProtoPrimitive("synchronizer::target")).toPhysical
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
    UniqueIdentifier.tryFromProtoPrimitive("bothsynchronizers::participant")
  )
  private val observingParticipant = ParticipantId(
    UniqueIdentifier.tryFromProtoPrimitive("bothsynchronizers::observingParticipant")
  )

  private val otherParticipant = ParticipantId(
    UniqueIdentifier.tryFromProtoPrimitive("synchronizer::participant")
  )

  private def submitterInfo(submitter: LfPartyId): ReassignmentSubmitterMetadata =
    ReassignmentSubmitterMetadata(
      submitter,
      submittingParticipant,
      LedgerCommandId.assertFromString("assignment-validation-command-id"),
      submissionId = None,
      LedgerUserId.assertFromString("tests"),
      workflowId = None,
    )

  private val identityFactory = TestingTopology()
    .withSynchronizers(sourceSynchronizer.unwrap)
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
      .forOwnerAndSynchronizer(submittingParticipant, sourceSynchronizer.unwrap)
      .currentSnapshotApproximation

  private val pureCrypto = new SymbolicPureCrypto

  private val seedGenerator = new SeedGenerator(pureCrypto)

  private def assignmentValidation(participantId: ParticipantId = submittingParticipant) =
    testInstance(targetSynchronizer, cryptoSnapshot, None, participantId)

  private val activenessF = FutureUnlessShutdown.pure(mkActivenessResult())

  val engineController =
    EngineController(submittingParticipant, RequestId(CantonTimestamp.Epoch), loggerFactory)

  private def mkParsedRequest(
      view: FullAssignmentTree,
      recipients: Recipients = RecipientsTest.testInstance,
  ): ParsedReassignmentRequest[FullAssignmentTree] = {
    val signature = cryptoSnapshot
      .sign(view.rootHash.unwrap, SigningKeyUsage.ProtocolOnly)
      .futureValueUS
      .toOption

    ParsedReassignmentRequest(
      RequestCounter(1),
      CantonTimestamp.Epoch,
      SequencerCounter(1),
      view,
      recipients,
      signature,
      None,
      isFreshOwnTimelyRequest = true,
      areContractsUnknown = false,
      Seq.empty,
      targetMediator,
      cryptoSnapshot,
      cryptoSnapshot.ipsSnapshot.findDynamicSynchronizerParameters().futureValueUS.value,
    )
  }

  "validateAssignmentRequest" should {
    val contract = ExampleTransactionFactory.authenticatedSerializableContract(
      ContractMetadata.tryCreate(
        signatories = Set(signatory),
        stakeholders = Set(signatory, observer),
        None,
      )
    )

    val reassignmentId = ReassignmentId(sourceSynchronizer, UnassignId(TestHash.digest(0)))

    val reassignmentDataHelpers = ReassignmentDataHelpers(
      contract,
      reassignmentId.sourceSynchronizer.map(_.toPhysical),
      targetSynchronizer,
      identityFactory,
    )

    val unassignmentRequest =
      reassignmentDataHelpers.unassignmentRequest(
        signatory,
        submittingParticipant,
        sourceMediator,
      )(reassigningParticipants = reassigningParticipants)

    val unassignmentData =
      reassignmentDataHelpers.unassignmentData(reassignmentId, unassignmentRequest)

    val assignmentRequest = makeFullAssignmentTree(
      reassignmentId,
      contract,
    )

    "succeed without errors in the basic case (no reassignment data) on a non reassigning Participant" in {
      val res = assignmentValidation(otherParticipant)
        .perform(
          Target(cryptoSnapshot),
          unassignmentDataE = Left(UnknownReassignmentId(reassignmentId)),
          activenessF = activenessF,
        )(mkParsedRequest(assignmentRequest))
        .futureValueUS
        .value

      res.isSuccessfulF.futureValueUS shouldBe true

    }

    "succeed without errors when reassignment data is valid" in {
      def validate(
          participantId: ParticipantId
      ): AssignmentValidationResult =
        assignmentValidation(participantId)
          .perform(
            Target(cryptoSnapshot),
            unassignmentDataE = Right(unassignmentData),
            activenessF = activenessF,
          )(mkParsedRequest(assignmentRequest))
          .futureValueUS
          .value

      validate(submittingParticipant).isSuccessfulF.futureValueUS shouldBe true

      validate(otherParticipant).isSuccessfulF.futureValueUS shouldBe true
    }

    "complain about inconsistent reassignment counters" in {
      val rightCounters = unassignmentData.contracts.contractIdCounters.toMap
      val wrongCounters = rightCounters.view.mapValues(_ + 1).toMap
      val assignmentTreeWrongCounter = makeFullAssignmentTree(
        unassignmentData.reassignmentId,
        contract,
        reassignmentCounter = wrongCounters(contract.contractId),
      )

      val result = assignmentValidation()
        .perform(
          Target(cryptoSnapshot),
          unassignmentDataE = Right(unassignmentData),
          activenessF = activenessF,
        )(mkParsedRequest(assignmentTreeWrongCounter))
        .value
        .futureValueUS
        .value

      result.isSuccessfulF.futureValueUS shouldBe false
      result.reassigningParticipantValidationResult should contain(
        InconsistentReassignmentCounters(
          reassignmentId,
          wrongCounters,
          rightCounters,
        )
      )
    }

    "detect inconsistent contract data" in {
      def validate(cid: LfContractId): Either[
        ReassignmentProcessingSteps.ReassignmentProcessorError,
        AssignmentValidationResult,
      ] = {
        val updatedContract = contract.copy(contractId = cid)

        val assignmentRequest = makeFullAssignmentTree(
          unassignmentData.reassignmentId,
          updatedContract,
        )

        assignmentValidation()
          .perform(
            Target(cryptoSnapshot),
            unassignmentDataE = Right(unassignmentData),
            activenessF = activenessF,
          )(mkParsedRequest(assignmentRequest))
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

      validate(contract.contractId).value shouldBe a[AssignmentValidationResult]

      // The data differs from the one stored locally in ReassignmentData
      validate(
        unauthenticatedContractId
      ).value.reassigningParticipantValidationResult.head shouldBe a[
        ContractDataMismatch
      ]
    }

    "detect reassigning participant mismatch" in {
      def validate(reassigningParticipants: Set[ParticipantId]) = {
        val assignmentTree = makeFullAssignmentTree(
          unassignmentData.reassignmentId,
          contract,
          reassigningParticipants = reassigningParticipants,
        )

        assignmentValidation()
          .perform(
            Target(cryptoSnapshot),
            unassignmentDataE = Right(unassignmentData),
            activenessF = activenessF,
          )(mkParsedRequest(assignmentTree))
          .value
          .futureValueUS
      }

      // Happy path / control
      validate(reassigningParticipants).value.isSuccessfulF.futureValueUS shouldBe true

      // Additional observing participant
      val additionalObservingParticipant = reassigningParticipants + otherParticipant

      validate(
        additionalObservingParticipant
      ).value.reassigningParticipantValidationResult shouldBe Seq(
        ReassigningParticipantsMismatch(
          ReassignmentRef(unassignmentData.reassignmentId),
          expected = reassigningParticipants,
          declared = additionalObservingParticipant,
        )
      )

      // Additional confirming participant
      val additionalConfirmingParticipant = reassigningParticipants + otherParticipant

      validate(
        additionalConfirmingParticipant
      ).value.reassigningParticipantValidationResult shouldBe Seq(
        ReassigningParticipantsMismatch(
          ReassignmentRef(unassignmentData.reassignmentId),
          expected = reassigningParticipants,
          declared = additionalConfirmingParticipant,
        )
      )

      // Empty reassigning participants means it's not a reassigning participant.
      validate(Set.empty).value.reassigningParticipantValidationResult shouldBe Nil
    }

    "detect non-stakeholder submitter" in {
      def validate(submitter: LfPartyId) = {
        val assignmentRequest = makeFullAssignmentTree(
          unassignmentData.reassignmentId,
          contract,
          submitter = submitter,
        )

        assignmentValidation()
          .perform(
            Target(cryptoSnapshot),
            unassignmentDataE = Right(unassignmentData),
            activenessF = activenessF,
          )(mkParsedRequest(assignmentRequest))
          .value
          .futureValueUS
      }

      // Happy path / control
      validate(signatory).value.isSuccessfulF.futureValueUS shouldBe true

      validate(otherParty).value.submitterCheckResult shouldBe Some(
        SubmitterMustBeStakeholder(
          ReassignmentIdRef(unassignmentData.reassignmentId),
          submittingParty = otherParty,
          stakeholders = assignmentRequest.stakeholders.all,
        )
      )

      validate(otherParty).value.reassigningParticipantValidationResult.loneElement shouldBe a[
        NonInitiatorSubmitsBeforeExclusivityTimeout
      ]

    }
  }

  private def testInstance(
      synchronizerId: Target[PhysicalSynchronizerId],
      snapshotOverride: SynchronizerSnapshotSyncCryptoApi,
      awaitTimestampOverride: Option[Future[Unit]],
      participantId: ParticipantId,
  ): AssignmentValidation = {

    val contractAuthenticator = ContractAuthenticator(new SymbolicPureCrypto())

    new AssignmentValidation(
      synchronizerId,
      Target(defaultStaticSynchronizerParameters),
      participantId,
      TestReassignmentCoordination.apply(
        Set(),
        CantonTimestamp.Epoch,
        Some(snapshotOverride),
        Some(awaitTimestampOverride),
        loggerFactory,
      ),
      loggerFactory = loggerFactory,
      contractAuthenticator = contractAuthenticator,
    )
  }

  private def makeFullAssignmentTree(
      reassignmentId: ReassignmentId,
      contract: SerializableContract,
      submitter: LfPartyId = signatory,
      uuid: UUID = new UUID(4L, 5L),
      targetSynchronizer: Target[PhysicalSynchronizerId] = targetSynchronizer,
      targetMediator: MediatorGroupRecipient = targetMediator,
      reassignmentCounter: ReassignmentCounter = ReassignmentCounter(1),
      reassigningParticipants: Set[ParticipantId] = reassigningParticipants,
  ): FullAssignmentTree = {
    val seed = seedGenerator.generateSaltSeed()
    valueOrFail(
      AssignmentProcessingSteps.makeFullAssignmentTree(
        pureCrypto,
        seed,
        reassignmentId,
        submitterInfo(submitter),
        ContractsReassignmentBatch(contract, reassignmentCounter),
        targetSynchronizer,
        targetMediator,
        uuid,
        Target(testedProtocolVersion),
        reassigningParticipants = reassigningParticipants,
      )
    )("Failed to create FullAssignmentTree")
  }

}
