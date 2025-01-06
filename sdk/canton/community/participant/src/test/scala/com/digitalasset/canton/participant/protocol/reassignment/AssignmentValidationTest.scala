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
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.protocol.EngineController
import com.digitalasset.canton.participant.protocol.conflictdetection.ConflictDetectionHelpers.mkActivenessResult
import com.digitalasset.canton.participant.protocol.reassignment.AssignmentValidationError.{
  ContractDataMismatch,
  InconsistentReassignmentCounter,
  NonInitiatorSubmitsBeforeExclusivityTimeout,
}
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.ParsedReassignmentRequest
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentValidationError.{
  ReassigningParticipantsMismatch,
  SubmitterMustBeStakeholder,
}
import com.digitalasset.canton.participant.protocol.submission.SeedGenerator
import com.digitalasset.canton.participant.store.ReassignmentStore.UnknownReassignmentId
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.*
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
import scala.concurrent.{Future, Promise}

class AssignmentValidationTest
    extends AsyncWordSpec
    with BaseTest
    with ProtocolVersionChecksAsyncWordSpec
    with HasActorSystem
    with HasExecutionContext
    with FailOnShutdown {
  private val sourceDomain = Source(
    SynchronizerId(UniqueIdentifier.tryFromProtoPrimitive("domain::source"))
  )
  private val sourceMediator = MediatorGroupRecipient(MediatorGroupIndex.tryCreate(0))
  private val targetDomain = Target(
    SynchronizerId(UniqueIdentifier.tryFromProtoPrimitive("domain::target"))
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

  private def assignmentValidation(participantId: ParticipantId = submittingParticipant) =
    testInstance(targetDomain, cryptoSnapshot, None, participantId)

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
      Seq.empty,
      targetMediator,
      cryptoSnapshot,
      cryptoSnapshot.ipsSnapshot.findDynamicDomainParameters().futureValueUS.value,
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

    "succeed without errors in the basic case (no reassignment data) on a non reassigning Participant" in {
      val res = assignmentValidation(otherParticipant)
        .perform(
          Target(cryptoSnapshot),
          reassignmentDataE = Left(UnknownReassignmentId(reassignmentId)),
          activenessF = activenessF,
          engineController,
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
            reassignmentDataE = Right(reassignmentData),
            activenessF = activenessF,
            engineController,
          )(mkParsedRequest(assignmentRequest))
          .futureValueUS
          .value

      validate(submittingParticipant).isSuccessfulF.futureValueUS shouldBe true

      validate(otherParticipant).isSuccessfulF.futureValueUS shouldBe true
    }

    "wait for the topology state to be available" in {
      val promise: Promise[Unit] = Promise()
      val assignmentProcessingSteps2 =
        testInstance(
          targetDomain,
          cryptoSnapshot,
          Some(promise.future), // Topology state is not available
          submittingParticipant,
        )

      val inValidated = assignmentProcessingSteps2
        .perform(
          Target(cryptoSnapshot),
          reassignmentDataE = Right(reassignmentData),
          activenessF = activenessF,
          engineController,
        )(mkParsedRequest(assignmentRequest))
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

      val result = assignmentValidation()
        .perform(
          Target(cryptoSnapshot),
          reassignmentDataE = Right(reassignmentData),
          activenessF = activenessF,
          engineController,
        )(mkParsedRequest(assignmentTreeWrongCounter))
        .value
        .futureValueUS
        .value

      result.isSuccessfulF.futureValueUS shouldBe false
      result.validationErrors shouldBe Seq(
        InconsistentReassignmentCounter(
          reassignmentId,
          assignmentTreeWrongCounter.reassignmentCounter,
          reassignmentData.reassignmentCounter,
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
          updatedContract,
          unassignmentResult,
        )

        assignmentValidation()
          .perform(
            Target(cryptoSnapshot),
            reassignmentDataE = Right(reassignmentData),
            activenessF = activenessF,
            engineController,
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
      validate(unauthenticatedContractId).value.validationErrors.head shouldBe a[
        ContractDataMismatch
      ]
    }

    "detect reassigning participant mismatch" in {
      def validate(reassigningParticipants: Set[ParticipantId]) = {
        val assignmentTree = makeFullAssignmentTree(
          contract,
          unassignmentResult,
          reassigningParticipants = reassigningParticipants,
        )

        assignmentValidation()
          .perform(
            Target(cryptoSnapshot),
            reassignmentDataE = Right(reassignmentData),
            activenessF = activenessF,
            engineController,
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
      ).value.validationErrors shouldBe Seq(
        ReassigningParticipantsMismatch(
          ReassignmentRef(unassignmentResult.reassignmentId),
          expected = reassigningParticipants,
          declared = additionalObservingParticipant,
        )
      )

      // Additional confirming participant
      val additionalConfirmingParticipant = reassigningParticipants + otherParticipant

      validate(
        additionalConfirmingParticipant
      ).value.validationErrors shouldBe Seq(
        ReassigningParticipantsMismatch(
          ReassignmentRef(unassignmentResult.reassignmentId),
          expected = reassigningParticipants,
          declared = additionalConfirmingParticipant,
        )
      )

      // Empty reassigning participants means it's not a reassigning participant.
      validate(Set.empty).value.validationErrors shouldBe Nil
    }

    "detect non-stakeholder submitter" in {
      def validate(submitter: LfPartyId) = {
        val assignmentRequest = makeFullAssignmentTree(
          contract,
          unassignmentResult,
          submitter = submitter,
        )

        assignmentValidation()
          .perform(
            Target(cryptoSnapshot),
            reassignmentDataE = Right(reassignmentData),
            activenessF = activenessF,
            engineController,
          )(mkParsedRequest(assignmentRequest))
          .value
          .futureValueUS
      }

      // Happy path / control
      validate(signatory).value.isSuccessfulF.futureValueUS shouldBe true

      validate(otherParty).value.validationErrors.map(_.getClass) shouldBe Seq(
        classOf[NonInitiatorSubmitsBeforeExclusivityTimeout],
        classOf[SubmitterMustBeStakeholder],
      )
    }
  }

  private def testInstance(
      synchronizerId: Target[SynchronizerId],
      snapshotOverride: DomainSnapshotSyncCryptoApi,
      awaitTimestampOverride: Option[Future[Unit]],
      participantId: ParticipantId,
  ): AssignmentValidation = {
    val damle = DAMLeTestInstance(
      submittingParticipant,
      Set(signatory),
      Set(signatory, observer),
    )(loggerFactory)

    new AssignmentValidation(
      synchronizerId,
      Target(defaultStaticDomainParameters),
      participantId,
      TestReassignmentCoordination.apply(
        Set(),
        CantonTimestamp.Epoch,
        Some(snapshotOverride),
        Some(awaitTimestampOverride),
        loggerFactory,
      ),
      engine = damle,
      loggerFactory = loggerFactory,
    )
  }

  private def makeFullAssignmentTree(
      contract: SerializableContract,
      unassignmentResult: DeliveredUnassignmentResult,
      submitter: LfPartyId = signatory,
      uuid: UUID = new UUID(4L, 5L),
      targetDomain: Target[SynchronizerId] = targetDomain,
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
