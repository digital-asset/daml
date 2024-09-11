// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.implicits.*
import com.digitalasset.canton.*
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.data.{
  CantonTimestamp,
  FullAssignmentTree,
  ReassignmentSubmitterMetadata,
}
import com.digitalasset.canton.participant.protocol.reassignment.AssignmentValidation.*
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.IncompatibleProtocolVersions
import com.digitalasset.canton.participant.protocol.submission.SeedGenerator
import com.digitalasset.canton.participant.store.ReassignmentStoreTest.transactionId1
import com.digitalasset.canton.protocol.ExampleTransactionFactory.submittingParticipant
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.protocol.MediatorGroupRecipient
import com.digitalasset.canton.time.TimeProofTestUtil
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.version.Reassignment.{SourceProtocolVersion, TargetProtocolVersion}
import org.scalatest.wordspec.AsyncWordSpec

import java.util.UUID
import scala.concurrent.{Future, Promise}

class AssignmentValidationTest
    extends AsyncWordSpec
    with BaseTest
    with ProtocolVersionChecksAsyncWordSpec
    with HasActorSystem
    with HasExecutionContext {
  private val sourceDomain = SourceDomainId(
    DomainId(UniqueIdentifier.tryFromProtoPrimitive("domain::source"))
  )
  private val sourceMediator = MediatorGroupRecipient(MediatorGroupIndex.tryCreate(100))
  private val targetDomain = TargetDomainId(
    DomainId(UniqueIdentifier.tryFromProtoPrimitive("domain::target"))
  )
  private val targetMediator = MediatorGroupRecipient(MediatorGroupIndex.tryCreate(200))

  private val party1: LfPartyId = PartyId(
    UniqueIdentifier.tryFromProtoPrimitive("party1::party")
  ).toLf
  private val party2: LfPartyId = PartyId(
    UniqueIdentifier.tryFromProtoPrimitive("party2::party")
  ).toLf

  private val participant = ParticipantId(
    UniqueIdentifier.tryFromProtoPrimitive("bothdomains::participant")
  )

  private val initialReassignmentCounter: ReassignmentCounter = ReassignmentCounter.Genesis

  private def submitterInfo(submitter: LfPartyId): ReassignmentSubmitterMetadata =
    ReassignmentSubmitterMetadata(
      submitter,
      participant,
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
    .withSimpleParticipants(participant) // required such that `participant` gets a signing key
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
    val contractId = ExampleTransactionFactory.suffixedId(10, 0)
    val contract = ExampleTransactionFactory.asSerializable(
      contractId,
      contractInstance = ExampleTransactionFactory.contractInstance(),
    )
    val unassignmentResult =
      ReassignmentResultHelpers.unassignmentResult(
        sourceDomain,
        cryptoSnapshot,
        submittingParticipant,
      )
    val inRequest =
      makeFullAssignmentTree(
        party1,
        Set(party1),
        contract,
        transactionId1,
        targetDomain,
        targetMediator,
        unassignmentResult,
      )

    "succeed without errors in the basic case" in {
      for {
        result <- valueOrFail(
          assignmentValidation
            .validateAssignmentRequest(
              CantonTimestamp.Epoch,
              inRequest,
              None,
              cryptoSnapshot,
              isReassigningParticipant = false,
            )
        )("validation of assignment request failed")
      } yield {
        result shouldBe None
      }
    }

    val reassignmentId = ReassignmentId(sourceDomain, CantonTimestamp.Epoch)
    val unassignmentRequest = UnassignmentRequest(
      submitterInfo(party1),
      Set(party1, party2), // Party 2 is a stakeholder and therefore a receiving party
      Set.empty,
      ExampleTransactionFactory.transactionId(0),
      contract,
      reassignmentId.sourceDomain,
      SourceProtocolVersion(testedProtocolVersion),
      sourceMediator,
      targetDomain,
      TargetProtocolVersion(testedProtocolVersion),
      TimeProofTestUtil.mkTimeProof(timestamp = CantonTimestamp.Epoch, targetDomain = targetDomain),
      initialReassignmentCounter,
    )
    val uuid = new UUID(3L, 4L)
    val seed = seedGenerator.generateSaltSeed()
    val fullUnassignmentTree = unassignmentRequest
      .toFullUnassignmentTree(
        pureCrypto,
        pureCrypto,
        seed,
        uuid,
      )
    val reassignmentData =
      ReassignmentData(
        SourceProtocolVersion(testedProtocolVersion),
        CantonTimestamp.Epoch,
        RequestCounter(1),
        fullUnassignmentTree,
        CantonTimestamp.Epoch,
        contract,
        transactionId1,
        Some(unassignmentResult),
        None,
      )

    "succeed without errors when reassignment data is valid" in {
      for {
        result <- valueOrFail(
          assignmentValidation
            .validateAssignmentRequest(
              CantonTimestamp.Epoch,
              inRequest,
              Some(reassignmentData),
              cryptoSnapshot,
              isReassigningParticipant = false,
            )
        )("validation of assignment request failed")
      } yield {
        result match {
          case Some(AssignmentValidationResult(confirmingParties)) =>
            assert(confirmingParties == Set(party1))
          case _ => fail()
        }
      }
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
          inRequest,
          Some(reassignmentData),
          cryptoSnapshot,
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
      val inRequestWithWrongCounter = makeFullAssignmentTree(
        party1,
        Set(party1),
        contract,
        transactionId1,
        targetDomain,
        targetMediator,
        unassignmentResult,
        reassignmentCounter = reassignmentData.reassignmentCounter + 1,
      )
      for {
        result <-
          assignmentValidation
            .validateAssignmentRequest(
              CantonTimestamp.Epoch,
              inRequestWithWrongCounter,
              Some(reassignmentData),
              cryptoSnapshot,
              isReassigningParticipant = true,
            )
            .value
      } yield {
        result shouldBe Left(
          InconsistentReassignmentCounter(
            reassignmentId,
            inRequestWithWrongCounter.reassignmentCounter,
            reassignmentData.reassignmentCounter,
          )
        )
      }
    }

    "disallow reassignments from source domain supporting reassignment counter to destination domain not supporting them" in {
      val reassignmentDataSourceDomainPVCNTestNet =
        reassignmentData.copy(sourceProtocolVersion = SourceProtocolVersion(ProtocolVersion.v32))
      for {
        result <-
          assignmentValidation
            .validateAssignmentRequest(
              CantonTimestamp.Epoch,
              inRequest,
              Some(reassignmentDataSourceDomainPVCNTestNet),
              cryptoSnapshot,
              isReassigningParticipant = true,
            )
            .value
      } yield {
        if (unassignmentRequest.targetProtocolVersion.v >= ProtocolVersion.v32) {
          result shouldBe Right(Some(AssignmentValidationResult(Set(party1))))
        } else {
          result shouldBe Left(
            IncompatibleProtocolVersions(
              reassignmentDataSourceDomainPVCNTestNet.contract.contractId,
              reassignmentDataSourceDomainPVCNTestNet.sourceProtocolVersion,
              unassignmentRequest.targetProtocolVersion,
            )
          )
        }
      }
    }
  }

  private def testInstance(
      domainId: TargetDomainId,
      signatories: Set[LfPartyId],
      stakeholders: Set[LfPartyId],
      snapshotOverride: DomainSnapshotSyncCryptoApi,
      awaitTimestampOverride: Option[Future[Unit]],
  ): AssignmentValidation = {
    val damle = DAMLeTestInstance(participant, signatories, stakeholders)(loggerFactory)

    new AssignmentValidation(
      domainId,
      defaultStaticDomainParameters,
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
      submitter: LfPartyId,
      stakeholders: Set[LfPartyId],
      contract: SerializableContract,
      creatingTransactionId: TransactionId,
      targetDomain: TargetDomainId,
      targetMediator: MediatorGroupRecipient,
      unassignmentResult: DeliveredUnassignmentResult,
      uuid: UUID = new UUID(4L, 5L),
      reassignmentCounter: ReassignmentCounter = initialReassignmentCounter,
  ): FullAssignmentTree = {
    val seed = seedGenerator.generateSaltSeed()
    valueOrFail(
      AssignmentProcessingSteps.makeFullAssignmentTree(
        pureCrypto,
        seed,
        submitterInfo(submitter),
        stakeholders,
        contract,
        reassignmentCounter,
        creatingTransactionId,
        targetDomain,
        targetMediator,
        unassignmentResult,
        uuid,
        SourceProtocolVersion(testedProtocolVersion),
        TargetProtocolVersion(testedProtocolVersion),
      )
    )("Failed to create FullAssignmentTree")
  }

}
