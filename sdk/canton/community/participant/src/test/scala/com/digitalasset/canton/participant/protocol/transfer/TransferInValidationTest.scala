// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.transfer

import cats.implicits.*
import com.digitalasset.canton.*
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.data.{CantonTimestamp, FullTransferInTree, TransferSubmitterMetadata}
import com.digitalasset.canton.participant.protocol.submission.SeedGenerator
import com.digitalasset.canton.participant.protocol.transfer.TransferInValidation.*
import com.digitalasset.canton.participant.protocol.transfer.TransferProcessingSteps.IncompatibleProtocolVersions
import com.digitalasset.canton.participant.store.TransferStoreTest.transactionId1
import com.digitalasset.canton.protocol.ExampleTransactionFactory.submittingParticipant
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.protocol.MediatorGroupRecipient
import com.digitalasset.canton.time.TimeProofTestUtil
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.version.Transfer.{SourceProtocolVersion, TargetProtocolVersion}
import org.scalatest.wordspec.AsyncWordSpec

import java.util.UUID
import scala.concurrent.{Future, Promise}

class TransferInValidationTest
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

  private val initialTransferCounter: TransferCounter = TransferCounter.Genesis

  private def submitterInfo(submitter: LfPartyId): TransferSubmitterMetadata = {
    TransferSubmitterMetadata(
      submitter,
      participant,
      LedgerCommandId.assertFromString("transfer-in-validation-command-id"),
      submissionId = None,
      LedgerApplicationId.assertFromString("tests"),
      workflowId = None,
    )
  }

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

  private val transferInValidation =
    testInstance(targetDomain, Set(party1), Set(party1), cryptoSnapshot, None)

  "validateTransferInRequest" should {
    val contractId = ExampleTransactionFactory.suffixedId(10, 0)
    val contract = ExampleTransactionFactory.asSerializable(
      contractId,
      contractInstance = ExampleTransactionFactory.contractInstance(),
    )
    val transferOutResult =
      TransferResultHelpers.transferOutResult(
        sourceDomain,
        cryptoSnapshot,
        submittingParticipant,
      )
    val inRequest =
      makeFullTransferInTree(
        party1,
        Set(party1),
        contract,
        transactionId1,
        targetDomain,
        targetMediator,
        transferOutResult,
      )

    "succeed without errors in the basic case" in {
      for {
        result <- valueOrFail(
          transferInValidation
            .validateTransferInRequest(
              CantonTimestamp.Epoch,
              inRequest,
              None,
              cryptoSnapshot,
              transferringParticipant = false,
            )
        )("validation of transfer in request failed")
      } yield {
        result shouldBe None
      }
    }

    val transferId = TransferId(sourceDomain, CantonTimestamp.Epoch)
    val transferOutRequest = TransferOutRequest(
      submitterInfo(party1),
      Set(party1, party2), // Party 2 is a stakeholder and therefore a receiving party
      Set.empty,
      ExampleTransactionFactory.transactionId(0),
      contract,
      transferId.sourceDomain,
      SourceProtocolVersion(testedProtocolVersion),
      sourceMediator,
      targetDomain,
      TargetProtocolVersion(testedProtocolVersion),
      TimeProofTestUtil.mkTimeProof(timestamp = CantonTimestamp.Epoch, targetDomain = targetDomain),
      initialTransferCounter,
    )
    val uuid = new UUID(3L, 4L)
    val seed = seedGenerator.generateSaltSeed()
    val fullTransferOutTree = transferOutRequest
      .toFullTransferOutTree(
        pureCrypto,
        pureCrypto,
        seed,
        uuid,
      )
    val transferData =
      TransferData(
        SourceProtocolVersion(testedProtocolVersion),
        CantonTimestamp.Epoch,
        RequestCounter(1),
        fullTransferOutTree,
        CantonTimestamp.Epoch,
        contract,
        transactionId1,
        Some(transferOutResult),
        None,
      )

    "succeed without errors when transfer data is valid" in {
      for {
        result <- valueOrFail(
          transferInValidation
            .validateTransferInRequest(
              CantonTimestamp.Epoch,
              inRequest,
              Some(transferData),
              cryptoSnapshot,
              transferringParticipant = false,
            )
        )("validation of transfer in request failed")
      } yield {
        result match {
          case Some(TransferInValidationResult(confirmingParties)) =>
            assert(confirmingParties == Set(party1))
          case _ => fail()
        }
      }
    }

    "wait for the topology state to be available " in {
      val promise: Promise[Unit] = Promise()
      val transferInProcessingSteps2 =
        testInstance(
          targetDomain,
          Set(party1),
          Set(party1),
          cryptoSnapshot,
          Some(promise.future), // Topology state is not available
        )

      val inValidated = transferInProcessingSteps2
        .validateTransferInRequest(
          CantonTimestamp.Epoch,
          inRequest,
          Some(transferData),
          cryptoSnapshot,
          transferringParticipant = false,
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

    "complain about inconsistent transfer counters" in {
      val inRequestWithWrongCounter = makeFullTransferInTree(
        party1,
        Set(party1),
        contract,
        transactionId1,
        targetDomain,
        targetMediator,
        transferOutResult,
        transferCounter = transferData.transferCounter + 1,
      )
      for {
        result <-
          transferInValidation
            .validateTransferInRequest(
              CantonTimestamp.Epoch,
              inRequestWithWrongCounter,
              Some(transferData),
              cryptoSnapshot,
              transferringParticipant = true,
            )
            .value
      } yield {
        result shouldBe Left(
          InconsistentTransferCounter(
            transferId,
            inRequestWithWrongCounter.transferCounter,
            transferData.transferCounter,
          )
        )
      }
    }

    "disallow transfers from source domain supporting transfer counter to destination domain not supporting them" in {
      val transferDataSourceDomainPVCNTestNet =
        transferData.copy(sourceProtocolVersion = SourceProtocolVersion(ProtocolVersion.v31))
      for {
        result <-
          transferInValidation
            .validateTransferInRequest(
              CantonTimestamp.Epoch,
              inRequest,
              Some(transferDataSourceDomainPVCNTestNet),
              cryptoSnapshot,
              transferringParticipant = true,
            )
            .value
      } yield {
        if (transferOutRequest.targetProtocolVersion.v >= ProtocolVersion.v31) {
          result shouldBe Right(Some(TransferInValidationResult(Set(party1))))
        } else {
          result shouldBe Left(
            IncompatibleProtocolVersions(
              transferDataSourceDomainPVCNTestNet.contract.contractId,
              transferDataSourceDomainPVCNTestNet.sourceProtocolVersion,
              transferOutRequest.targetProtocolVersion,
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
  ): TransferInValidation = {
    val damle = DAMLeTestInstance(participant, signatories, stakeholders)(loggerFactory)

    new TransferInValidation(
      domainId,
      submittingParticipant,
      damle,
      TestTransferCoordination.apply(
        Set(),
        CantonTimestamp.Epoch,
        Some(snapshotOverride),
        Some(awaitTimestampOverride),
        loggerFactory,
      ),
      loggerFactory = loggerFactory,
    )
  }

  private def makeFullTransferInTree(
      submitter: LfPartyId,
      stakeholders: Set[LfPartyId],
      contract: SerializableContract,
      creatingTransactionId: TransactionId,
      targetDomain: TargetDomainId,
      targetMediator: MediatorGroupRecipient,
      transferOutResult: DeliveredTransferOutResult,
      uuid: UUID = new UUID(4L, 5L),
      transferCounter: TransferCounter = initialTransferCounter,
  ): FullTransferInTree = {
    val seed = seedGenerator.generateSaltSeed()
    valueOrFail(
      TransferInProcessingSteps.makeFullTransferInTree(
        pureCrypto,
        seed,
        submitterInfo(submitter),
        stakeholders,
        contract,
        transferCounter,
        creatingTransactionId,
        targetDomain,
        targetMediator,
        transferOutResult,
        uuid,
        SourceProtocolVersion(testedProtocolVersion),
        TargetProtocolVersion(testedProtocolVersion),
      )
    )("Failed to create FullTransferInTree")
  }

}
