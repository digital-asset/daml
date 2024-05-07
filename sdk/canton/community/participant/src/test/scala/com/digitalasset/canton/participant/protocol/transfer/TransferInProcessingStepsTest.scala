// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.transfer

import cats.Eval
import cats.implicits.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{CachingConfigs, DefaultProcessingTimeouts}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.data.ViewType.TransferInViewType
import com.digitalasset.canton.data.{CantonTimestamp, FullTransferInTree, TransferSubmitterMetadata}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.metrics.ParticipantTestMetrics
import com.digitalasset.canton.participant.protocol.EngineController.EngineAbortStatus
import com.digitalasset.canton.participant.protocol.conflictdetection.ConflictDetectionHelpers.{
  mkActivenessResult,
  mkActivenessSet,
}
import com.digitalasset.canton.participant.protocol.submission.{
  EncryptedViewMessageFactory,
  InFlightSubmissionTracker,
  SeedGenerator,
}
import com.digitalasset.canton.participant.protocol.transfer.TransferInProcessingSteps.*
import com.digitalasset.canton.participant.protocol.transfer.TransferInValidation.*
import com.digitalasset.canton.participant.protocol.transfer.TransferProcessingSteps.{
  NoTransferSubmissionPermission,
  ParsedTransferRequest,
  StakeholdersMismatch,
  SubmittingPartyMustBeStakeholderIn,
}
import com.digitalasset.canton.participant.protocol.{EngineController, ProcessingStartingPoints}
import com.digitalasset.canton.participant.store.TransferStoreTest.{contract, transactionId1}
import com.digitalasset.canton.participant.store.memory.*
import com.digitalasset.canton.participant.store.{
  MultiDomainEventLog,
  SyncDomainEphemeralState,
  SyncDomainPersistentState,
  TransferStoreTest,
}
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceAlarm
import com.digitalasset.canton.protocol.ExampleTransactionFactory.{submitter, submittingParticipant}
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.store.memory.InMemoryIndexedStringStore
import com.digitalasset.canton.store.{IndexedDomain, SessionKeyStore}
import com.digitalasset.canton.time.{DomainTimeTracker, TimeProofTestUtil, WallClock}
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.version.Transfer.{SourceProtocolVersion, TargetProtocolVersion}
import com.digitalasset.canton.version.{HasTestCloseContext, ProtocolVersion}
import org.scalatest.wordspec.AsyncWordSpec

import java.util.UUID
import scala.concurrent.Future

class TransferInProcessingStepsTest extends AsyncWordSpec with BaseTest with HasTestCloseContext {
  private val sourceDomain = SourceDomainId(
    DomainId(UniqueIdentifier.tryFromProtoPrimitive("domain::source"))
  )
  private val sourceMediator = MediatorGroupRecipient(MediatorGroupIndex.tryCreate(100))
  private val targetDomain = TargetDomainId(
    DomainId(UniqueIdentifier.tryFromProtoPrimitive("domain::target"))
  )
  private val targetMediator = MediatorGroupRecipient(MediatorGroupIndex.tryCreate(200))
  private val anotherDomain = DomainId(UniqueIdentifier.tryFromProtoPrimitive("domain::another"))
  private val anotherMediator = MediatorGroupRecipient(MediatorGroupIndex.tryCreate(300))
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
      LedgerCommandId.assertFromString("transfer-in-processing-steps-command-id"),
      submissionId = None,
      LedgerApplicationId.assertFromString("tests"),
      workflowId = None,
    )
  }

  private lazy val identityFactory = TestingTopology()
    .withDomains(sourceDomain.unwrap)
    .withReversedTopology(
      Map(submittingParticipant -> Map(party1 -> ParticipantPermission.Submission))
    )
    .withSimpleParticipants(participant) // required such that `participant` gets a signing key
    .build(loggerFactory)

  private lazy val cryptoSnapshot =
    identityFactory
      .forOwnerAndDomain(submittingParticipant, sourceDomain.unwrap)
      .currentSnapshotApproximation

  private val clock = new WallClock(timeouts, loggerFactory)
  private val crypto = TestingIdentityFactory.newCrypto(loggerFactory)(participant)

  private val seedGenerator = new SeedGenerator(crypto.pureCrypto)

  private lazy val transferInProcessingSteps =
    testInstance(targetDomain, Set(party1), Set(party1), cryptoSnapshot, None)

  private lazy val indexedStringStore = new InMemoryIndexedStringStore(minIndex = 1, maxIndex = 1)

  private def statefulDependencies
      : Future[(SyncDomainPersistentState, SyncDomainEphemeralState)] = {
    val multiDomainEventLog = mock[MultiDomainEventLog]
    val persistentState =
      new InMemorySyncDomainPersistentState(
        participant,
        clock,
        crypto,
        IndexedDomain.tryCreate(targetDomain.unwrap, 1),
        testedProtocolVersion,
        enableAdditionalConsistencyChecks = true,
        indexedStringStore = indexedStringStore,
        loggerFactory = loggerFactory,
        timeouts = timeouts,
        futureSupervisor = futureSupervisor,
      )
    for {
      _ <- persistentState.parameterStore.setParameters(defaultStaticDomainParameters)
    } yield {
      val state = new SyncDomainEphemeralState(
        participant,
        persistentState,
        Eval.now(multiDomainEventLog),
        mock[InFlightSubmissionTracker],
        ProcessingStartingPoints.default,
        _ => mock[DomainTimeTracker],
        ParticipantTestMetrics.domain,
        CachingConfigs.defaultSessionKeyCacheConfig,
        DefaultProcessingTimeouts.testing,
        loggerFactory = loggerFactory,
        FutureSupervisor.Noop,
      )
      (persistentState, state)
    }
  }

  def mkParsedRequest(
      view: FullTransferInTree,
      recipients: Recipients = RecipientsTest.testInstance,
      signatureO: Option[Signature] = None,
  ): ParsedTransferRequest[FullTransferInTree] = ParsedTransferRequest(
    RequestCounter(1),
    CantonTimestamp.Epoch,
    SequencerCounter(1),
    view,
    recipients,
    signatureO,
    None,
    isFreshOwnTimelyRequest = true,
    transferringParticipant = false,
    Seq.empty,
    targetMediator,
    cryptoSnapshot,
    cryptoSnapshot.ipsSnapshot.findDynamicDomainParameters().futureValue.value,
  )

  "prepare submission" should {
    def setUpOrFail(
        transferData: TransferData,
        transferOutResult: DeliveredTransferOutResult,
        persistentState: SyncDomainPersistentState,
    ): Future[Unit] = {
      for {
        _ <- valueOrFail(persistentState.transferStore.addTransfer(transferData))(
          "add transfer data failed"
        )
        _ <- valueOrFail(persistentState.transferStore.addTransferOutResult(transferOutResult))(
          "add transfer out result failed"
        )
      } yield ()
    }

    val transferId = TransferId(sourceDomain, CantonTimestamp.Epoch)
    val transferDataF =
      TransferStoreTest.mkTransferDataForDomain(transferId, sourceMediator, party1, targetDomain)
    val submissionParam =
      SubmissionParam(
        submitterInfo(party1),
        transferId,
        SourceProtocolVersion(testedProtocolVersion),
      )
    val transferOutResult =
      TransferResultHelpers.transferOutResult(
        sourceDomain,
        cryptoSnapshot,
        participant,
      )

    "succeed without errors" in {
      for {
        transferData <- transferDataF
        deps <- statefulDependencies
        (persistentState, state) = deps
        _ <- setUpOrFail(transferData, transferOutResult, persistentState)
        _preparedSubmission <-
          transferInProcessingSteps
            .createSubmission(
              submissionParam,
              targetMediator,
              state,
              cryptoSnapshot,
            )
            .valueOrFailShutdown("transfer in submission")
      } yield succeed
    }

    "fail when a receiving party has no participant on the domain" in {
      val transferOutRequest = TransferOutRequest(
        submitterInfo(party1),
        Set(party1, party2), // Party 2 is a stakeholder and therefore a receiving party
        Set.empty,
        TransferStoreTest.transactionId1,
        TransferStoreTest.contract,
        transferId.sourceDomain,
        SourceProtocolVersion(testedProtocolVersion),
        sourceMediator,
        targetDomain,
        TargetProtocolVersion(testedProtocolVersion),
        TimeProofTestUtil.mkTimeProof(
          timestamp = CantonTimestamp.Epoch,
          targetDomain = targetDomain,
        ),
        initialTransferCounter,
      )
      val uuid = new UUID(1L, 2L)
      val seed = seedGenerator.generateSaltSeed()
      val transferData2 = {
        val fullTransferOutTree = transferOutRequest
          .toFullTransferOutTree(
            crypto.pureCrypto,
            crypto.pureCrypto,
            seed,
            uuid,
          )
        TransferData(
          SourceProtocolVersion(testedProtocolVersion),
          transferId.transferOutTimestamp,
          RequestCounter(0),
          fullTransferOutTree,
          CantonTimestamp.ofEpochSecond(10),
          contract,
          transactionId1,
          None,
          None,
        )
      }
      for {
        deps <- statefulDependencies
        (persistentState, state) = deps
        _ <- setUpOrFail(transferData2, transferOutResult, persistentState)
        preparedSubmission <- leftOrFailShutdown(
          transferInProcessingSteps.createSubmission(
            submissionParam,
            targetMediator,
            state,
            cryptoSnapshot,
          )
        )("prepare submission did not return a left")
      } yield {
        inside(preparedSubmission) { case NoParticipantForReceivingParty(_, p) =>
          assert(p == party2)
        }
      }
    }

    "fail when transfer-out processing is not yet complete" in {
      for {
        transferData <- transferDataF
        deps <- statefulDependencies
        (persistentState, state) = deps
        _ <- valueOrFail(persistentState.transferStore.addTransfer(transferData))(
          "add transfer data failed"
        )
        preparedSubmission <- leftOrFailShutdown(
          transferInProcessingSteps.createSubmission(
            submissionParam,
            targetMediator,
            state,
            cryptoSnapshot,
          )
        )("prepare submission did not return a left")
      } yield {
        preparedSubmission should matchPattern { case TransferOutIncomplete(_, _) =>
        }
      }
    }

    "fail when submitting party is not a stakeholder" in {
      val submissionParam2 =
        SubmissionParam(
          submitterInfo(party2),
          transferId,
          SourceProtocolVersion(testedProtocolVersion),
        )

      for {
        transferData <- transferDataF
        deps <- statefulDependencies
        (persistentState, state) = deps
        _ <- setUpOrFail(transferData, transferOutResult, persistentState)
        preparedSubmission <- leftOrFailShutdown(
          transferInProcessingSteps.createSubmission(
            submissionParam2,
            targetMediator,
            state,
            cryptoSnapshot,
          )
        )("prepare submission did not return a left")
      } yield {
        preparedSubmission should matchPattern { case SubmittingPartyMustBeStakeholderIn(_, _, _) =>
        }
      }
    }

    "fail when participant does not have submission permission for party" in {

      val failingTopology = TestingTopology(domains = Set(sourceDomain.unwrap))
        .withReversedTopology(
          Map(submittingParticipant -> Map(party1 -> ParticipantPermission.Observation))
        )
        .build(loggerFactory)
      val cryptoSnapshot2 = failingTopology
        .forOwnerAndDomain(participant, sourceDomain.unwrap)
        .currentSnapshotApproximation

      for {
        transferData <- transferDataF
        deps <- statefulDependencies
        (persistentState, state) = deps
        _ <- setUpOrFail(transferData, transferOutResult, persistentState)
        preparedSubmission <- leftOrFailShutdown(
          transferInProcessingSteps.createSubmission(
            submissionParam,
            targetMediator,
            state,
            cryptoSnapshot2,
          )
        )("prepare submission did not return a left")
      } yield {
        preparedSubmission should matchPattern { case NoTransferSubmissionPermission(_, _, _) => }
      }
    }

    "fail when submitting party not hosted on the participant" in {
      val submissionParam2 =
        SubmissionParam(
          submitterInfo(party2),
          transferId,
          SourceProtocolVersion(testedProtocolVersion),
        )
      for {
        transferData2 <- TransferStoreTest.mkTransferDataForDomain(
          transferId,
          sourceMediator,
          party2,
          targetDomain,
        )
        deps <- statefulDependencies
        (persistentState, ephemeralState) = deps
        _ <- setUpOrFail(transferData2, transferOutResult, persistentState)
        preparedSubmission <- leftOrFailShutdown(
          transferInProcessingSteps.createSubmission(
            submissionParam2,
            targetMediator,
            ephemeralState,
            cryptoSnapshot,
          )
        )("prepare submission did not return a left")
      } yield {
        preparedSubmission should matchPattern { case NoTransferSubmissionPermission(_, _, _) =>
        }
      }
    }

    "fail when protocol version are incompatible" in {
      // source domain does not support transfer counters
      val submissionParam2 =
        submissionParam.copy(sourceProtocolVersion = SourceProtocolVersion(ProtocolVersion.v31))
      for {
        transferData <- transferDataF
        deps <- statefulDependencies
        (persistentState, ephemeralState) = deps
        _ <- setUpOrFail(transferData, transferOutResult, persistentState)
        preparedSubmission <-
          transferInProcessingSteps
            .createSubmission(
              submissionParam2,
              targetMediator,
              ephemeralState,
              cryptoSnapshot,
            )
            .value
            .failOnShutdown
      } yield {
        preparedSubmission should matchPattern { case Right(_) => }
      }

    }
  }

  "receive request" should {
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
    val inTree =
      makeFullTransferInTree(
        party1,
        Set(party1),
        contract,
        transactionId1,
        targetDomain,
        targetMediator,
        transferOutResult,
      )

    "succeed without errors" in {
      val sessionKeyStore = SessionKeyStore(CachingConfigs.defaultSessionKeyCacheConfig)
      for {
        inRequest <- encryptFullTransferInTree(inTree, sessionKeyStore)
        envelopes = NonEmpty(
          Seq,
          OpenEnvelope(inRequest, RecipientsTest.testInstance)(testedProtocolVersion),
        )
        decrypted <-
          transferInProcessingSteps
            .decryptViews(envelopes, cryptoSnapshot, sessionKeyStore)
            .valueOrFailShutdown(
              "decrypt request failed"
            )
        (WithRecipients(view, recipients), signature) = decrypted.views.loneElement
        activenessSet =
          transferInProcessingSteps
            .computeActivenessSet(
              mkParsedRequest(
                view,
                recipients,
                signature,
              )
            )
            .value
      } yield {
        decrypted.decryptionErrors shouldBe Seq.empty
        activenessSet shouldBe mkActivenessSet(tfIn = Set(contractId))
      }
    }

    "fail when target domain is not current domain" in {
      val inTree2 = makeFullTransferInTree(
        party1,
        Set(party1),
        contract,
        transactionId1,
        TargetDomainId(anotherDomain),
        anotherMediator,
        transferOutResult,
      )
      val error =
        transferInProcessingSteps.computeActivenessSet(mkParsedRequest(inTree2)).left.value

      inside(error) { case UnexpectedDomain(_, targetD, currentD) =>
        assert(targetD == anotherDomain)
        assert(currentD == targetDomain.unwrap)
      }
    }

    "deduplicate requests with an alarm" in {
      // Send the same transfer-in request twice
      val parsedRequest = mkParsedRequest(inTree)
      val viewWithMetadata = (
        WithRecipients(parsedRequest.fullViewTree, parsedRequest.recipients),
        parsedRequest.signatureO,
      )
      for {
        result <-
          loggerFactory.assertLogs(
            transferInProcessingSteps.computeParsedRequest(
              parsedRequest.rc,
              parsedRequest.requestTimestamp,
              parsedRequest.sc,
              NonEmpty(
                Seq,
                viewWithMetadata,
                viewWithMetadata,
              ),
              parsedRequest.submitterMetadataO,
              parsedRequest.isFreshOwnTimelyRequest,
              parsedRequest.malformedPayloads,
              parsedRequest.mediator,
              parsedRequest.snapshot,
              parsedRequest.domainParameters,
            ),
            _.shouldBeCantonError(
              SyncServiceAlarm,
              _ shouldBe s"Received 2 instead of 1 views in Request ${CantonTimestamp.Epoch}. Discarding all but the first view.",
            ),
          )
      } yield {
        result shouldBe parsedRequest
      }
    }
  }

  "construct pending data and response" should {
    val contractId = ExampleTransactionFactory.suffixedId(10, 0)
    val contract =
      ExampleTransactionFactory.asSerializable(
        contractId,
        contractInstance = ExampleTransactionFactory.contractInstance(),
        metadata = ContractMetadata.tryCreate(Set.empty, Set(party1), None),
      )
    val transferOutResult =
      TransferResultHelpers.transferOutResult(
        sourceDomain,
        cryptoSnapshot,
        submittingParticipant,
      )

    "fail when wrong stakeholders given" in {
      for {
        deps <- statefulDependencies
        (persistentState, ephemeralState) = deps

        // party2 is incorrectly registered as a stakeholder
        fullTransferInTree2 = makeFullTransferInTree(
          party1,
          stakeholders = Set(party1, party2),
          contract,
          transactionId1,
          targetDomain,
          targetMediator,
          transferOutResult,
        )

        transferLookup = ephemeralState.transferCache

        result <- leftOrFail(
          transferInProcessingSteps
            .constructPendingDataAndResponse(
              mkParsedRequest(fullTransferInTree2),
              transferLookup,
              FutureUnlessShutdown.pure(mkActivenessResult()),
              engineController =
                EngineController(participant, RequestId(CantonTimestamp.Epoch), loggerFactory),
            )
            .flatMap(_.confirmationResponsesF)
        )("construction of pending data and response did not return a left").failOnShutdown
      } yield {
        result should matchPattern { case StakeholdersMismatch(_, _, _, _) =>
        }
      }
    }

    "succeed without errors" in {

      for {
        deps <- statefulDependencies
        (_persistentState, ephemeralState) = deps

        transferLookup = ephemeralState.transferCache
        contractLookup = ephemeralState.contractLookup

        fullTransferInTree = makeFullTransferInTree(
          party1,
          Set(party1),
          contract,
          transactionId1,
          targetDomain,
          targetMediator,
          transferOutResult,
        )

        _result <- valueOrFail(
          transferInProcessingSteps
            .constructPendingDataAndResponse(
              mkParsedRequest(fullTransferInTree),
              transferLookup,
              FutureUnlessShutdown.pure(mkActivenessResult()),
              engineController =
                EngineController(participant, RequestId(CantonTimestamp.Epoch), loggerFactory),
            )
        )("construction of pending data and response failed").failOnShutdown
      } yield {
        succeed
      }
    }
  }

  "get commit set and contracts to be stored and event" should {
    "succeed without errors" in {

      val inRes = TransferResultHelpers.transferInResult(targetDomain)

      val contractId = ExampleTransactionFactory.suffixedId(10, 0)
      val contract =
        ExampleTransactionFactory.asSerializable(
          contractId,
          contractInstance = ExampleTransactionFactory.contractInstance(),
          metadata = ContractMetadata.tryCreate(Set(party1), Set(party1), None),
        )
      val transferId = TransferId(sourceDomain, CantonTimestamp.Epoch)
      val rootHash = mock[RootHash]
      when(rootHash.asLedgerTransactionId).thenReturn(LedgerTransactionId.fromString("id1"))
      val pendingRequestData = TransferInProcessingSteps.PendingTransferIn(
        RequestId(CantonTimestamp.Epoch),
        RequestCounter(1),
        SequencerCounter(1),
        rootHash,
        contract,
        initialTransferCounter,
        submitterInfo(submitter),
        transactionId1,
        isTransferringParticipant = false,
        transferId,
        contract.metadata.stakeholders,
        MediatorGroupRecipient(MediatorGroupIndex.one),
        locallyRejectedF = FutureUnlessShutdown.pure(false),
        abortEngine = _ => (),
        engineAbortStatusF = FutureUnlessShutdown.pure(EngineAbortStatus.notAborted),
      )

      for {
        deps <- statefulDependencies
        (_persistentState, state) = deps

        _result <- valueOrFail(
          transferInProcessingSteps.getCommitSetAndContractsToBeStoredAndEvent(
            NoOpeningErrors(
              SignedContent(
                mock[Deliver[DefaultOpenEnvelope]],
                Signature.noSignature,
                None,
                testedProtocolVersion,
              )
            ),
            inRes.verdict,
            pendingRequestData,
            state.pendingTransferInSubmissions,
            crypto.pureCrypto,
          )
        )("get commit set and contracts to be stored and event failed")
      } yield succeed
    }
  }

  private def testInstance(
      targetDomain: TargetDomainId,
      signatories: Set[LfPartyId],
      stakeholders: Set[LfPartyId],
      snapshotOverride: DomainSnapshotSyncCryptoApi,
      awaitTimestampOverride: Option[Future[Unit]],
  ): TransferInProcessingSteps = {

    val pureCrypto = new SymbolicPureCrypto
    val damle = DAMLeTestInstance(participant, signatories, stakeholders)(loggerFactory)
    val seedGenerator = new SeedGenerator(pureCrypto)

    new TransferInProcessingSteps(
      targetDomain,
      submittingParticipant,
      damle,
      TestTransferCoordination.apply(
        Set(),
        CantonTimestamp.Epoch,
        Some(snapshotOverride),
        Some(awaitTimestampOverride),
        loggerFactory,
      ),
      seedGenerator,
      TargetProtocolVersion(testedProtocolVersion),
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
  ): FullTransferInTree = {
    val seed = seedGenerator.generateSaltSeed()

    valueOrFail(
      TransferInProcessingSteps.makeFullTransferInTree(
        crypto.pureCrypto,
        seed,
        submitterInfo(submitter),
        stakeholders,
        contract,
        initialTransferCounter,
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

  private def encryptFullTransferInTree(
      tree: FullTransferInTree,
      sessionKeyStore: SessionKeyStore,
  ): Future[EncryptedViewMessage[TransferInViewType]] =
    EncryptedViewMessageFactory
      .create(TransferInViewType)(tree, cryptoSnapshot, sessionKeyStore, testedProtocolVersion)
      .valueOrFailShutdown("cannot encrypt transfer-in request")
}
