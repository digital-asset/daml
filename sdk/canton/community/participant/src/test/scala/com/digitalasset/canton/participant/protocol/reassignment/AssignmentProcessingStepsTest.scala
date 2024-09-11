// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.Eval
import cats.implicits.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{CachingConfigs, DefaultProcessingTimeouts}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.provider.symbolic.{SymbolicCrypto, SymbolicPureCrypto}
import com.digitalasset.canton.data.ViewType.AssignmentViewType
import com.digitalasset.canton.data.{
  CantonTimestamp,
  FullAssignmentTree,
  ReassignmentSubmitterMetadata,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.admin.PackageDependencyResolver
import com.digitalasset.canton.participant.ledger.api.{LedgerApiIndexer, LedgerApiStore}
import com.digitalasset.canton.participant.metrics.ParticipantTestMetrics
import com.digitalasset.canton.participant.protocol.EngineController.EngineAbortStatus
import com.digitalasset.canton.participant.protocol.conflictdetection.ConflictDetectionHelpers.{
  mkActivenessResult,
  mkActivenessSet,
}
import com.digitalasset.canton.participant.protocol.reassignment.AssignmentProcessingSteps.*
import com.digitalasset.canton.participant.protocol.reassignment.AssignmentValidation.*
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.{
  NoReassignmentSubmissionPermission,
  ParsedReassignmentRequest,
  StakeholdersMismatch,
  SubmittingPartyMustBeStakeholderIn,
}
import com.digitalasset.canton.participant.protocol.submission.{
  EncryptedViewMessageFactory,
  SeedGenerator,
}
import com.digitalasset.canton.participant.protocol.{EngineController, ProcessingStartingPoints}
import com.digitalasset.canton.participant.store.ReassignmentStoreTest.{contract, transactionId1}
import com.digitalasset.canton.participant.store.memory.*
import com.digitalasset.canton.participant.store.{
  ParticipantNodeEphemeralState,
  ReassignmentStoreTest,
  SyncDomainEphemeralState,
  SyncDomainPersistentState,
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
import com.digitalasset.canton.version.HasTestCloseContext
import com.digitalasset.canton.version.Reassignment.{SourceProtocolVersion, TargetProtocolVersion}
import org.scalatest.wordspec.AsyncWordSpec

import java.util.UUID
import scala.concurrent.Future

class AssignmentProcessingStepsTest
    extends AsyncWordSpec
    with BaseTest
    with HasTestCloseContext
    with HasExecutionContext {
  private lazy val sourceDomain = SourceDomainId(
    DomainId(UniqueIdentifier.tryFromProtoPrimitive("domain::source"))
  )
  private lazy val sourceMediator = MediatorGroupRecipient(MediatorGroupIndex.tryCreate(100))
  private lazy val targetDomain = TargetDomainId(
    DomainId(UniqueIdentifier.tryFromProtoPrimitive("domain::target"))
  )
  private lazy val targetMediator = MediatorGroupRecipient(MediatorGroupIndex.tryCreate(200))
  private lazy val anotherDomain = DomainId(
    UniqueIdentifier.tryFromProtoPrimitive("domain::another")
  )
  private lazy val anotherMediator = MediatorGroupRecipient(MediatorGroupIndex.tryCreate(300))
  private lazy val party1: LfPartyId = PartyId(
    UniqueIdentifier.tryFromProtoPrimitive("party1::party")
  ).toLf
  private lazy val party2: LfPartyId = PartyId(
    UniqueIdentifier.tryFromProtoPrimitive("party2::party")
  ).toLf

  private lazy val participant = ParticipantId(
    UniqueIdentifier.tryFromProtoPrimitive("bothdomains::participant")
  )

  private lazy val initialReassignmentCounter: ReassignmentCounter = ReassignmentCounter.Genesis

  private def submitterInfo(submitter: LfPartyId): ReassignmentSubmitterMetadata =
    ReassignmentSubmitterMetadata(
      submitter,
      participant,
      LedgerCommandId.assertFromString("assignment-processing-steps-command-id"),
      submissionId = None,
      LedgerApplicationId.assertFromString("tests"),
      workflowId = None,
    )

  private lazy val clock = new WallClock(timeouts, loggerFactory)
  private lazy val crypto =
    SymbolicCrypto.create(testedReleaseProtocolVersion, timeouts, loggerFactory)

  private lazy val seedGenerator = new SeedGenerator(crypto.pureCrypto)

  private lazy val identityFactory = TestingTopology()
    .withDomains(sourceDomain.unwrap)
    .withReversedTopology(
      Map(submittingParticipant -> Map(party1 -> ParticipantPermission.Submission))
    )
    .withSimpleParticipants(participant) // required such that `participant` gets a signing key
    .build(crypto, loggerFactory)

  private lazy val cryptoSnapshot =
    identityFactory
      .forOwnerAndDomain(submittingParticipant, sourceDomain.unwrap)
      .currentSnapshotApproximation

  private lazy val assignmentProcessingSteps =
    testInstance(targetDomain, Set(party1), Set(party1), cryptoSnapshot, None)

  private lazy val indexedStringStore = new InMemoryIndexedStringStore(minIndex = 1, maxIndex = 1)

  private def statefulDependencies
      : Future[(SyncDomainPersistentState, SyncDomainEphemeralState)] = {
    val ledgerApiIndexer = mock[LedgerApiIndexer]
    val persistentState =
      new InMemorySyncDomainPersistentState(
        participant,
        clock,
        crypto,
        IndexedDomain.tryCreate(targetDomain.unwrap, 1),
        defaultStaticDomainParameters,
        enableAdditionalConsistencyChecks = true,
        indexedStringStore = indexedStringStore,
        packageDependencyResolver = mock[PackageDependencyResolver],
        ledgerApiStore = Eval.now(mock[LedgerApiStore]),
        loggerFactory = loggerFactory,
        exitOnFatalFailures = true,
        timeouts = timeouts,
        futureSupervisor = futureSupervisor,
      )
    for {
      _ <- persistentState.parameterStore.setParameters(defaultStaticDomainParameters)
    } yield {
      val state = new SyncDomainEphemeralState(
        participant,
        mock[ParticipantNodeEphemeralState],
        persistentState,
        ledgerApiIndexer,
        ProcessingStartingPoints.default,
        () => mock[DomainTimeTracker],
        ParticipantTestMetrics.domain,
        exitOnFatalFailures = true,
        CachingConfigs.defaultSessionKeyCacheConfig,
        DefaultProcessingTimeouts.testing,
        loggerFactory = loggerFactory,
        FutureSupervisor.Noop,
        clock,
      )
      (persistentState, state)
    }
  }

  def mkParsedRequest(
      view: FullAssignmentTree,
      recipients: Recipients = RecipientsTest.testInstance,
      signatureO: Option[Signature] = None,
  ): ParsedReassignmentRequest[FullAssignmentTree] = ParsedReassignmentRequest(
    RequestCounter(1),
    CantonTimestamp.Epoch,
    SequencerCounter(1),
    view,
    recipients,
    signatureO,
    None,
    isFreshOwnTimelyRequest = true,
    isReassigningParticipant = false,
    Seq.empty,
    targetMediator,
    cryptoSnapshot,
    cryptoSnapshot.ipsSnapshot.findDynamicDomainParameters().futureValue.value,
  )

  "prepare submission" should {
    def setUpOrFail(
        reassignmentData: ReassignmentData,
        unassignmentResult: DeliveredUnassignmentResult,
        persistentState: SyncDomainPersistentState,
    ): FutureUnlessShutdown[Unit] =
      for {
        _ <- valueOrFail(persistentState.reassignmentStore.addReassignment(reassignmentData))(
          "add reassignment data failed"
        )
        _ <- valueOrFail(
          persistentState.reassignmentStore.addUnassignmentResult(unassignmentResult)
        )(
          "add unassignment result failed"
        )
      } yield ()

    val reassignmentId = ReassignmentId(sourceDomain, CantonTimestamp.Epoch)
    val reassignmentDataF =
      ReassignmentStoreTest.mkReassignmentDataForDomain(
        reassignmentId,
        sourceMediator,
        party1,
        targetDomain,
      )
    val submissionParam = SubmissionParam(
      submitterInfo(party1),
      reassignmentId,
    )
    val unassignmentResult =
      ReassignmentResultHelpers.unassignmentResult(
        sourceDomain,
        cryptoSnapshot,
        participant,
      )

    "succeed without errors" in {
      for {
        reassignmentData <- reassignmentDataF
        deps <- statefulDependencies
        (persistentState, state) = deps
        _ <- setUpOrFail(reassignmentData, unassignmentResult, persistentState).failOnShutdown
        _preparedSubmission <-
          assignmentProcessingSteps
            .createSubmission(
              submissionParam,
              targetMediator,
              state,
              cryptoSnapshot,
            )
            .valueOrFailShutdown("assignment submission")
      } yield succeed
    }

    "fail when a receiving party has no participant on the domain" in {
      val unassignmentRequest = UnassignmentRequest(
        submitterInfo(party1),
        Set(party1, party2), // Party 2 is a stakeholder and therefore a receiving party
        Set.empty,
        ReassignmentStoreTest.transactionId1,
        ReassignmentStoreTest.contract,
        reassignmentId.sourceDomain,
        SourceProtocolVersion(testedProtocolVersion),
        sourceMediator,
        targetDomain,
        TargetProtocolVersion(testedProtocolVersion),
        TimeProofTestUtil.mkTimeProof(
          timestamp = CantonTimestamp.Epoch,
          targetDomain = targetDomain,
        ),
        initialReassignmentCounter,
      )
      val uuid = new UUID(1L, 2L)
      val seed = seedGenerator.generateSaltSeed()
      val reassignmentData2 = {
        val fullUnassignmentTree = unassignmentRequest
          .toFullUnassignmentTree(
            crypto.pureCrypto,
            crypto.pureCrypto,
            seed,
            uuid,
          )
        ReassignmentData(
          SourceProtocolVersion(testedProtocolVersion),
          reassignmentId.unassignmentTs,
          RequestCounter(0),
          fullUnassignmentTree,
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
        _ <- setUpOrFail(reassignmentData2, unassignmentResult, persistentState).failOnShutdown
        preparedSubmission <- leftOrFailShutdown(
          assignmentProcessingSteps.createSubmission(
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

    "fail when unassignment processing is not yet complete" in {
      for {
        reassignmentData <- reassignmentDataF
        deps <- statefulDependencies
        (persistentState, state) = deps
        _ <- valueOrFail(persistentState.reassignmentStore.addReassignment(reassignmentData))(
          "add reassignment data failed"
        ).failOnShutdown
        preparedSubmission <- leftOrFailShutdown(
          assignmentProcessingSteps.createSubmission(
            submissionParam,
            targetMediator,
            state,
            cryptoSnapshot,
          )
        )("prepare submission did not return a left")
      } yield {
        preparedSubmission should matchPattern { case UnassignmentIncomplete(_, _) =>
        }
      }
    }

    "fail when submitting party is not a stakeholder" in {
      val submissionParam2 = SubmissionParam(
        submitterInfo(party2),
        reassignmentId,
      )

      for {
        reassignmentData <- reassignmentDataF
        deps <- statefulDependencies
        (persistentState, state) = deps
        _ <- setUpOrFail(reassignmentData, unassignmentResult, persistentState).failOnShutdown
        preparedSubmission <- leftOrFailShutdown(
          assignmentProcessingSteps.createSubmission(
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
        reassignmentData <- reassignmentDataF
        deps <- statefulDependencies
        (persistentState, state) = deps
        _ <- setUpOrFail(reassignmentData, unassignmentResult, persistentState).failOnShutdown
        preparedSubmission <- leftOrFailShutdown(
          assignmentProcessingSteps.createSubmission(
            submissionParam,
            targetMediator,
            state,
            cryptoSnapshot2,
          )
        )("prepare submission did not return a left")
      } yield {
        preparedSubmission should matchPattern { case NoReassignmentSubmissionPermission(_, _, _) =>
        }
      }
    }

    "fail when submitting party not hosted on the participant" in {
      val submissionParam2 = SubmissionParam(
        submitterInfo(party2),
        reassignmentId,
      )
      for {
        reassignmentData2 <- ReassignmentStoreTest.mkReassignmentDataForDomain(
          reassignmentId,
          sourceMediator,
          party2,
          targetDomain,
        )
        deps <- statefulDependencies
        (persistentState, ephemeralState) = deps
        _ <- setUpOrFail(reassignmentData2, unassignmentResult, persistentState).failOnShutdown
        preparedSubmission <- leftOrFailShutdown(
          assignmentProcessingSteps.createSubmission(
            submissionParam2,
            targetMediator,
            ephemeralState,
            cryptoSnapshot,
          )
        )("prepare submission did not return a left")
      } yield {
        preparedSubmission should matchPattern { case NoReassignmentSubmissionPermission(_, _, _) =>
        }
      }
    }
  }

  "receive request" should {
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
    val inTree =
      makeFullAssignmentTree(
        party1,
        Set(party1),
        contract,
        transactionId1,
        targetDomain,
        targetMediator,
        unassignmentResult,
      )

    "succeed without errors" in {
      val sessionKeyStore = SessionKeyStore(CachingConfigs.defaultSessionKeyCacheConfig)
      for {
        inRequest <- encryptFullAssignmentTree(inTree, sessionKeyStore)
        envelopes = NonEmpty(
          Seq,
          OpenEnvelope(inRequest, RecipientsTest.testInstance)(testedProtocolVersion),
        )
        decrypted <-
          assignmentProcessingSteps
            .decryptViews(envelopes, cryptoSnapshot, sessionKeyStore)
            .valueOrFailShutdown(
              "decrypt request failed"
            )
        (WithRecipients(view, recipients), signature) = decrypted.views.loneElement
        activenessSet =
          assignmentProcessingSteps
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
        activenessSet shouldBe mkActivenessSet(assign = Set(contractId))
      }
    }

    "fail when target domain is not current domain" in {
      val inTree2 = makeFullAssignmentTree(
        party1,
        Set(party1),
        contract,
        transactionId1,
        TargetDomainId(anotherDomain),
        anotherMediator,
        unassignmentResult,
      )
      val error =
        assignmentProcessingSteps.computeActivenessSet(mkParsedRequest(inTree2)).left.value

      inside(error) { case UnexpectedDomain(_, targetD, currentD) =>
        assert(targetD == anotherDomain)
        assert(currentD == targetDomain.unwrap)
      }
    }

    "deduplicate requests with an alarm" in {
      // Send the same assignment request twice
      val parsedRequest = mkParsedRequest(inTree)
      val viewWithMetadata = (
        WithRecipients(parsedRequest.fullViewTree, parsedRequest.recipients),
        parsedRequest.signatureO,
      )
      for {
        result <-
          loggerFactory.assertLogs(
            assignmentProcessingSteps.computeParsedRequest(
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
    val unassignmentResult =
      ReassignmentResultHelpers.unassignmentResult(
        sourceDomain,
        cryptoSnapshot,
        submittingParticipant,
      )

    "fail when wrong stakeholders given" in {
      for {
        deps <- statefulDependencies
        (_persistentState, ephemeralState) = deps

        // party2 is incorrectly registered as a stakeholder
        fullAssignmentTree2 = makeFullAssignmentTree(
          party1,
          stakeholders = Set(party1, party2),
          contract,
          transactionId1,
          targetDomain,
          targetMediator,
          unassignmentResult,
        )

        reassignmentLookup = ephemeralState.reassignmentCache

        result <- leftOrFail(
          assignmentProcessingSteps
            .constructPendingDataAndResponse(
              mkParsedRequest(fullAssignmentTree2),
              reassignmentLookup,
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

        reassignmentLookup = ephemeralState.reassignmentCache
        contractLookup = ephemeralState.contractLookup

        fullAssignmentTree = makeFullAssignmentTree(
          party1,
          Set(party1),
          contract,
          transactionId1,
          targetDomain,
          targetMediator,
          unassignmentResult,
        )

        _result <- valueOrFail(
          assignmentProcessingSteps
            .constructPendingDataAndResponse(
              mkParsedRequest(fullAssignmentTree),
              reassignmentLookup,
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

      val inRes = ReassignmentResultHelpers.assignmentResult(targetDomain)

      val contractId = ExampleTransactionFactory.suffixedId(10, 0)
      val contract =
        ExampleTransactionFactory.asSerializable(
          contractId,
          contractInstance = ExampleTransactionFactory.contractInstance(),
          metadata = ContractMetadata.tryCreate(Set(party1), Set(party1), None),
        )
      val reassignmentId = ReassignmentId(sourceDomain, CantonTimestamp.Epoch)
      val rootHash = mock[RootHash]
      when(rootHash.asLedgerTransactionId).thenReturn(LedgerTransactionId.fromString("id1"))
      val pendingRequestData = AssignmentProcessingSteps.PendingAssignment(
        RequestId(CantonTimestamp.Epoch),
        RequestCounter(1),
        SequencerCounter(1),
        rootHash,
        contract,
        initialReassignmentCounter,
        submitterInfo(submitter),
        transactionId1,
        isReassigningParticipant = false,
        reassignmentId,
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
          assignmentProcessingSteps
            .getCommitSetAndContractsToBeStoredAndEvent(
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
              state.pendingAssignmentSubmissions,
              crypto.pureCrypto,
            )
            .failOnShutdown
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
  ): AssignmentProcessingSteps = {

    val pureCrypto = new SymbolicPureCrypto
    val damle = DAMLeTestInstance(participant, signatories, stakeholders)(loggerFactory)
    val seedGenerator = new SeedGenerator(pureCrypto)

    new AssignmentProcessingSteps(
      targetDomain,
      submittingParticipant,
      damle,
      TestReassignmentCoordination.apply(
        Set(),
        CantonTimestamp.Epoch,
        Some(snapshotOverride),
        Some(awaitTimestampOverride),
        loggerFactory,
      ),
      seedGenerator,
      defaultStaticDomainParameters,
      TargetProtocolVersion(testedProtocolVersion),
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
  ): FullAssignmentTree = {
    val seed = seedGenerator.generateSaltSeed()

    valueOrFail(
      AssignmentProcessingSteps.makeFullAssignmentTree(
        crypto.pureCrypto,
        seed,
        submitterInfo(submitter),
        stakeholders,
        contract,
        initialReassignmentCounter,
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

  private def encryptFullAssignmentTree(
      tree: FullAssignmentTree,
      sessionKeyStore: SessionKeyStore,
  ): Future[EncryptedViewMessage[AssignmentViewType]] =
    EncryptedViewMessageFactory
      .create(AssignmentViewType)(tree, cryptoSnapshot, sessionKeyStore, testedProtocolVersion)
      .valueOrFailShutdown("cannot encrypt assignment request")
}
