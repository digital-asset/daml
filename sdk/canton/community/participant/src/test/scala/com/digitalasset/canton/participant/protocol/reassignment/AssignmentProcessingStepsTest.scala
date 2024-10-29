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
  AssignmentSubmitterMustBeStakeholder,
  NoReassignmentSubmissionPermission,
  ParsedReassignmentRequest,
  StakeholdersMismatch,
}
import com.digitalasset.canton.participant.protocol.submission.EncryptedViewMessageFactory.{
  ViewHashAndRecipients,
  ViewKeyData,
}
import com.digitalasset.canton.participant.protocol.submission.{
  EncryptedViewMessageFactory,
  SeedGenerator,
}
import com.digitalasset.canton.participant.protocol.{
  EngineController,
  ProcessingStartingPoints,
  SerializableContractAuthenticator,
}
import com.digitalasset.canton.participant.store.ReassignmentStoreTest.coidAbs1
import com.digitalasset.canton.participant.store.memory.*
import com.digitalasset.canton.participant.store.{
  ParticipantNodeEphemeralState,
  ReassignmentStoreTest,
  SyncDomainEphemeralState,
  SyncDomainPersistentState,
}
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceAlarm
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.ExampleTransactionFactory.submitter
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.store.memory.InMemoryIndexedStringStore
import com.digitalasset.canton.store.{
  ConfirmationRequestSessionKeyStore,
  IndexedDomain,
  SessionKeyStoreWithInMemoryCache,
}
import com.digitalasset.canton.time.{DomainTimeTracker, WallClock}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.transaction.ParticipantPermission.Confirmation
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.version.HasTestCloseContext
import monocle.macros.syntax.lens.*
import org.scalatest.wordspec.AsyncWordSpec

import java.util.UUID
import scala.concurrent.Future

class AssignmentProcessingStepsTest
    extends AsyncWordSpec
    with BaseTest
    with HasTestCloseContext
    with HasExecutionContext {
  private lazy val sourceDomain = Source(
    DomainId(UniqueIdentifier.tryFromProtoPrimitive("domain::source"))
  )
  private lazy val sourceMediator = MediatorGroupRecipient(MediatorGroupIndex.tryCreate(0))
  private lazy val targetDomain = Target(
    DomainId(UniqueIdentifier.tryFromProtoPrimitive("domain::target"))
  )
  private lazy val targetMediator = MediatorGroupRecipient(MediatorGroupIndex.tryCreate(0))
  private lazy val anotherDomain = DomainId(
    UniqueIdentifier.tryFromProtoPrimitive("domain::another")
  )
  private lazy val anotherMediator = MediatorGroupRecipient(MediatorGroupIndex.tryCreate(1))
  private lazy val party1: LfPartyId = PartyId(
    UniqueIdentifier.tryFromProtoPrimitive("party1::party")
  ).toLf
  private lazy val party2: LfPartyId = PartyId(
    UniqueIdentifier.tryFromProtoPrimitive("party2::party")
  ).toLf
  private lazy val party3: LfPartyId = PartyId(
    UniqueIdentifier.tryFromProtoPrimitive("party3::party")
  ).toLf

  private lazy val participant = ParticipantId(
    UniqueIdentifier.tryFromProtoPrimitive("bothdomains::participant")
  )

  private lazy val contract = ExampleTransactionFactory.authenticatedSerializableContract(
    metadata = ContractMetadata.tryCreate(
      stakeholders = Set(party1),
      signatories = Set(party1),
      maybeKeyWithMaintainersVersioned = None,
    )
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
      Map(
        participant -> Map(
          party1 -> ParticipantPermission.Submission,
          party2 -> Confirmation,
        )
      )
    )
    .withSimpleParticipants(participant) // required such that `participant` gets a signing key
    .build(crypto, loggerFactory)

  private lazy val cryptoSnapshot =
    identityFactory
      .forOwnerAndDomain(participant, sourceDomain.unwrap)
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

  private lazy val reassignmentId = ReassignmentId(sourceDomain, CantonTimestamp.Epoch)

  private lazy val reassignmentDataHelpers = new ReassignmentDataHelpers(
    contract,
    reassignmentId.sourceDomain,
    targetDomain,
    identityFactory,
  )

  private lazy val unassignmentRequest = reassignmentDataHelpers.unassignmentRequest(
    party1,
    DefaultTestIdentities.participant1,
    sourceMediator,
  )()

  private lazy val reassignmentData =
    reassignmentDataHelpers.reassignmentData(reassignmentId, unassignmentRequest)(None)

  private lazy val unassignmentResult =
    reassignmentDataHelpers.unassignmentResult(reassignmentData).futureValue

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

    val submissionParam = SubmissionParam(
      submitterInfo(party1),
      reassignmentId,
    )

    "succeed without errors" in {
      for {
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

      val helpers = reassignmentDataHelpers
        .focus(_.contract.metadata)
        .modify(metadata =>
          ContractMetadata.tryCreate(
            signatories = metadata.signatories,
            // party3 is a stakeholder and therefore a receiving party
            stakeholders = metadata.stakeholders + party3,
            maybeKeyWithMaintainersVersioned = None,
          )
        )

      val unassignmentRequest = helpers.unassignmentRequest(
        party1,
        DefaultTestIdentities.participant1,
        sourceMediator,
      )()

      val reassignmentData2 =
        reassignmentDataHelpers.reassignmentData(reassignmentId, unassignmentRequest)()

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
          p shouldBe party3
        }
      }
    }

    "fail when unassignment processing is not yet complete" in {
      for {
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
        preparedSubmission should matchPattern {
          case AssignmentSubmitterMustBeStakeholder(_, _, _) =>
        }
      }
    }

    "fail when participant does not have submission permission for party" in {

      val failingTopology = TestingTopology(domains = Set(sourceDomain.unwrap))
        .withReversedTopology(
          Map(participant -> Map(party1 -> ParticipantPermission.Observation))
        )
        .build(loggerFactory)
      val cryptoSnapshot2 = failingTopology
        .forOwnerAndDomain(participant, sourceDomain.unwrap)
        .currentSnapshotApproximation

      for {
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

      // We need to change the contract instance otherwise we get another error (AssignmentSubmitterMustBeStakeholder)
      val contract = ExampleTransactionFactory.asSerializable(
        contractId = coidAbs1,
        contractInstance = ExampleTransactionFactory.contractInstance(),
        ledgerTime = CantonTimestamp.Epoch,
        metadata = ContractMetadata.tryCreate(Set(), Set(party2), None),
      )

      val reassignmentData2 = ReassignmentStoreTest.mkReassignmentDataForDomain(
        reassignmentId,
        sourceMediator,
        party2,
        targetDomain,
        contract,
      )

      for {
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
    val assignmentTree =
      makeFullAssignmentTree(
        party1,
        contract,
        ExampleTransactionFactory.transactionId(0),
        targetDomain,
        targetMediator,
        unassignmentResult,
      )

    "succeed without errors" in {
      val sessionKeyStore =
        new SessionKeyStoreWithInMemoryCache(CachingConfigs.defaultSessionKeyCacheConfig)
      for {
        assignmentRequest <- encryptFullAssignmentTree(
          assignmentTree,
          RecipientsTest.testInstance,
          sessionKeyStore,
        )
        envelopes = NonEmpty(
          Seq,
          OpenEnvelope(assignmentRequest, RecipientsTest.testInstance)(testedProtocolVersion),
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
        activenessSet shouldBe mkActivenessSet(assign = Set(contract.contractId))
      }
    }

    "fail when target domain is not current domain" in {
      val inTree2 = makeFullAssignmentTree(
        party1,
        contract,
        ExampleTransactionFactory.transactionId(0),
        Target(anotherDomain),
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
      val parsedRequest = mkParsedRequest(assignmentTree)
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
    "fail when wrong stakeholders given" in {
      lazy val contractWrongStakeholders =
        ExampleTransactionFactory.authenticatedSerializableContract(
          metadata = ContractMetadata.tryCreate(
            stakeholders = Set(party1, party2),
            signatories = Set(party1),
            maybeKeyWithMaintainersVersioned = None,
          )
        )

      for {
        deps <- statefulDependencies
        (_persistentState, ephemeralState) = deps

        // party2 is incorrectly registered as a stakeholder
        fullAssignmentTree2 = makeFullAssignmentTree(
          party1,
          contractWrongStakeholders,
          ExampleTransactionFactory.transactionId(0),
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

        fullAssignmentTree = makeFullAssignmentTree(
          party1,
          contract,
          ExampleTransactionFactory.transactionId(0),
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

      val assignmentResult = reassignmentDataHelpers.assignmentResult()

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
        ExampleTransactionFactory.transactionId(0),
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
              assignmentResult.verdict,
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
      targetDomain: Target[DomainId],
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
      participant,
      damle,
      TestReassignmentCoordination.apply(
        Set(),
        CantonTimestamp.Epoch,
        Some(snapshotOverride),
        Some(awaitTimestampOverride),
        loggerFactory,
      ),
      seedGenerator,
      SerializableContractAuthenticator(pureCrypto),
      Target(defaultStaticDomainParameters),
      Target(testedProtocolVersion),
      loggerFactory = loggerFactory,
    )
  }

  private def makeFullAssignmentTree(
      submitter: LfPartyId,
      contract: SerializableContract,
      creatingTransactionId: TransactionId,
      targetDomain: Target[DomainId],
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
        contract,
        initialReassignmentCounter,
        creatingTransactionId,
        targetDomain,
        targetMediator,
        unassignmentResult,
        uuid,
        Source(testedProtocolVersion),
        Target(testedProtocolVersion),
        confirmingReassigningParticipants = Set.empty,
      )
    )("Failed to create FullAssignmentTree")
  }

  private def encryptFullAssignmentTree(
      tree: FullAssignmentTree,
      recipients: Recipients,
      sessionKeyStore: ConfirmationRequestSessionKeyStore,
  ): Future[EncryptedViewMessage[AssignmentViewType]] =
    for {
      viewsToKeyMap <- EncryptedViewMessageFactory
        .generateKeysFromRecipients(
          Seq((ViewHashAndRecipients(tree.viewHash, recipients), None, tree.informees.toList)),
          parallel = true,
          crypto.pureCrypto,
          cryptoSnapshot,
          sessionKeyStore,
          testedProtocolVersion,
        )
        .valueOrFailShutdown("cannot generate encryption key for transfer-in request")
      ViewKeyData(_, viewKey, viewKeyMap) = viewsToKeyMap(tree.viewHash)
      encryptedTree <- EncryptedViewMessageFactory
        .create(AssignmentViewType)(
          tree,
          (viewKey, viewKeyMap),
          cryptoSnapshot,
          testedProtocolVersion,
        )
        .valueOrFailShutdown("cannot encrypt assignment request")
    } yield encryptedTree
}
