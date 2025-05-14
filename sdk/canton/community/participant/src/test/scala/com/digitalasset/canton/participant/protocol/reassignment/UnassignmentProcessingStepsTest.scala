// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.Eval
import cats.data.EitherT
import cats.implicits.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{CachingConfigs, DefaultProcessingTimeouts}
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.crypto.{
  Signature,
  SigningKeyUsage,
  SyncCryptoError,
  SynchronizerCrypto,
  SynchronizerSnapshotSyncCryptoApi,
  TestHash,
}
import com.digitalasset.canton.data.*
import com.digitalasset.canton.data.ViewType.UnassignmentViewType
import com.digitalasset.canton.lifecycle.{DefaultPromiseUnlessShutdownFactory, FutureUnlessShutdown}
import com.digitalasset.canton.participant.admin.PackageDependencyResolver
import com.digitalasset.canton.participant.event.RecordOrderPublisher
import com.digitalasset.canton.participant.ledger.api.{LedgerApiIndexer, LedgerApiStore}
import com.digitalasset.canton.participant.metrics.ParticipantTestMetrics
import com.digitalasset.canton.participant.protocol.EngineController.EngineAbortStatus
import com.digitalasset.canton.participant.protocol.conflictdetection.ConflictDetectionHelpers.{
  mkActivenessResult,
  mkActivenessSet,
}
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.*
import com.digitalasset.canton.participant.protocol.reassignment.UnassignmentProcessingSteps.PendingUnassignment
import com.digitalasset.canton.participant.protocol.reassignment.UnassignmentProcessorError.*
import com.digitalasset.canton.participant.protocol.reassignment.UnassignmentValidationError.PackageIdUnknownOrUnvetted
import com.digitalasset.canton.participant.protocol.submission.EncryptedViewMessageFactory.{
  ViewHashAndRecipients,
  ViewKeyData,
}
import com.digitalasset.canton.participant.protocol.submission.TransactionTreeFactory.PackageUnknownTo
import com.digitalasset.canton.participant.protocol.submission.{
  EncryptedViewMessageFactory,
  InFlightSubmissionSynchronizerTracker,
  SeedGenerator,
}
import com.digitalasset.canton.participant.protocol.validation.{
  AuthenticationError,
  AuthenticationValidator,
}
import com.digitalasset.canton.participant.protocol.{
  ContractAuthenticator,
  EngineController,
  ProcessingStartingPoints,
}
import com.digitalasset.canton.participant.store.AcsCounterParticipantConfigStore
import com.digitalasset.canton.participant.store.memory.*
import com.digitalasset.canton.participant.sync.SyncEphemeralState
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.sequencing.traffic.TrafficReceipt
import com.digitalasset.canton.store.memory.InMemoryIndexedStringStore
import com.digitalasset.canton.store.{
  ConfirmationRequestSessionKeyStore,
  IndexedSynchronizer,
  SessionKeyStoreWithInMemoryCache,
}
import com.digitalasset.canton.time.{SynchronizerTimeTracker, TimeProofTestUtil, WallClock}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.transaction.ParticipantPermission.{
  Confirmation,
  Observation,
  Submission,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.version.HasTestCloseContext
import com.digitalasset.canton.{
  BaseTest,
  FailOnShutdown,
  HasExecutorService,
  LedgerCommandId,
  LedgerUserId,
  LfPackageId,
  LfPartyId,
  ReassignmentCounter,
  RequestCounter,
  SequencerCounter,
}
import org.scalatest.wordspec.AsyncWordSpec

import java.util.UUID
import scala.annotation.nowarn
import scala.concurrent.{ExecutionContext, Future}

@nowarn("msg=match may not be exhaustive")
final class UnassignmentProcessingStepsTest
    extends AsyncWordSpec
    with BaseTest
    with HasExecutorService
    with HasTestCloseContext
    with FailOnShutdown {

  private implicit val ec: ExecutionContext = executorService

  private val testTopologyTimestamp = CantonTimestamp.Epoch

  private lazy val sourceSynchronizer = Source(
    SynchronizerId(UniqueIdentifier.tryFromProtoPrimitive("source::synchronizer")).toPhysical
  )
  private lazy val sourceMediator = MediatorGroupRecipient(MediatorGroupIndex.tryCreate(100))
  private lazy val targetSynchronizer = Target(
    SynchronizerId(UniqueIdentifier.tryFromProtoPrimitive("target::synchronizer")).toPhysical
  )

  private lazy val submitter: LfPartyId = PartyId(
    UniqueIdentifier.tryFromProtoPrimitive("submitter::party")
  ).toLf
  private lazy val party1: LfPartyId = PartyId(
    UniqueIdentifier.tryFromProtoPrimitive("party1::party")
  ).toLf
  private lazy val party2: LfPartyId = PartyId(
    UniqueIdentifier.tryFromProtoPrimitive("party2::party")
  ).toLf

  private lazy val submittingParticipant = ParticipantId(
    UniqueIdentifier.tryFromProtoPrimitive("submitting::participant")
  )

  private lazy val initialReassignmentCounter: ReassignmentCounter = ReassignmentCounter.Genesis

  private def submitterMetadata(submitter: LfPartyId): ReassignmentSubmitterMetadata =
    ReassignmentSubmitterMetadata(
      submitter,
      submittingParticipant,
      LedgerCommandId.assertFromString("unassignment-processing-steps-command-id"),
      submissionId = None,
      LedgerUserId.assertFromString("tests"),
      workflowId = None,
    )

  private lazy val adminSubmitter: LfPartyId = submittingParticipant.adminParty.toLf

  private lazy val crypto =
    SymbolicCrypto.create(testedReleaseProtocolVersion, timeouts, loggerFactory)

  private lazy val ledgerApiIndexer = mock[LedgerApiIndexer]
  private lazy val contractStore = new InMemoryContractStore(timeouts, loggerFactory)

  private lazy val clock = new WallClock(timeouts, loggerFactory)
  private lazy val indexedStringStore = new InMemoryIndexedStringStore(minIndex = 1, maxIndex = 1)
  private lazy val persistentState =
    new InMemorySyncPersistentState(
      submittingParticipant,
      clock,
      SynchronizerCrypto(crypto, defaultStaticSynchronizerParameters),
      IndexedSynchronizer.tryCreate(sourceSynchronizer.unwrap, 1),
      defaultStaticSynchronizerParameters,
      enableAdditionalConsistencyChecks = true,
      indexedStringStore = indexedStringStore,
      contractStore = contractStore,
      acsCounterParticipantConfigStore = mock[AcsCounterParticipantConfigStore],
      exitOnFatalFailures = true,
      packageDependencyResolver = mock[PackageDependencyResolver],
      Eval.now(mock[LedgerApiStore]),
      loggerFactory,
      timeouts,
      futureSupervisor,
    )

  private def mkState: SyncEphemeralState =
    new SyncEphemeralState(
      submittingParticipant,
      mock[RecordOrderPublisher],
      mock[SynchronizerTimeTracker],
      mock[InFlightSubmissionSynchronizerTracker],
      persistentState,
      ledgerApiIndexer,
      contractStore,
      new DefaultPromiseUnlessShutdownFactory(timeouts, loggerFactory),
      ProcessingStartingPoints.default,
      ParticipantTestMetrics.synchronizer,
      exitOnFatalFailures = true,
      CachingConfigs.defaultSessionEncryptionKeyCacheConfig,
      DefaultProcessingTimeouts.testing,
      loggerFactory,
      FutureSupervisor.Noop,
      clock,
    )

  private lazy val unassignmentRequest = UnassignmentRequest(
    submitterMetadata = submitterMetadata(party1),
    reassigningParticipants = Set(submittingParticipant),
    ContractsReassignmentBatch(contract, initialReassignmentCounter),
    sourceSynchronizer,
    Source(testedProtocolVersion),
    sourceMediator,
    targetSynchronizer,
    Target(testedProtocolVersion),
    timeProof,
  )

  private def createTestingIdentityFactory(
      topology: Map[ParticipantId, Map[LfPartyId, ParticipantPermission]],
      packages: Map[ParticipantId, Seq[LfPackageId]],
      synchronizers: Set[SynchronizerId] = Set(DefaultTestIdentities.synchronizerId),
  ) =
    TestingTopology(synchronizers)
      .withReversedTopology(topology)
      .withPackages(packages)
      .build(loggerFactory)

  private def createTestingTopologySnapshot(
      topology: Map[ParticipantId, Map[LfPartyId, ParticipantPermission]],
      packagesOverride: Option[Map[ParticipantId, Seq[LfPackageId]]] = None,
  ): TopologySnapshot = {

    val defaultPackages = topology.keys
      .map(_ -> Seq(ExampleTransactionFactory.packageId))
      .toMap

    val packages = packagesOverride.getOrElse(defaultPackages)
    createTestingIdentityFactory(topology, packages).topologySnapshot()
  }

  private def createCryptoFactory(
      packages: Seq[LfPackageId] = Seq(ExampleTransactionFactory.packageId)
  ) = {
    val topology = Map(
      submittingParticipant -> Map(
        party1 -> ParticipantPermission.Submission,
        submitter -> ParticipantPermission.Submission,
        submittingParticipant.adminParty.toLf -> ParticipantPermission.Submission,
      )
    )
    createTestingIdentityFactory(
      topology = topology,
      packages = topology.keys.map(_ -> packages).toMap,
      synchronizers = Set(sourceSynchronizer.unwrap, targetSynchronizer.unwrap),
    )
  }

  private lazy val cryptoFactory = createCryptoFactory()

  private def createCryptoSnapshot(
      testingIdentityFactory: TestingIdentityFactory = cryptoFactory
  ) =
    testingIdentityFactory
      .forOwnerAndSynchronizer(submittingParticipant, sourceSynchronizer.unwrap)
      .currentSnapshotApproximation

  private lazy val cryptoSnapshot = createCryptoSnapshot()

  private lazy val seedGenerator = new SeedGenerator(crypto.pureCrypto)

  private def createReassignmentCoordination(
      cryptoSnapshot: SynchronizerSnapshotSyncCryptoApi = cryptoSnapshot
  ) =
    TestReassignmentCoordination(
      Set(Target(sourceSynchronizer.unwrap), targetSynchronizer),
      CantonTimestamp.Epoch,
      Some(cryptoSnapshot),
      Some(None),
      loggerFactory,
      Seq(ExampleTransactionFactory.packageId),
    )(directExecutionContext)

  private lazy val coordination: ReassignmentCoordination =
    createReassignmentCoordination()

  private def createUnassignmentProcessingSteps(
      reassignmentCoordination: ReassignmentCoordination = coordination
  ) =
    new UnassignmentProcessingSteps(
      sourceSynchronizer,
      submittingParticipant,
      reassignmentCoordination,
      seedGenerator,
      Source(defaultStaticSynchronizerParameters),
      ContractAuthenticator(crypto.pureCrypto),
      Source(testedProtocolVersion),
      loggerFactory,
    )(executorService)

  private lazy val unassignmentProcessingSteps: UnassignmentProcessingSteps =
    createUnassignmentProcessingSteps()

  private lazy val Seq(
    (participant1, admin1),
    (participant2, _),
    (participant3, _),
    (participant4, _),
  ) =
    (1 to 4).map { i =>
      val participant =
        ParticipantId(UniqueIdentifier.tryFromProtoPrimitive(s"participant$i::participant"))
      val admin = participant.adminParty.toLf
      participant -> admin
    }

  private lazy val timeProof =
    TimeProofTestUtil.mkTimeProof(
      timestamp = CantonTimestamp.Epoch,
      targetSynchronizer = targetSynchronizer,
    )

  private lazy val contract = ExampleTransactionFactory.authenticatedSerializableContract(
    metadata = ContractMetadata.tryCreate(
      signatories = Set(submitter),
      stakeholders = Set(submitter, party1),
      maybeKeyWithMaintainersVersioned = None,
    )
  )
  private lazy val contractId = contract.contractId

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

  "UnassignmentRequest.validated" should {
    val testingTopology = createTestingTopologySnapshot(
      Map(
        submittingParticipant -> Map(submitter -> Submission),
        participant1 -> Map(party1 -> Submission),
        participant2 -> Map(party2 -> Submission),
      )
    )

    def mkUnassignmentResult(
        sourceTopologySnapshot: TopologySnapshot,
        targetTopologySnapshot: TopologySnapshot,
        stakeholdersOverride: Option[Stakeholders] = None,
    ): Either[ReassignmentValidationError, UnassignmentRequestValidated] = {
      val updatedContract = stakeholdersOverride.fold(contract)(stakeholders =>
        contract.copy(metadata = ContractMetadata(stakeholders))
      )

      UnassignmentRequest
        .validated(
          submittingParticipant,
          timeProof,
          ContractsReassignmentBatch(updatedContract, initialReassignmentCounter),
          submitterMetadata(submitter),
          sourceSynchronizer,
          Source(testedProtocolVersion),
          sourceMediator,
          targetSynchronizer,
          Target(testedProtocolVersion),
          Source(sourceTopologySnapshot),
          Target(targetTopologySnapshot),
        )
        .value
        .failOnShutdown
        .futureValue
    }

    "fail if submitter is not a stakeholder" in {
      val stakeholders = Stakeholders.tryCreate(Set(party1, party2), Set(party2))
      mkUnassignmentResult(
        testingTopology,
        testingTopology,
        stakeholdersOverride = Some(stakeholders),
      ).left.value shouldBe ReassignmentValidationError.SubmitterMustBeStakeholder(
        ReassignmentRef(contractId),
        submitter,
        stakeholders.all,
      )
    }

    "fail if submitting party is not hosted on participant" in {
      val ipsNotHostedOnParticipant =
        createTestingTopologySnapshot(Map.empty)

      mkUnassignmentResult(
        ipsNotHostedOnParticipant,
        testingTopology,
      ).left.value shouldBe ReassignmentValidationError.NotHostedOnParticipant(
        ReassignmentRef(contractId),
        submitter,
        submittingParticipant,
      )
    }

    "succeed if a stakeholder cannot submit on target synchronizer" in {
      val ipsNoSubmissionOnTarget = createTestingTopologySnapshot(
        Map(
          submittingParticipant -> Map(submitter -> Submission),
          participant1 -> Map(party1 -> Confirmation),
        )
      )

      val stakeholders = Stakeholders.tryCreate(Set(submitter, party1), Set())
      mkUnassignmentResult(
        testingTopology,
        ipsNoSubmissionOnTarget,
        stakeholdersOverride = Some(stakeholders),
      ).value shouldBe a[UnassignmentRequestValidated]
    }

    "fail if a signatory is not hosted on a confirming reassigning participant" in {
      val ipsConfirmationOnSource = createTestingTopologySnapshot(
        Map(
          submittingParticipant -> Map(submitter -> Submission),
          participant1 -> Map(party1 -> Confirmation),
        )
      )

      val ipsNoConfirmationOnTarget = createTestingTopologySnapshot(
        Map(
          submittingParticipant -> Map(submitter -> Submission),
          participant1 -> Map(party1 -> Observation),
        )
      )

      val stakeholders =
        Stakeholders.withSignatoriesAndObservers(Set(party1), Set(party1, submitter))
      val result = mkUnassignmentResult(
        ipsConfirmationOnSource,
        ipsNoConfirmationOnTarget,
        stakeholdersOverride = Some(stakeholders),
      )

      val expectedError = ReassignmentValidationError.StakeholderHostingErrors(
        s"Signatory $party1 requires at least 1 signatory reassigning participants on synchronizer target, but only 0 are available"
      )

      result.left.value shouldBe expectedError
    }

    // TODO(i13201) This should ideally be covered in integration tests as well
    "fail if the package for the contract being reassigned is unvetted on the target synchronizer" in {
      val sourceSynchronizerTopology =
        createTestingTopologySnapshot(
          Map(
            submittingParticipant -> Map(submitter -> Submission),
            participant1 -> Map(party1 -> Submission),
          ),
          // The package is known on the source synchronizer
          packagesOverride = Some(
            Seq(submittingParticipant, participant1)
              .map(_ -> Seq(ExampleTransactionFactory.packageId))
              .toMap
          ),
        )

      val targetSynchronizerTopology =
        createTestingTopologySnapshot(
          topology = Map(
            submittingParticipant -> Map(submitter -> Submission),
            participant1 -> Map(party1 -> Submission),
          ),
          packagesOverride = Some(Map.empty), // The package is not known on the target synchronizer
        )

      val stakeholders = Stakeholders.tryCreate(Set(submitter, adminSubmitter, admin1), Set())
      val result = mkUnassignmentResult(
        sourceTopologySnapshot = sourceSynchronizerTopology,
        targetTopologySnapshot = targetSynchronizerTopology,
        stakeholdersOverride = Some(stakeholders),
      )

      val expectedError = PackageIdUnknownOrUnvetted(
        Set(contractId),
        unknownTo = List(
          PackageUnknownTo(ExampleTransactionFactory.packageId, submittingParticipant),
          PackageUnknownTo(ExampleTransactionFactory.packageId, participant1),
        ),
      )

      result.left.value shouldBe expectedError
    }

    "fail if the package for the contract being reassigned is unvetted on one non-reassigning participant connected to the target synchronizer" in {

      val sourceSynchronizerTopology =
        createTestingIdentityFactory(
          topology = Map(
            submittingParticipant -> Map(submitter -> Submission),
            participant1 -> Map(party1 -> Submission),
          ),
          // On the source synchronizer, the package is vetted on all participants
          packages = Seq(submittingParticipant, participant1)
            .map(_ -> Seq(ExampleTransactionFactory.packageId))
            .toMap,
        ).topologySnapshot()

      val targetSynchronizerTopology =
        createTestingIdentityFactory(
          topology = Map(
            submittingParticipant -> Map(submitter -> Submission),
            participant1 -> Map(party1 -> Submission),
          ),
          // On the target synchronizer, the package is not vetted on `participant1`
          packages = Map(submittingParticipant -> Seq(ExampleTransactionFactory.packageId)),
        ).topologySnapshot()

      // `party1` is a stakeholder hosted on `participant1`, but it has not vetted `templateId.packageId` on the target synchronizer
      val stakeholders =
        Stakeholders.tryCreate(Set(submitter, party1, adminSubmitter, admin1), Set())

      val result =
        mkUnassignmentResult(
          sourceTopologySnapshot = sourceSynchronizerTopology,
          targetTopologySnapshot = targetSynchronizerTopology,
          stakeholdersOverride = Some(stakeholders),
        )

      val expectedError = PackageIdUnknownOrUnvetted(
        Set(contractId),
        unknownTo = List(
          PackageUnknownTo(ExampleTransactionFactory.packageId, participant1)
        ),
      )

      result.left.value shouldBe expectedError
    }

    "pick the active confirming admin party" in {
      val ipsAdminNoConfirmation = createTestingTopologySnapshot(
        Map(
          submittingParticipant -> Map(adminSubmitter -> Submission, submitter -> Submission),
          participant1 -> Map(party1 -> Confirmation),
          participant2 -> Map(party1 -> Observation), // Not reassigning (cannot confirm)
        )
      )
      val result = mkUnassignmentResult(ipsAdminNoConfirmation, testingTopology)

      result.value shouldEqual
        UnassignmentRequestValidated(
          UnassignmentRequest(
            submitterMetadata = submitterMetadata(submitter),
            reassigningParticipants = Set(submittingParticipant, participant1),
            contracts = ContractsReassignmentBatch(contract, initialReassignmentCounter),
            sourceSynchronizer = sourceSynchronizer,
            sourceProtocolVersion = Source(testedProtocolVersion),
            sourceMediator = sourceMediator,
            targetSynchronizer = targetSynchronizer,
            targetProtocolVersion = Target(testedProtocolVersion),
            targetTimeProof = timeProof,
          ),
          Set(submittingParticipant, participant1, participant2),
        )
    }

    "work if topology constraints are satisfied" in {
      val ipsSource = createTestingTopologySnapshot(
        Map(
          submittingParticipant -> Map(adminSubmitter -> Submission, submitter -> Submission),
          participant1 -> Map(submitter -> Confirmation),
          participant2 -> Map(party1 -> Submission),
          participant3 -> Map(party1 -> Submission),
          participant4 -> Map(party1 -> Confirmation),
        )
      )
      val ipsTarget = createTestingTopologySnapshot(
        Map(
          submittingParticipant -> Map(submitter -> Submission),
          participant1 -> Map(submitter -> Observation),
          participant3 -> Map(party1 -> Submission),
          participant4 -> Map(party1 -> Observation),
        )
      )

      val result = mkUnassignmentResult(ipsSource, ipsTarget)

      result.value shouldEqual
        UnassignmentRequestValidated(
          UnassignmentRequest(
            submitterMetadata = submitterMetadata(submitter),
            reassigningParticipants =
              Set(submittingParticipant, participant1, participant3, participant4),
            contracts = ContractsReassignmentBatch(contract, initialReassignmentCounter),
            sourceSynchronizer = sourceSynchronizer,
            sourceProtocolVersion = Source(testedProtocolVersion),
            sourceMediator = sourceMediator,
            targetSynchronizer = targetSynchronizer,
            targetProtocolVersion = Target(testedProtocolVersion),
            targetTimeProof = timeProof,
          ),
          Set(submittingParticipant, participant1, participant2, participant3, participant4),
        )
    }

    "allow admin parties as stakeholders" in {
      val stakeholders = Set(submitter, adminSubmitter, admin1)
      val updatedContract = contract.copy(metadata =
        ContractMetadata.tryCreate(
          signatories = Set(),
          stakeholders = stakeholders,
          None,
        )
      )

      val unassignmentResult = mkUnassignmentResult(
        testingTopology,
        testingTopology,
        stakeholdersOverride = Some(Stakeholders(updatedContract.metadata)),
      ).value

      val expectedUnassignmentResult = UnassignmentRequestValidated(
        UnassignmentRequest(
          submitterMetadata = submitterMetadata(submitter),
          // Because admin1 is a stakeholder, participant1 is reassigning
          reassigningParticipants = Set(submittingParticipant, participant1),
          contracts = ContractsReassignmentBatch(updatedContract, initialReassignmentCounter),
          sourceSynchronizer = sourceSynchronizer,
          sourceProtocolVersion = Source(testedProtocolVersion),
          sourceMediator = sourceMediator,
          targetSynchronizer = targetSynchronizer,
          targetProtocolVersion = Target(testedProtocolVersion),
          targetTimeProof = timeProof,
        ),
        Set(submittingParticipant, participant1),
      )

      unassignmentResult shouldBe expectedUnassignmentResult
    }
  }

  "prepare submission" should {
    "succeed without errors" in {
      val state = mkState
      val submissionParam =
        UnassignmentProcessingSteps.SubmissionParam(
          submitterMetadata = submitterMetadata(party1),
          Seq(contractId),
          targetSynchronizer,
          Target(testedProtocolVersion),
        )

      for {
        _ <- state.contractStore.storeContract(contract)
        _ <- persistentState.activeContractStore
          .markContractsCreated(
            Seq(contractId -> initialReassignmentCounter),
            TimeOfChange(timeProof.timestamp),
          )
          .value
        _ <-
          unassignmentProcessingSteps
            .createSubmission(
              submissionParam,
              sourceMediator,
              state,
              cryptoSnapshot,
            )
            .valueOrFail("prepare submission failed")
      } yield succeed
    }

    "check that the target synchronizer is not equal to the source synchronizer" in {
      val state = mkState
      val contract = ExampleTransactionFactory.asSerializable(
        contractId,
        contractInstance = ExampleTransactionFactory.contractInstance(),
      )
      val submissionParam = UnassignmentProcessingSteps.SubmissionParam(
        submitterMetadata = submitterMetadata(party1),
        Seq(contractId),
        Target(sourceSynchronizer.unwrap),
        Target(testedProtocolVersion),
      )

      for {
        _ <- state.contractStore.storeContract(contract).failOnShutdown
        submissionResult <- leftOrFailShutdown(
          unassignmentProcessingSteps.createSubmission(
            submissionParam,
            sourceMediator,
            state,
            cryptoSnapshot,
          )
        )("prepare submission succeeded unexpectedly")
      } yield {
        submissionResult shouldBe a[TargetSynchronizerIsSourceSynchronizer]
      }
    }
  }

  "receive request" should {
    val unassignmentTree = makeFullUnassignmentTree(unassignmentRequest)
    "succeed without errors" in {
      val sessionKeyStore =
        new SessionKeyStoreWithInMemoryCache(CachingConfigs.defaultSessionEncryptionKeyCacheConfig)
      for {
        encryptedOutRequest <- encryptUnassignmentTree(
          unassignmentTree,
          RecipientsTest.testInstance,
          sessionKeyStore,
        )
        envelopes =
          NonEmpty(
            Seq,
            OpenEnvelope(encryptedOutRequest, RecipientsTest.testInstance)(testedProtocolVersion),
          )
        decrypted <-
          unassignmentProcessingSteps
            .decryptViews(envelopes, cryptoSnapshot, sessionKeyStore)
            .valueOrFailShutdown(
              "decrypt request failed"
            )
        activenessSet =
          unassignmentProcessingSteps
            .computeActivenessSet(
              mkParsedRequest(unassignmentTree, RecipientsTest.testInstance, None)
            )
            .value
      } yield {
        decrypted.decryptionErrors shouldBe Seq.empty
        activenessSet shouldBe mkActivenessSet(deact = Set(contractId), prior = Set(contractId))
      }
    }
  }

  "construct pending data and response" should {

    def constructPendingDataAndResponseWith(
        unassignmentProcessingSteps: UnassignmentProcessingSteps
    ) = {
      val state = mkState
      val unassignmentRequest = UnassignmentRequest(
        submitterMetadata = submitterMetadata(party1),
        reassigningParticipants = Set(submittingParticipant),
        ContractsReassignmentBatch(contract, initialReassignmentCounter),
        sourceSynchronizer,
        Source(testedProtocolVersion),
        sourceMediator,
        targetSynchronizer,
        Target(testedProtocolVersion),
        timeProof,
      )
      val fullUnassignmentTree = makeFullUnassignmentTree(unassignmentRequest)

      state.contractStore
        .storeContract(contract)
        .failOnShutdown
        .futureValue

      val signature = cryptoSnapshot
        .sign(fullUnassignmentTree.rootHash.unwrap, SigningKeyUsage.ProtocolOnly)
        .value
        .onShutdown(fail("unexpected shutdown during a test"))
        .futureValue
        .toOption

      unassignmentProcessingSteps
        .constructPendingDataAndResponse(
          mkParsedRequest(
            fullUnassignmentTree,
            Recipients.cc(submittingParticipant),
            signatureO = signature,
          ),
          state.reassignmentCache,
          FutureUnlessShutdown.pure(mkActivenessResult()),
          engineController = EngineController(
            submittingParticipant,
            RequestId(CantonTimestamp.Epoch),
            loggerFactory,
          ),
        )
        .value
        .onShutdown(fail("unexpected shutdown during a test"))
        .futureValue

    }

    "succeed without errors" in {
      constructPendingDataAndResponseWith(unassignmentProcessingSteps).valueOrFail(
        "construction of pending data and response failed"
      )
      succeed
    }

    // TODO(i13201) This should ideally be covered in integration tests as well
    "prevent the contract being reassigned is not vetted on the target synchronizer" in {
      val unassignmentProcessingStepsWithoutPackages = {
        val f = createCryptoFactory(packages = Seq.empty)
        val s = createCryptoSnapshot(f)
        val c = createReassignmentCoordination(s)
        createUnassignmentProcessingSteps(c)
      }

      constructPendingDataAndResponseWith(
        unassignmentProcessingStepsWithoutPackages
      ).value.pendingData.unassignmentValidationResult.reassigningParticipantValidationResult.head shouldBe a[
        PackageIdUnknownOrUnvetted
      ]
    }
  }

  "get commit set and contracts to be stored and event" should {
    "succeed without errors" in {
      val state = mkState
      val reassignmentId = ReassignmentId(sourceSynchronizer, CantonTimestamp.Epoch)
      val rootHash = TestHash.dummyRootHash
      val reassignmentResult =
        ConfirmationResultMessage.create(
          sourceSynchronizer.unwrap,
          UnassignmentViewType,
          RequestId(CantonTimestamp.Epoch),
          rootHash,
          Verdict.Approve(testedProtocolVersion),
          testedProtocolVersion,
        )

      val synchronizerParameters = DynamicSynchronizerParametersWithValidity(
        DynamicSynchronizerParameters.defaultValues(testedProtocolVersion),
        CantonTimestamp.MinValue,
        None,
        targetSynchronizer.unwrap,
      )

      for {
        signedResult <- SignedProtocolMessage
          .trySignAndCreate(
            reassignmentResult,
            cryptoSnapshot,
            testedProtocolVersion,
          )
          .failOnShutdown
        deliver: Deliver[OpenEnvelope[SignedProtocolMessage[ConfirmationResultMessage]]] = {
          val batch: Batch[OpenEnvelope[SignedProtocolMessage[ConfirmationResultMessage]]] =
            Batch.of(testedProtocolVersion, (signedResult, Recipients.cc(submittingParticipant)))
          Deliver.create(
            None,
            CantonTimestamp.Epoch,
            sourceSynchronizer.unwrap,
            Some(MessageId.tryCreate("msg-0")),
            batch,
            None,
            testedProtocolVersion,
            Option.empty[TrafficReceipt],
          )
        }
        signedContent = SignedContent(
          deliver,
          SymbolicCrypto.emptySignature,
          None,
          testedProtocolVersion,
        )
        assignmentExclusivity = synchronizerParameters
          .assignmentExclusivityLimitFor(timeProof.timestamp)
          .value

        fullUnassignmentTree = makeFullUnassignmentTree(unassignmentRequest)

        unassignmentValidationResult = UnassignmentValidationResult(
          fullTree = fullUnassignmentTree,
          reassignmentId = reassignmentId,
          assignmentExclusivity = Some(Target(assignmentExclusivity)),
          hostedStakeholders = Set(party1),
          validationResult = UnassignmentValidationResult.ValidationResult(
            activenessResult = mkActivenessResult(),
            participantSignatureVerificationResult = None,
            contractAuthenticationResultF = EitherT.right(FutureUnlessShutdown.unit),
            submitterCheckResult = None,
            reassigningParticipantValidationResult = Nil,
          ),
        )

        pendingUnassignment = PendingUnassignment(
          RequestId(CantonTimestamp.Epoch),
          RequestCounter(1),
          SequencerCounter(1),
          unassignmentValidationResult,
          MediatorGroupRecipient(MediatorGroupIndex.one),
          locallyRejectedF = FutureUnlessShutdown.pure(false),
          abortEngine = _ => (),
          engineAbortStatusF = FutureUnlessShutdown.pure(EngineAbortStatus.notAborted),
        )
        _ <- valueOrFail(
          unassignmentProcessingSteps
            .getCommitSetAndContractsToBeStoredAndEvent(
              NoOpeningErrors(signedContent),
              reassignmentResult.verdict,
              pendingUnassignment,
              state.pendingUnassignmentSubmissions,
              crypto.pureCrypto,
            )
            .failOnShutdown
        )("get commit set and contract to be stored and event")
      } yield succeed
    }
  }

  "verify the submitting participant signature" should {
    val fullUnassignmentTree = makeFullUnassignmentTree(unassignmentRequest)

    "succeed when the signature is correct" in {
      for {
        signature <- cryptoSnapshot
          .sign(fullUnassignmentTree.rootHash.unwrap, SigningKeyUsage.ProtocolOnly)
          .failOnShutdown

        parsed = mkParsedRequest(
          fullUnassignmentTree,
          Recipients.cc(submittingParticipant),
          signatureO = Some(signature),
        )

        authenticationError <- EitherT
          .liftF[FutureUnlessShutdown, SyncCryptoError, Option[AuthenticationError]](
            AuthenticationValidator.verifyViewSignature(parsed)
          )
          .failOnShutdown
      } yield authenticationError shouldBe None
    }

    "fail when the signature is missing" in {
      val parsed = mkParsedRequest(
        fullUnassignmentTree,
        Recipients.cc(submittingParticipant),
        signatureO = None,
      )
      for {
        authenticationError <-
          AuthenticationValidator.verifyViewSignature(parsed).failOnShutdown
      } yield authenticationError shouldBe Some(
        AuthenticationError.MissingSignature(parsed.requestId, ViewPosition(List()))
      )
    }

    "fail when the signature is incorrect" in {
      for {
        signature <- cryptoSnapshot
          .sign(TestHash.digest("wrong signature"), SigningKeyUsage.ProtocolOnly)
          .valueOrFailShutdown("signing failed")

        parsed = mkParsedRequest(
          fullUnassignmentTree,
          Recipients.cc(submittingParticipant),
          signatureO = Some(signature),
        )
        authenticationError <-
          AuthenticationValidator.verifyViewSignature(parsed).failOnShutdown
      } yield {
        parsed.requestId
        authenticationError.value should matchPattern {
          case AuthenticationError.InvalidSignature(_, ViewPosition(List()), _) =>
        }
      }
    }
  }

  def makeFullUnassignmentTree(
      request: UnassignmentRequest,
      uuid: UUID = new UUID(6L, 7L),
  ): FullUnassignmentTree = {
    val seed = seedGenerator.generateSaltSeed()
    request.toFullUnassignmentTree(crypto.pureCrypto, crypto.pureCrypto, seed, uuid)
  }

  def encryptUnassignmentTree(
      tree: FullUnassignmentTree,
      recipients: Recipients,
      sessionKeyStore: ConfirmationRequestSessionKeyStore,
  ): Future[EncryptedViewMessage[UnassignmentViewType]] =
    for {
      viewsToKeyMap <- EncryptedViewMessageFactory
        .generateKeysFromRecipients(
          Seq((ViewHashAndRecipients(tree.viewHash, recipients), None, tree.informees.toList)),
          parallel = true,
          crypto.pureCrypto,
          cryptoSnapshot,
          sessionKeyStore,
        )
        .valueOrFailShutdown("cannot generate encryption key for unassignment request")
      ViewKeyData(_, viewKey, viewKeyMap) = viewsToKeyMap(tree.viewHash)
      encryptedTree <- EncryptedViewMessageFactory
        .create(UnassignmentViewType)(
          tree,
          (viewKey, viewKeyMap),
          cryptoSnapshot,
          testedProtocolVersion,
        )(
          implicitly[TraceContext],
          executorService,
        )
        .valueOrFailShutdown("failed to encrypt unassignment request")
    } yield encryptedTree

  def makeRootHashMessage(
      request: FullUnassignmentTree
  ): RootHashMessage[SerializedRootHashMessagePayload] =
    RootHashMessage(
      request.rootHash,
      sourceSynchronizer.unwrap,
      testedProtocolVersion,
      UnassignmentViewType,
      testTopologyTimestamp,
      SerializedRootHashMessagePayload.empty,
    )
}
