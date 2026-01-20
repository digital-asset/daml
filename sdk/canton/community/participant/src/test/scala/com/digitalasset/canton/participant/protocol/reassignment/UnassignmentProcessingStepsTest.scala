// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.Eval
import cats.data.EitherT
import cats.implicits.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{
  DefaultProcessingTimeouts,
  SessionEncryptionKeyCacheConfig,
  TopologyConfig,
}
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.crypto.{
  Signature,
  SigningKeyUsage,
  SyncCryptoError,
  SynchronizerCrypto,
  SynchronizerCryptoClient,
  SynchronizerSnapshotSyncCryptoApi,
  TestHash,
}
import com.digitalasset.canton.data.*
import com.digitalasset.canton.data.ViewType.UnassignmentViewType
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.lifecycle.{DefaultPromiseUnlessShutdownFactory, FutureUnlessShutdown}
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.participant.ParticipantNodeParameters
import com.digitalasset.canton.participant.admin.party.OnboardingClearanceScheduler
import com.digitalasset.canton.participant.event.RecordOrderPublisher
import com.digitalasset.canton.participant.ledger.api.{LedgerApiIndexer, LedgerApiStore}
import com.digitalasset.canton.participant.metrics.ParticipantTestMetrics
import com.digitalasset.canton.participant.protocol.EngineController.EngineAbortStatus
import com.digitalasset.canton.participant.protocol.conflictdetection.ConflictDetectionHelpers.{
  mkActivenessResult,
  mkActivenessSet,
}
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentDataHelpers.TestValidator
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.*
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentValidationError.ContractValidationError
import com.digitalasset.canton.participant.protocol.reassignment.UnassignmentProcessingSteps.PendingUnassignment
import com.digitalasset.canton.participant.protocol.reassignment.UnassignmentProcessorError.*
import com.digitalasset.canton.participant.protocol.reassignment.UnassignmentValidationError.PackageIdUnknownOrUnvetted
import com.digitalasset.canton.participant.protocol.reassignment.UnassignmentValidationResult.{
  CommonValidationResult,
  ReassigningParticipantValidationResult,
}
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
import com.digitalasset.canton.participant.protocol.{EngineController, ProcessingStartingPoints}
import com.digitalasset.canton.participant.store.ActiveContractStore.Active
import com.digitalasset.canton.participant.store.memory.*
import com.digitalasset.canton.participant.store.{
  AcsCounterParticipantConfigStore,
  SyncPersistentState,
}
import com.digitalasset.canton.participant.sync.SyncEphemeralState
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.sequencing.traffic.TrafficReceipt
import com.digitalasset.canton.store.memory.InMemoryIndexedStringStore
import com.digitalasset.canton.store.{
  ConfirmationRequestSessionKeyStore,
  IndexedPhysicalSynchronizer,
  IndexedSynchronizer,
  SessionKeyStoreWithInMemoryCache,
}
import com.digitalasset.canton.time.SynchronizerTimeTracker.DummyTickRequest
import com.digitalasset.canton.time.{SynchronizerTimeTracker, WallClock}
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
import com.digitalasset.canton.util.{ContractValidator, ReassignmentTag, ResourceUtil}
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
import com.google.rpc.status.Status
import io.grpc.Status.Code.FAILED_PRECONDITION
import org.scalatest
import org.scalatest.Assertion
import org.scalatest.wordspec.AsyncWordSpec

import java.time.Instant
import java.util.UUID
import scala.annotation.nowarn
import scala.concurrent.duration.{DurationInt, FiniteDuration}
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
  private lazy val sourceMediator = MediatorGroupRecipient(MediatorGroupIndex.zero)
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
  private lazy val logicalPersistentState =
    new InMemoryLogicalSyncPersistentState(
      IndexedSynchronizer.tryCreate(sourceSynchronizer.unwrap, 1),
      enableAdditionalConsistencyChecks = true,
      indexedStringStore = indexedStringStore,
      contractStore = contractStore,
      acsCounterParticipantConfigStore = mock[AcsCounterParticipantConfigStore],
      Eval.now(mock[LedgerApiStore]),
      loggerFactory,
    )
  private lazy val physicalSyncPersistentState = new InMemoryPhysicalSyncPersistentState(
    submittingParticipant,
    clock,
    SynchronizerCrypto(crypto, defaultStaticSynchronizerParameters),
    IndexedPhysicalSynchronizer.tryCreate(sourceSynchronizer.unwrap, 1),
    defaultStaticSynchronizerParameters,
    parameters = ParticipantNodeParameters.forTestingOnly(testedProtocolVersion),
    topologyConfig = TopologyConfig.forTesting,
    packageMetadataView = mock[PackageMetadataView],
    Eval.now(mock[LedgerApiStore]),
    logicalPersistentState,
    loggerFactory,
    timeouts,
    futureSupervisor,
  )

  val persistentState =
    new SyncPersistentState(logicalPersistentState, physicalSyncPersistentState, loggerFactory)

  private def mkState: SyncEphemeralState =
    new SyncEphemeralState(
      submittingParticipant,
      mock[RecordOrderPublisher],
      mock[SynchronizerTimeTracker],
      mock[InFlightSubmissionSynchronizerTracker],
      mock[OnboardingClearanceScheduler],
      persistentState,
      ledgerApiIndexer,
      contractStore,
      new DefaultPromiseUnlessShutdownFactory(timeouts, loggerFactory),
      ProcessingStartingPoints.default,
      ParticipantTestMetrics.synchronizer,
      exitOnFatalFailures = true,
      // Disable the session encryption key cache: it starts a scheduler that must be closed properly,
      // otherwise we see RejectedExecutionException warnings during shutdown.
      SessionEncryptionKeyCacheConfig(enabled = false),
      DefaultProcessingTimeouts.testing,
      loggerFactory,
      FutureSupervisor.Noop,
      clock,
    )

  private lazy val unassignmentRequest = UnassignmentRequest(
    submitterMetadata = submitterMetadata(party1),
    reassigningParticipants = Set(submittingParticipant),
    ContractsReassignmentBatch(
      contract,
      sourceValidationPackageId,
      targetValidationPackageId,
      initialReassignmentCounter,
    ),
    sourceSynchronizer,
    sourceMediator,
    targetSynchronizer,
    targetTs,
  )

  private def createTestingIdentityFactory(
      topology: Map[ParticipantId, Map[LfPartyId, ParticipantPermission]],
      packages: Map[ParticipantId, Seq[LfPackageId]],
      synchronizers: Set[PhysicalSynchronizerId] = Set(DefaultTestIdentities.physicalSynchronizerId),
  ) =
    TestingTopology(synchronizers)
      .withReversedTopology(topology)
      .withPackages(packages)
      .build(loggerFactory)

  private val defaultTopologyPackageIds = Seq(
    sourceValidationPackageId.unwrap,
    targetValidationPackageId.unwrap,
    ExampleContractFactory.packageId,
  )

  private def createTestingTopologySnapshot(
      topology: Map[ParticipantId, Map[LfPartyId, ParticipantPermission]],
      packagesOverride: Option[Map[ParticipantId, Seq[LfPackageId]]] = None,
  ): TopologySnapshot = {

    val defaultPackages = topology.keys.map(_ -> defaultTopologyPackageIds).toMap

    val packages = packagesOverride.getOrElse(defaultPackages)
    createTestingIdentityFactory(topology, packages).topologySnapshot()
  }

  private def createCryptoFactory(
      packages: Seq[LfPackageId] = defaultTopologyPackageIds
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

  private def createCryptoClient(
      testingIdentityFactory: TestingIdentityFactory = cryptoFactory
  ) =
    testingIdentityFactory
      .forOwnerAndSynchronizer(submittingParticipant, sourceSynchronizer.unwrap)

  private lazy val cryptoClient = createCryptoClient()
  private lazy val cryptoSnapshot = cryptoClient.currentSnapshotApproximation.futureValueUS

  private lazy val seedGenerator = new SeedGenerator(crypto.pureCrypto)

  private def createReassignmentCoordination(
      cryptoSnapshot: SynchronizerSnapshotSyncCryptoApi = cryptoSnapshot,
      approximateTimestamp: CantonTimestamp = CantonTimestamp.Epoch,
      targetTimestampForwardTolerance: FiniteDuration = 30.seconds,
  ) =
    TestReassignmentCoordination(
      synchronizers = Set(Target(sourceSynchronizer.unwrap), targetSynchronizer),
      timeProofTimestamp = approximateTimestamp,
      snapshotOverride = Some(cryptoSnapshot),
      awaitTimestampOverride = Some(None),
      loggerFactory = loggerFactory,
      packages = Seq(ExampleContractFactory.packageId),
      targetTimestampForwardTolerance = targetTimestampForwardTolerance,
    )(directExecutionContext)

  private lazy val coordination: ReassignmentCoordination =
    createReassignmentCoordination()

  private def createUnassignmentProcessingSteps(
      reassignmentCoordination: ReassignmentCoordination = coordination,
      cryptoClient: SynchronizerCryptoClient = cryptoClient,
      contractValidator: ContractValidator = ContractValidator.AllowAll,
  ) =
    new UnassignmentProcessingSteps(
      sourceSynchronizer,
      submittingParticipant,
      reassignmentCoordination,
      cryptoClient,
      seedGenerator,
      Source(defaultStaticSynchronizerParameters),
      contractValidator,
      clock,
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

  private lazy val targetTs = Target(CantonTimestamp.Epoch)

  private lazy val contract = ExampleContractFactory.build(
    signatories = Set(submitter),
    stakeholders = Set(submitter, party1),
  )
  private lazy val contractId = contract.contractId

  private lazy val sourceValidationPackageId = Source(
    LfPackageId.assertFromString("source-rep-pkg-id")
  )
  private lazy val targetValidationPackageId = Target(
    LfPackageId.assertFromString("target-rep-pkg-id")
  )

  private val reassignmentId = ReassignmentId.tryCreate("00")

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
    areContractsUnknown = false,
    Seq.empty,
    sourceMediator,
    cryptoSnapshot,
    cryptoSnapshot.ipsSnapshot.findDynamicSynchronizerParameters().futureValueUS.value,
    reassignmentId,
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
        contractValidator: ContractValidator = ContractValidator.AllowAll,
        stakeholdersOverride: Option[Stakeholders] = None,
    ): Either[ReassignmentValidationError, UnassignmentRequestValidated] = {
      val updatedContract = stakeholdersOverride.fold(contract)(stakeholders =>
        ExampleContractFactory.build(
          signatories = stakeholders.signatories,
          stakeholders = stakeholders.all,
          overrideContractId = Some(contract.contractId),
        )
      )
      mkUnassignmentResultForContract(
        sourceTopologySnapshot,
        targetTopologySnapshot,
        updatedContract,
        contractValidator: ContractValidator,
      )
    }

    def mkUnassignmentResultForContract(
        sourceTopologySnapshot: TopologySnapshot,
        targetTopologySnapshot: TopologySnapshot,
        updatedContract: ContractInstance,
        contractValidator: ContractValidator = ContractValidator.AllowAll,
    ): Either[ReassignmentValidationError, UnassignmentRequestValidated] =
      UnassignmentRequest
        .validated(
          submittingParticipant,
          ContractsReassignmentBatch(
            updatedContract,
            sourceValidationPackageId,
            targetValidationPackageId,
            initialReassignmentCounter,
          ),
          contractValidator,
          submitterMetadata(submitter),
          sourceSynchronizer,
          sourceMediator,
          targetSynchronizer,
          Source(sourceTopologySnapshot),
          Target(targetTopologySnapshot),
        )
        .value
        .failOnShutdown
        .futureValue

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

    def testInvalidRepresentativeContract(invalidPackageId: LfPackageId): scalatest.Assertion = {
      val expected = "invalid-contract"

      val contractValidator =
        new ReassignmentDataHelpers.TestValidator(
          Map(
            (contract.contractId, invalidPackageId) -> expected
          )
        )

      inside(
        mkUnassignmentResult(
          testingTopology,
          testingTopology,
          contractValidator = contractValidator,
        ).left.value
      ) { case actual: ContractValidationError =>
        actual.reassignmentRef shouldBe ReassignmentRef(contract.contractId)
        actual.contractId shouldBe contract.contractId
        actual.representativePackageId shouldBe invalidPackageId
        actual.reason should include(expected)
      }
    }

    "fail if contract does not authenticate against source validation package" in {
      testInvalidRepresentativeContract(sourceValidationPackageId.unwrap)
    }

    "fail if contract does not authenticate against target validation package" in {
      testInvalidRepresentativeContract(targetValidationPackageId.unwrap)
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

      val stakeholders = Stakeholders.tryCreate(Set(submitter, party1), Set(submitter))
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

    def testPackageVettingFailure(
        participantId: ParticipantId,
        missingPackageId: ReassignmentTag[LfPackageId],
        expectedSychronizerId: PhysicalSynchronizerId,
    ): Assertion = {

      val packagesOverride =
        Seq(submittingParticipant, participant1)
          .map(_ -> Seq(sourceValidationPackageId, targetValidationPackageId).map(_.unwrap))
          .toMap

      val modifiedPackageOverride =
        packagesOverride.map {
          case (k, v) if k == participantId => (k, v.filterNot(_ == missingPackageId.unwrap))
          case other => other
        }

      val (sourcePackagesOverride, targetPackagesOverride) = missingPackageId match {
        case Source(_) => (modifiedPackageOverride, packagesOverride)
        case Target(_) => (packagesOverride, modifiedPackageOverride)
      }

      val sourceSynchronizerTopology =
        createTestingTopologySnapshot(
          Map(
            submittingParticipant -> Map(submitter -> Submission),
            participant1 -> Map(party1 -> Submission),
          ),
          packagesOverride = Some(sourcePackagesOverride),
        )

      val targetSynchronizerTopology =
        createTestingTopologySnapshot(
          topology = Map(
            submittingParticipant -> Map(submitter -> Submission),
            participant1 -> Map(party1 -> Submission),
          ),
          packagesOverride = Some(targetPackagesOverride),
        )

      val stakeholders =
        Stakeholders.tryCreate(Set(submitter, adminSubmitter, admin1), Set(submitter))
      val result = mkUnassignmentResult(
        sourceTopologySnapshot = sourceSynchronizerTopology,
        targetTopologySnapshot = targetSynchronizerTopology,
        stakeholdersOverride = Some(stakeholders),
      )

      val expected = PackageIdUnknownOrUnvetted(
        Set(contractId),
        unknownTo = List(PackageUnknownTo(missingPackageId.unwrap, participantId)),
        expectedSychronizerId,
      )

      result.left.value shouldBe expected
    }

    // TODO(i13201) This should ideally be covered in integration tests as well
    "fail vetting if source validation packages are not vetted on source synchronizer" in {
      forEvery(List(submittingParticipant, participant1)) { participantId =>
        testPackageVettingFailure(
          participantId,
          sourceValidationPackageId,
          sourceSynchronizer.unwrap,
        )
      }
    }

    // TODO(i13201) This should ideally be covered in integration tests as well
    "fail vetting if target validation packages are not vetted in target synchronizer" in {
      forEvery(List(submittingParticipant, participant1)) { participantId =>
        testPackageVettingFailure(
          participantId,
          targetValidationPackageId,
          targetSynchronizer.unwrap,
        )
      }
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
            contracts = ContractsReassignmentBatch(
              contract,
              sourceValidationPackageId,
              targetValidationPackageId,
              initialReassignmentCounter,
            ),
            sourceSynchronizer = sourceSynchronizer,
            sourceMediator = sourceMediator,
            targetSynchronizer = targetSynchronizer,
            targetTimestamp = targetTs,
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
            contracts = ContractsReassignmentBatch(
              contract,
              sourceValidationPackageId,
              targetValidationPackageId,
              initialReassignmentCounter,
            ),
            sourceSynchronizer = sourceSynchronizer,
            sourceMediator = sourceMediator,
            targetSynchronizer = targetSynchronizer,
            targetTimestamp = targetTs,
          ),
          Set(submittingParticipant, participant1, participant2, participant3, participant4),
        )
    }

    "allow admin parties as stakeholders" in {

      val updatedContract =
        ExampleContractFactory.build(
          signatories = Set(submitter),
          stakeholders = Set(submitter, adminSubmitter, admin1),
        )

      val unassignmentResult =
        mkUnassignmentResultForContract(testingTopology, testingTopology, updatedContract).value

      val expectedUnassignmentResult = UnassignmentRequestValidated(
        UnassignmentRequest(
          submitterMetadata = submitterMetadata(submitter),
          // Because admin1 is a stakeholder, participant1 is reassigning
          reassigningParticipants = Set(submittingParticipant, participant1),
          contracts = ContractsReassignmentBatch(
            updatedContract,
            sourceValidationPackageId,
            targetValidationPackageId,
            initialReassignmentCounter,
          ),
          sourceSynchronizer = sourceSynchronizer,
          sourceMediator = sourceMediator,
          targetSynchronizer = targetSynchronizer,
          targetTimestamp = targetTs,
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
          overrideSourceValidationPkgIds = Map.empty,
          overrideTargetValidationPkgIds = Map.empty,
        )

      for {
        _ <- state.contractStore.storeContract(contract)
        _ <- persistentState.activeContractStore
          .markContractsCreated(
            Seq(contractId -> initialReassignmentCounter),
            TimeOfChange(targetTs.unwrap),
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
      val contract = ExampleContractFactory.build()
      val submissionParam = UnassignmentProcessingSteps.SubmissionParam(
        submitterMetadata = submitterMetadata(party1),
        Seq(contract.contractId),
        Target(sourceSynchronizer.unwrap),
        overrideSourceValidationPkgIds = Map.empty,
        overrideTargetValidationPkgIds = Map.empty,
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

    def checkContractsValidateAgainstValidationPackage(
        invalidPackageId: ReassignmentTag[LfPackageId]
    ): Future[scalatest.Assertion] = {
      val state = mkState
      val submissionParam =
        UnassignmentProcessingSteps.SubmissionParam(
          submitterMetadata = submitterMetadata(party1),
          Seq(contractId),
          targetSynchronizer,
          overrideSourceValidationPkgIds = invalidPackageId match {
            case Source(rpId) => Map(contractId -> rpId)
            case Target(_) => Map.empty
          },
          overrideTargetValidationPkgIds = invalidPackageId match {
            case Source(_) => Map.empty
            case Target(rpId) => Map(contractId -> rpId)
          },
        )

      val contractValidator =
        new TestValidator(Map((contractId, invalidPackageId.unwrap) -> "Invalid, as expected"))

      val unassignmentProcessingSteps =
        createUnassignmentProcessingSteps(contractValidator = contractValidator)

      for {
        _ <- state.contractStore.storeContract(contract)
        _ <- persistentState.activeContractStore
          .markContractsCreated(
            Seq(contractId -> initialReassignmentCounter),
            TimeOfChange(targetTs.unwrap),
          )
          .value
        submissionResult <- leftOrFail(
          unassignmentProcessingSteps
            .createSubmission(
              submissionParam,
              sourceMediator,
              state,
              cryptoSnapshot,
            )
        )("prepare submission succeeded unexpectedly")
      } yield {
        inside(submissionResult) { case SubmissionValidationError(message) =>
          message should include regex s"contract authentication failure.*${contract.contractId.coid}.*${invalidPackageId.unwrap}"
        }
      }
    }

    "check that the contracts validate against the source validation package" in {
      checkContractsValidateAgainstValidationPackage(
        sourceValidationPackageId
      )
    }

    "check that the contracts validate against the target validation package" in {
      checkContractsValidateAgainstValidationPackage(
        targetValidationPackageId
      )
    }

  }

  "receive request" should {
    val unassignmentTree = makeFullUnassignmentTree(unassignmentRequest)
    "succeed without errors" in {
      ResourceUtil.withResourceM(
        new SessionKeyStoreWithInMemoryCache(
          SessionEncryptionKeyCacheConfig(),
          timeouts,
          loggerFactory,
        )
      ) { sessionKeyStore =>
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
  }

  "construct pending data and response" should {

    def constructPendingDataAndResponseWith(
        unassignmentProcessingSteps: UnassignmentProcessingSteps,
        requestTargetTs: Target[CantonTimestamp] = targetTs,
    ) = {
      val state = mkState
      val unassignmentRequest = UnassignmentRequest(
        submitterMetadata = submitterMetadata(party1),
        reassigningParticipants = Set(submittingParticipant),
        ContractsReassignmentBatch(
          contract,
          sourceValidationPackageId,
          targetValidationPackageId,
          ReassignmentCounter(1),
        ),
        sourceSynchronizer,
        sourceMediator,
        targetSynchronizer,
        requestTargetTs,
      )
      val fullUnassignmentTree = makeFullUnassignmentTree(unassignmentRequest)

      state.contractStore
        .storeContract(contract)
        .failOnShutdown
        .futureValue

      val signature = cryptoSnapshot
        .sign(fullUnassignmentTree.rootHash.unwrap, SigningKeyUsage.ProtocolOnly, None)
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
          FutureUnlessShutdown.pure(
            mkActivenessResult(
              prior = Map(contract.contractId -> Some(Active(initialReassignmentCounter)))
            )
          ),
          engineController = EngineController(
            submittingParticipant,
            RequestId(CantonTimestamp.Epoch),
            loggerFactory,
          ),
          DummyTickRequest,
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
        val s = createCryptoClient(f).currentSnapshotApproximation.futureValueUS
        val c = createReassignmentCoordination(s)
        createUnassignmentProcessingSteps(c)
      }

      constructPendingDataAndResponseWith(
        unassignmentProcessingStepsWithoutPackages
      ).value.pendingData.unassignmentValidationResult.reassigningParticipantValidationResult.errors.head shouldBe a[
        PackageIdUnknownOrUnvetted
      ]
    }

    "with a requested target timestamp" should {
      val localTs = CantonTimestamp.assertFromInstant(Instant.parse("2020-01-01T00:00:00Z"))
      val forwardTolerance = 2.seconds

      def getConfirmationResponses(requestTargetTs: CantonTimestamp): Seq[ConfirmationResponse] =
        constructPendingDataAndResponseWith(
          createUnassignmentProcessingSteps(
            createReassignmentCoordination(
              approximateTimestamp = localTs,
              targetTimestampForwardTolerance = forwardTolerance,
            )
          ),
          requestTargetTs = Target(requestTargetTs),
        ).value.confirmationResponsesF.value.futureValueUS.value.value._1.responses

      "approve when less than forward-tolerance ahead of local timestamp" in {
        getConfirmationResponses(
          requestTargetTs = localTs.add(forwardTolerance).add(-1.millisecond)
        ) should matchPattern { case Seq(ConfirmationResponse(_, _: LocalApprove, _)) =>
        }
      }
      "approve when exactly forward-tolerance ahead of local timestamp" in {
        getConfirmationResponses(
          requestTargetTs = localTs.add(forwardTolerance)
        ) should matchPattern { case Seq(ConfirmationResponse(_, _: LocalApprove, _)) =>
        }
      }
      "abstain when more than forward-tolerance ahead of local timestamp" in {
        getConfirmationResponses(
          requestTargetTs = localTs.add(forwardTolerance).add(1.millisecond)
        ) should matchPattern {
          case Seq(ConfirmationResponse(_, LocalAbstain(Status(code, msg, _, _)), _))
              if code == FAILED_PRECONDITION.value && msg.contains(
                s"Non-validatable target timestamp when processing unassignment $reassignmentId"
              ) =>
        }
      }
    }
  }

  "get commit set and contracts to be stored and event" should {
    val state = mkState
    val rootHash = TestHash.dummyRootHash
    val reassignmentResult =
      ConfirmationResultMessage.create(
        sourceSynchronizer.unwrap,
        UnassignmentViewType,
        RequestId(CantonTimestamp.Epoch),
        rootHash,
        Verdict.Approve(testedProtocolVersion),
      )

    val synchronizerParameters = DynamicSynchronizerParametersWithValidity(
      DynamicSynchronizerParameters.defaultValues(testedProtocolVersion),
      CantonTimestamp.MinValue,
      None,
    )
    val signedResult = SignedProtocolMessage
      .trySignAndCreate(
        reassignmentResult,
        cryptoSnapshot,
        None,
      )
      .futureValueUS

    val deliver: Deliver[OpenEnvelope[SignedProtocolMessage[ConfirmationResultMessage]]] = {
      val batch: Batch[OpenEnvelope[SignedProtocolMessage[ConfirmationResultMessage]]] =
        Batch.of(testedProtocolVersion, (signedResult, Recipients.cc(submittingParticipant)))
      Deliver.create(
        None,
        CantonTimestamp.Epoch,
        sourceSynchronizer.unwrap,
        Some(MessageId.tryCreate("msg-0")),
        batch,
        None,
        Option.empty[TrafficReceipt],
      )
    }
    val signedContent = SignedContent(
      deliver,
      SymbolicCrypto.emptySignature,
      None,
      testedProtocolVersion,
    )
    val assignmentExclusivity = synchronizerParameters
      .assignmentExclusivityLimitFor(targetTs.unwrap)
      .value

    val fullUnassignmentTree = makeFullUnassignmentTree(unassignmentRequest)

    val unassignmentValidationResult = UnassignmentValidationResult(
      unassignmentData = UnassignmentData(fullUnassignmentTree, CantonTimestamp.Epoch),
      rootHash = fullUnassignmentTree.rootHash,
      assignmentExclusivity = Some(Target(assignmentExclusivity)),
      hostedConfirmingReassigningParties = Set(party1),
      commonValidationResult = CommonValidationResult(
        activenessResult = mkActivenessResult(),
        participantSignatureVerificationResult = None,
        contractAuthenticationResultF = EitherT.right(FutureUnlessShutdown.unit),
        submitterCheckResult = None,
      ),
      reassigningParticipantValidationResult = ReassigningParticipantValidationResult(errors = Nil),
    )

    val pendingUnassignment = PendingUnassignment(
      RequestId(CantonTimestamp.Epoch),
      RequestCounter(1),
      SequencerCounter(1),
      unassignmentValidationResult,
      MediatorGroupRecipient(MediatorGroupIndex.zero),
      locallyRejectedF = FutureUnlessShutdown.pure(false),
      abortEngine = _ => (),
      engineAbortStatusF = FutureUnlessShutdown.pure(EngineAbortStatus.notAborted),
      DummyTickRequest,
    )

    "succeed without errors" in {
      for {
        _ <- valueOrFail(
          unassignmentProcessingSteps
            .getCommitSetAndContractsToBeStoredAndEventFactory(
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

    "fail with mediator is not active anymore" in {
      for {
        _ <- loggerFactory.assertLoggedWarningsAndErrorsSeq(
          valueOrFail(
            unassignmentProcessingSteps
              .getCommitSetAndContractsToBeStoredAndEventFactory(
                NoOpeningErrors(signedContent),
                reassignmentResult.verdict,
                // request used MediatorGroupIndex.zero
                pendingUnassignment.copy(mediator = MediatorGroupRecipient(MediatorGroupIndex.one)),
                state.pendingUnassignmentSubmissions,
                crypto.pureCrypto,
              )
              .failOnShutdown
          )("get commit set and contract to be stored and event"),
          LogEntry.assertLogSeq(
            Seq(
              (
                _.shouldBeCantonErrorCode(LocalRejectError.MalformedRejects.MalformedRequest),
                "mediator is not active anymore",
              )
            )
          ),
        )
      } yield succeed
    }
  }

  "verify the submitting participant signature" should {
    val fullUnassignmentTree = makeFullUnassignmentTree(unassignmentRequest)

    "succeed when the signature is correct" in {
      for {
        signature <- cryptoSnapshot
          .sign(fullUnassignmentTree.rootHash.unwrap, SigningKeyUsage.ProtocolOnly, None)
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
          .sign(TestHash.digest("wrong signature"), SigningKeyUsage.ProtocolOnly, None)
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
          None,
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
      UnassignmentViewType,
      testTopologyTimestamp,
      SerializedRootHashMessagePayload.empty,
    )
}
