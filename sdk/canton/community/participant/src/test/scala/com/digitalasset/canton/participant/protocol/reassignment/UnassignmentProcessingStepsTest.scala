// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.Eval
import cats.implicits.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{CachingConfigs, DefaultProcessingTimeouts}
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.crypto.{DomainSnapshotSyncCryptoApi, Signature, TestHash}
import com.digitalasset.canton.data.ViewType.UnassignmentViewType
import com.digitalasset.canton.data.{
  CantonTimestamp,
  FullUnassignmentTree,
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
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.{
  NoReassignmentSubmissionPermission,
  ParsedReassignmentRequest,
  ReassignmentProcessorError,
}
import com.digitalasset.canton.participant.protocol.reassignment.UnassignmentProcessingSteps.PendingUnassignment
import com.digitalasset.canton.participant.protocol.reassignment.UnassignmentProcessorError.*
import com.digitalasset.canton.participant.protocol.submission.EncryptedViewMessageFactory.{
  ViewHashAndRecipients,
  ViewKeyData,
}
import com.digitalasset.canton.participant.protocol.submission.{
  EncryptedViewMessageFactory,
  SeedGenerator,
}
import com.digitalasset.canton.participant.protocol.{EngineController, ProcessingStartingPoints}
import com.digitalasset.canton.participant.store.memory.*
import com.digitalasset.canton.participant.store.{
  ParticipantNodeEphemeralState,
  SyncDomainEphemeralState,
}
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.sequencing.traffic.TrafficReceipt
import com.digitalasset.canton.store.memory.InMemoryIndexedStringStore
import com.digitalasset.canton.store.{
  ConfirmationRequestSessionKeyStore,
  IndexedDomain,
  SessionKeyStoreWithInMemoryCache,
}
import com.digitalasset.canton.time.{DomainTimeTracker, TimeProofTestUtil, WallClock}
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.*
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
import com.digitalasset.canton.version.Reassignment.{SourceProtocolVersion, TargetProtocolVersion}
import com.digitalasset.canton.{
  BaseTest,
  HasExecutorService,
  LedgerApplicationId,
  LedgerCommandId,
  LfPackageId,
  LfPackageName,
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
    with HasTestCloseContext {

  private implicit val ec: ExecutionContext = executorService

  private val testTopologyTimestamp = CantonTimestamp.Epoch

  private lazy val sourceDomain = Source(
    DomainId(UniqueIdentifier.tryFromProtoPrimitive("source::domain"))
  )
  private lazy val sourceMediator = MediatorGroupRecipient(MediatorGroupIndex.tryCreate(100))
  private lazy val targetDomain = Target(
    DomainId(UniqueIdentifier.tryFromProtoPrimitive("target::domain"))
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

  private lazy val templateId =
    LfTemplateId.assertFromString("unassignmentprocessingstepstestpackage:template:id")
  private lazy val packageName =
    LfPackageName.assertFromString("unassignmentprocessingstepstestpackagename")

  private lazy val initialReassignmentCounter: ReassignmentCounter = ReassignmentCounter.Genesis

  private def submitterMetadata(submitter: LfPartyId): ReassignmentSubmitterMetadata =
    ReassignmentSubmitterMetadata(
      submitter,
      submittingParticipant,
      LedgerCommandId.assertFromString("unassignment-processing-steps-command-id"),
      submissionId = None,
      LedgerApplicationId.assertFromString("tests"),
      workflowId = None,
    )

  private lazy val adminSubmitter: LfPartyId = submittingParticipant.adminParty.toLf

  private lazy val crypto =
    SymbolicCrypto.create(testedReleaseProtocolVersion, timeouts, loggerFactory)

  private lazy val ledgerApiIndexer = mock[LedgerApiIndexer]
  private lazy val clock = new WallClock(timeouts, loggerFactory)
  private lazy val indexedStringStore = new InMemoryIndexedStringStore(minIndex = 1, maxIndex = 1)
  private lazy val persistentState =
    new InMemorySyncDomainPersistentState(
      submittingParticipant,
      clock,
      crypto,
      IndexedDomain.tryCreate(sourceDomain.unwrap, 1),
      defaultStaticDomainParameters,
      enableAdditionalConsistencyChecks = true,
      indexedStringStore = indexedStringStore,
      exitOnFatalFailures = true,
      packageDependencyResolver = mock[PackageDependencyResolver],
      Eval.now(mock[LedgerApiStore]),
      loggerFactory,
      timeouts,
      futureSupervisor,
    )

  private def mkState: SyncDomainEphemeralState =
    new SyncDomainEphemeralState(
      submittingParticipant,
      mock[ParticipantNodeEphemeralState],
      persistentState,
      ledgerApiIndexer,
      ProcessingStartingPoints.default,
      () => mock[DomainTimeTracker],
      ParticipantTestMetrics.domain,
      exitOnFatalFailures = true,
      CachingConfigs.defaultSessionKeyCacheConfig,
      DefaultProcessingTimeouts.testing,
      loggerFactory,
      FutureSupervisor.Noop,
      clock,
    )

  private lazy val damle =
    DAMLeTestInstance(submittingParticipant, signatories = Set(party1), stakeholders = Set(party1))(
      loggerFactory
    )

  private def createTestingIdentityFactory(
      topology: Map[ParticipantId, Map[LfPartyId, ParticipantPermission]],
      packages: Map[ParticipantId, Seq[LfPackageId]] = Map.empty,
      domains: Set[DomainId] = Set(DefaultTestIdentities.domainId),
  ) =
    TestingTopology(domains)
      .withReversedTopology(topology)
      .withPackages(packages)
      .build(loggerFactory)

  private def createTestingTopologySnapshot(
      topology: Map[ParticipantId, Map[LfPartyId, ParticipantPermission]],
      packages: Map[ParticipantId, Seq[LfPackageId]] = Map.empty,
  ): TopologySnapshot =
    createTestingIdentityFactory(topology, packages).topologySnapshot()

  private def createCryptoFactory(packages: Seq[LfPackageId] = Seq(templateId.packageId)) = {
    val topology = Map(
      submittingParticipant -> Map(
        party1 -> ParticipantPermission.Submission,
        submittingParticipant.adminParty.toLf -> ParticipantPermission.Submission,
      )
    )
    createTestingIdentityFactory(
      topology = topology,
      packages = topology.keys.map(_ -> packages).toMap,
      domains = Set(sourceDomain.unwrap, targetDomain.unwrap),
    )
  }

  private lazy val cryptoFactory = createCryptoFactory()

  private def createCryptoSnapshot(
      testingIdentityFactory: TestingIdentityFactory = cryptoFactory
  ) =
    testingIdentityFactory
      .forOwnerAndDomain(submittingParticipant, sourceDomain.unwrap)
      .currentSnapshotApproximation

  private lazy val cryptoSnapshot = createCryptoSnapshot()

  private lazy val seedGenerator = new SeedGenerator(crypto.pureCrypto)

  private def createReassignmentCoordination(
      cryptoSnapshot: DomainSnapshotSyncCryptoApi = cryptoSnapshot
  ) =
    TestReassignmentCoordination(
      Set(Target(sourceDomain.unwrap), targetDomain),
      CantonTimestamp.Epoch,
      Some(cryptoSnapshot),
      Some(None),
      loggerFactory,
      Seq(templateId.packageId),
    )(directExecutionContext)

  private lazy val coordination: ReassignmentCoordination =
    createReassignmentCoordination()

  private def createUnassignmentProcessingSteps(
      reassignmentCoordination: ReassignmentCoordination = coordination
  ) =
    new UnassignmentProcessingSteps(
      sourceDomain,
      submittingParticipant,
      damle,
      reassignmentCoordination,
      seedGenerator,
      Source(defaultStaticDomainParameters),
      SourceProtocolVersion(testedProtocolVersion),
      loggerFactory,
    )(executorService)

  private lazy val unassignmentProcessingSteps: UnassignmentProcessingSteps =
    createUnassignmentProcessingSteps()

  private lazy val Seq(
    (participant1, admin1),
    (participant2, _),
    (participant3, admin3),
    (participant4, admin4),
  ) =
    (1 to 4).map { i =>
      val participant =
        ParticipantId(UniqueIdentifier.tryFromProtoPrimitive(s"participant$i::participant"))
      val admin = participant.adminParty.toLf
      participant -> admin
    }

  private lazy val timeEvent =
    TimeProofTestUtil.mkTimeProof(timestamp = CantonTimestamp.Epoch, targetDomain = targetDomain)

  private lazy val contractId = ExampleTransactionFactory.suffixedId(10, 0)

  private lazy val contract = ExampleTransactionFactory.asSerializable(
    contractId,
    contractInstance = ExampleTransactionFactory.contractInstance(templateId = templateId),
    metadata = ContractMetadata.tryCreate(
      signatories = Set(submitter),
      stakeholders = Set(submitter),
      maybeKeyWithMaintainersVersioned = None,
    ),
  )
  private lazy val creatingTransactionId = ExampleTransactionFactory.transactionId(0)

  def mkParsedRequest(
      view: FullUnassignmentTree,
      recipients: Recipients = RecipientsTest.testInstance,
      signatureO: Option[Signature] = None,
  ): ParsedReassignmentRequest[FullUnassignmentTree] = ParsedReassignmentRequest(
    RequestCounter(1),
    CantonTimestamp.Epoch,
    SequencerCounter(1),
    view,
    recipients,
    signatureO,
    None,
    isFreshOwnTimelyRequest = true,
    isReassigningParticipant = true,
    Seq.empty,
    sourceMediator,
    cryptoSnapshot,
    cryptoSnapshot.ipsSnapshot.findDynamicDomainParameters().futureValue.value,
  )

  "UnassignmentRequest.validated" should {
    val testingTopology = createTestingTopologySnapshot(
      Map(
        submittingParticipant -> Map(submitter -> Submission),
        participant1 -> Map(party1 -> Submission),
        participant2 -> Map(party2 -> Submission),
      ),
      packages = Seq(submittingParticipant, participant1, participant2)
        .map(_ -> Seq(templateId.packageId))
        .toMap,
    )

    def mkUnassignmentResult(
        stakeholders: Set[LfPartyId],
        sourceTopologySnapshot: TopologySnapshot,
        targetTopologySnapshot: TopologySnapshot,
    ): Either[ReassignmentProcessorError, UnassignmentRequestValidated] =
      UnassignmentRequest
        .validated(
          submittingParticipant,
          timeEvent,
          creatingTransactionId,
          contract,
          submitterMetadata(submitter),
          stakeholders,
          sourceDomain,
          SourceProtocolVersion(testedProtocolVersion),
          sourceMediator,
          targetDomain,
          TargetProtocolVersion(testedProtocolVersion),
          Source(sourceTopologySnapshot),
          Target(targetTopologySnapshot),
          initialReassignmentCounter,
        )
        .value
        .failOnShutdown
        .futureValue

    "fail if submitter is not a stakeholder" in {
      val stakeholders = Set(party1, party2)
      val result = mkUnassignmentResult(stakeholders, testingTopology, testingTopology)
      result.left.value shouldBe a[UnassignmentSubmitterMustBeStakeholder]
    }

    "fail if submitting participant does not have submission permission" in {
      val ipsNoSubmissionPermission =
        createTestingTopologySnapshot(Map(submittingParticipant -> Map(submitter -> Confirmation)))

      val result = mkUnassignmentResult(Set(submitter), ipsNoSubmissionPermission, testingTopology)
      result.left.value shouldBe a[NoReassignmentSubmissionPermission]
    }

    "fail if a stakeholder cannot submit on target domain" in {
      val ipsNoSubmissionOnTarget = createTestingTopologySnapshot(
        Map(
          submittingParticipant -> Map(submitter -> Submission),
          participant1 -> Map(party1 -> Confirmation),
        )
      )

      val stakeholders = Set(submitter, party1)
      val result = mkUnassignmentResult(stakeholders, testingTopology, ipsNoSubmissionOnTarget)
      result.left.value shouldBe a[PermissionErrors]
    }

    "fail if a stakeholder cannot confirm on target domain" in {
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

      val stakeholders = Set(submitter, party1)
      val result =
        mkUnassignmentResult(stakeholders, ipsConfirmationOnSource, ipsNoConfirmationOnTarget)

      val expectedError = StakeholderHostingErrors(
        s"The following stakeholders are not hosted with confirmation rights on target domain: Set($party1)"
      )

      result.left.value shouldBe expectedError
    }

    "fail if a stakeholder is not hosted on the same participant on both domains" in {
      val ipsDifferentParticipant = createTestingTopologySnapshot(
        Map(
          submittingParticipant -> Map(submitter -> Submission),
          participant1 -> Map(party1 -> Confirmation),
          participant2 -> Map(party1 -> Submission),
        )
      )

      val stakeholders = Set(submitter, party1)
      val result = mkUnassignmentResult(stakeholders, testingTopology, ipsDifferentParticipant)
      result.left.value shouldBe a[PermissionErrors]
    }

    // TODO(i13201) This should ideally be covered in integration tests as well
    "fail if the package for the contract being reassigned is unvetted on the target domain" in {
      val sourceDomainTopology =
        createTestingTopologySnapshot(
          Map(
            submittingParticipant -> Map(submitter -> Submission),
            participant1 -> Map(party1 -> Submission),
          ),
          packages = Seq(submittingParticipant, participant1)
            .map(_ -> Seq(templateId.packageId))
            .toMap, // The package is known on the source domain
        )

      val targetDomainTopology =
        createTestingTopologySnapshot(
          topology = Map(
            submittingParticipant -> Map(submitter -> Submission),
            participant1 -> Map(party1 -> Submission),
          ),
          packages = Map.empty, // The package is not known on the target domain
        )

      val result =
        mkUnassignmentResult(
          stakeholders = Set(submitter, adminSubmitter, admin1),
          sourceTopologySnapshot = sourceDomainTopology,
          targetTopologySnapshot = targetDomainTopology,
        )

      result.left.value shouldBe a[PackageIdUnknownOrUnvetted]
    }

    "fail if the package for the contract being reassigned is unvetted on one non-reassigning participant connected to the target domain" in {

      val sourceDomainTopology =
        createTestingIdentityFactory(
          topology = Map(
            submittingParticipant -> Map(submitter -> Submission),
            participant1 -> Map(party1 -> Submission),
          ),
          // On the source domain, the package is vetted on all participants
          packages =
            Seq(submittingParticipant, participant1).map(_ -> Seq(templateId.packageId)).toMap,
        ).topologySnapshot()

      val targetDomainTopology =
        createTestingIdentityFactory(
          topology = Map(
            submittingParticipant -> Map(submitter -> Submission),
            participant1 -> Map(party1 -> Submission),
          ),
          // On the target domain, the package is not vetted on `participant1`
          packages = Map(submittingParticipant -> Seq(templateId.packageId)),
        ).topologySnapshot()

      // `party1` is a stakeholder hosted on `participant1`, but it has not vetted `templateId.packageId` on the target domain
      val result =
        mkUnassignmentResult(
          stakeholders = Set(submitter, party1, adminSubmitter, admin1),
          sourceTopologySnapshot = sourceDomainTopology,
          targetTopologySnapshot = targetDomainTopology,
        )

      result.left.value shouldBe a[PackageIdUnknownOrUnvetted]

    }

    "pick the active confirming admin party" in {
      val ipsAdminNoConfirmation = createTestingTopologySnapshot(
        Map(
          submittingParticipant -> Map(adminSubmitter -> Submission, submitter -> Submission),
          participant1 -> Map(party1 -> Confirmation),
          participant2 -> Map(party1 -> Observation), // Not reassigning (cannot confirm)
        )
      )
      val result =
        mkUnassignmentResult(Set(submitter, party1), ipsAdminNoConfirmation, testingTopology)

      result.value shouldEqual
        UnassignmentRequestValidated(
          UnassignmentRequest(
            submitterMetadata = submitterMetadata(submitter),
            stakeholders = Set(submitter, party1),
            reassigningParticipants = Set(submittingParticipant, participant1),
            creatingTransactionId = creatingTransactionId,
            contract = contract,
            sourceDomain = sourceDomain,
            sourceProtocolVersion = SourceProtocolVersion(testedProtocolVersion),
            sourceMediator = sourceMediator,
            targetDomain = targetDomain,
            targetProtocolVersion = TargetProtocolVersion(testedProtocolVersion),
            targetTimeProof = timeEvent,
            reassignmentCounter = initialReassignmentCounter,
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
        ),
        packages = Seq(
          submittingParticipant,
          participant1,
          participant2,
          participant3,
          participant4,
        ).map(_ -> Seq(templateId.packageId)).toMap,
      )
      val ipsTarget = createTestingTopologySnapshot(
        Map(
          submittingParticipant -> Map(submitter -> Submission),
          participant1 -> Map(submitter -> Observation),
          participant3 -> Map(party1 -> Submission),
          participant4 -> Map(party1 -> Confirmation),
        ),
        packages = Seq(
          submittingParticipant,
          participant1,
          participant3,
          participant4,
        ).map(_ -> Seq(templateId.packageId)).toMap,
      )
      val stakeholders = Set(submitter, party1)
      val result = mkUnassignmentResult(stakeholders, ipsSource, ipsTarget)
      result.value shouldEqual
        UnassignmentRequestValidated(
          UnassignmentRequest(
            submitterMetadata = submitterMetadata(submitter),
            stakeholders = stakeholders,
            reassigningParticipants = Set(submittingParticipant, participant3, participant4),
            creatingTransactionId = creatingTransactionId,
            contract = contract,
            sourceDomain = sourceDomain,
            sourceProtocolVersion = SourceProtocolVersion(testedProtocolVersion),
            sourceMediator = sourceMediator,
            targetDomain = targetDomain,
            targetProtocolVersion = TargetProtocolVersion(testedProtocolVersion),
            targetTimeProof = timeEvent,
            reassignmentCounter = initialReassignmentCounter,
          ),
          Set(submittingParticipant, participant1, participant2, participant3, participant4),
        )
    }

    "allow admin parties as stakeholders" in {
      val stakeholders = Set(submitter, adminSubmitter, admin1)

      mkUnassignmentResult(stakeholders, testingTopology, testingTopology) shouldBe Right(
        UnassignmentRequestValidated(
          UnassignmentRequest(
            submitterMetadata = submitterMetadata(submitter),
            stakeholders = stakeholders,
            // Because admin1 is a stakeholder, participant1 is reassigning
            reassigningParticipants = Set(submittingParticipant, participant1),
            creatingTransactionId = creatingTransactionId,
            contract = contract,
            sourceDomain = sourceDomain,
            sourceProtocolVersion = SourceProtocolVersion(testedProtocolVersion),
            sourceMediator = sourceMediator,
            targetDomain = targetDomain,
            targetProtocolVersion = TargetProtocolVersion(testedProtocolVersion),
            targetTimeProof = timeEvent,
            reassignmentCounter = initialReassignmentCounter,
          ),
          Set(submittingParticipant, participant1),
        )
      )
    }
  }

  "prepare submission" should {
    "succeed without errors" in {
      val state = mkState
      val contract = ExampleTransactionFactory.asSerializable(
        contractId,
        contractInstance = ExampleTransactionFactory.contractInstance(templateId = templateId),
        metadata = ContractMetadata.tryCreate(
          signatories = Set(party1),
          stakeholders = Set(party1),
          maybeKeyWithMaintainersVersioned = None,
        ),
      )
      val transactionId = ExampleTransactionFactory.transactionId(1)
      val submissionParam =
        UnassignmentProcessingSteps.SubmissionParam(
          submitterMetadata = submitterMetadata(party1),
          contractId,
          targetDomain,
          TargetProtocolVersion(testedProtocolVersion),
        )

      for {
        _ <- state.contractStore.storeCreatedContract(
          RequestCounter(1),
          transactionId,
          contract,
        )
        _ <- persistentState.activeContractStore
          .markContractsCreated(
            Seq(contractId -> initialReassignmentCounter),
            TimeOfChange(RequestCounter(1), timeEvent.timestamp),
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
            .valueOrFailShutdown("prepare submission failed")
      } yield succeed
    }

    "check that the target domain is not equal to the source domain" in {
      val state = mkState
      val contract = ExampleTransactionFactory.asSerializable(
        contractId,
        contractInstance = ExampleTransactionFactory.contractInstance(),
      )
      val transactionId = ExampleTransactionFactory.transactionId(1)
      val submissionParam = UnassignmentProcessingSteps.SubmissionParam(
        submitterMetadata = submitterMetadata(party1),
        contractId,
        Target(sourceDomain.unwrap),
        TargetProtocolVersion(testedProtocolVersion),
      )

      for {
        _ <- state.contractStore.storeCreatedContract(
          RequestCounter(1),
          transactionId,
          contract,
        )
        submissionResult <- leftOrFailShutdown(
          unassignmentProcessingSteps.createSubmission(
            submissionParam,
            sourceMediator,
            state,
            cryptoSnapshot,
          )
        )("prepare submission succeeded unexpectedly")
      } yield {
        submissionResult shouldBe a[TargetDomainIsSourceDomain]
      }
    }
  }

  "receive request" should {
    val outRequest = UnassignmentRequest(
      submitterMetadata = submitterMetadata(party1),
      Set(party1),
      reassigningParticipants = Set(submittingParticipant),
      creatingTransactionId,
      contract,
      sourceDomain,
      SourceProtocolVersion(testedProtocolVersion),
      sourceMediator,
      targetDomain,
      TargetProtocolVersion(testedProtocolVersion),
      timeEvent,
      reassignmentCounter = initialReassignmentCounter,
    )
    val outTree = makeFullUnassignmentTree(outRequest)

    "succeed without errors" in {
      val sessionKeyStore =
        new SessionKeyStoreWithInMemoryCache(CachingConfigs.defaultSessionKeyCacheConfig)
      for {
        encryptedOutRequest <- encryptUnassignmentTree(
          outTree,
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
              mkParsedRequest(outTree, RecipientsTest.testInstance, None)
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
      val metadata = ContractMetadata.tryCreate(Set.empty, Set(party1), None)
      val contract = ExampleTransactionFactory.asSerializable(
        contractId,
        contractInstance = ExampleTransactionFactory.contractInstance(templateId = templateId),
        metadata = metadata,
      )
      val transactionId = ExampleTransactionFactory.transactionId(1)
      val outRequest = UnassignmentRequest(
        submitterMetadata = submitterMetadata(party1),
        Set(party1),
        reassigningParticipants = Set(submittingParticipant),
        creatingTransactionId,
        contract,
        sourceDomain,
        SourceProtocolVersion(testedProtocolVersion),
        sourceMediator,
        targetDomain,
        TargetProtocolVersion(testedProtocolVersion),
        timeEvent,
        reassignmentCounter = initialReassignmentCounter,
      )
      val fullUnassignmentTree = makeFullUnassignmentTree(outRequest)

      state.contractStore
        .storeCreatedContract(
          RequestCounter(1),
          transactionId,
          contract,
        )
        .futureValue

      unassignmentProcessingSteps
        .constructPendingDataAndResponse(
          mkParsedRequest(fullUnassignmentTree, Recipients.cc(submittingParticipant)),
          state.reassignmentCache,
          FutureUnlessShutdown.pure(mkActivenessResult()),
          engineController =
            EngineController(submittingParticipant, RequestId(CantonTimestamp.Epoch), loggerFactory),
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
    "prevent the contract being reassigned is not vetted on the target domain" in {
      val unassignmentProcessingStepsWithoutPackages = {
        val f = createCryptoFactory(packages = Seq.empty)
        val s = createCryptoSnapshot(f)
        val c = createReassignmentCoordination(s)
        createUnassignmentProcessingSteps(c)
      }

      constructPendingDataAndResponseWith(unassignmentProcessingStepsWithoutPackages).leftOrFail(
        "construction of pending data and response succeeded unexpectedly"
      ) shouldBe a[PackageIdUnknownOrUnvetted]
    }
  }

  "get commit set and contracts to be stored and event" should {
    "succeed without errors" in {
      val state = mkState
      val reassignmentId = ReassignmentId(sourceDomain, CantonTimestamp.Epoch)
      val rootHash = TestHash.dummyRootHash
      val reassignmentResult =
        ConfirmationResultMessage.create(
          sourceDomain.unwrap,
          UnassignmentViewType,
          RequestId(CantonTimestamp.Epoch),
          rootHash,
          Verdict.Approve(testedProtocolVersion),
          Set(),
          testedProtocolVersion,
        )

      val domainParameters = DynamicDomainParametersWithValidity(
        DynamicDomainParameters.defaultValues(testedProtocolVersion),
        CantonTimestamp.MinValue,
        None,
        targetDomain.unwrap,
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
            SequencerCounter(0),
            CantonTimestamp.Epoch,
            sourceDomain.unwrap,
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
        assignmentExclusivity = domainParameters
          .assignmentExclusivityLimitFor(timeEvent.timestamp)
          .value
        pendingOut = PendingUnassignment(
          RequestId(CantonTimestamp.Epoch),
          RequestCounter(1),
          SequencerCounter(1),
          rootHash,
          contractId,
          ReassignmentCounter.Genesis,
          templateId = templateId,
          packageName = packageName,
          isReassigningParticipant = false,
          submitterMetadata = submitterMetadata(submitter),
          reassignmentId,
          targetDomain,
          Set(party1),
          Set(party1),
          timeEvent,
          Some(assignmentExclusivity),
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
              pendingOut,
              state.pendingUnassignmentSubmissions,
              crypto.pureCrypto,
            )
            .failOnShutdown
        )("get commit set and contract to be stored and event")
      } yield succeed
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
          Seq((ViewHashAndRecipients(tree.viewHash, recipients), tree.informees.toList)),
          parallel = true,
          crypto.pureCrypto,
          cryptoSnapshot,
          sessionKeyStore,
          testedProtocolVersion,
        )
        .valueOrFailShutdown("cannot generate encryption key for transfer-out request")
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
      sourceDomain.unwrap,
      testedProtocolVersion,
      UnassignmentViewType,
      testTopologyTimestamp,
      SerializedRootHashMessagePayload.empty,
    )
}
