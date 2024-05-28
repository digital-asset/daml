// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.transfer

import cats.Eval
import cats.implicits.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{CachingConfigs, DefaultProcessingTimeouts}
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.crypto.{DomainSnapshotSyncCryptoApi, Signature, TestHash}
import com.digitalasset.canton.data.ViewType.TransferOutViewType
import com.digitalasset.canton.data.{
  CantonTimestamp,
  FullTransferOutTree,
  TransferSubmitterMetadata,
}
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
import com.digitalasset.canton.participant.protocol.transfer.TransferOutProcessingSteps.PendingTransferOut
import com.digitalasset.canton.participant.protocol.transfer.TransferOutProcessorError.*
import com.digitalasset.canton.participant.protocol.transfer.TransferProcessingSteps.{
  NoTransferSubmissionPermission,
  ParsedTransferRequest,
  TransferProcessorError,
}
import com.digitalasset.canton.participant.protocol.{EngineController, ProcessingStartingPoints}
import com.digitalasset.canton.participant.store.memory.*
import com.digitalasset.canton.participant.store.{MultiDomainEventLog, SyncDomainEphemeralState}
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.store.memory.InMemoryIndexedStringStore
import com.digitalasset.canton.store.{IndexedDomain, SessionKeyStore}
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
import com.digitalasset.canton.version.HasTestCloseContext
import com.digitalasset.canton.version.Transfer.{SourceProtocolVersion, TargetProtocolVersion}
import com.digitalasset.canton.{
  BaseTest,
  HasExecutorService,
  LedgerApplicationId,
  LedgerCommandId,
  LfPackageId,
  LfPackageName,
  LfPartyId,
  RequestCounter,
  SequencerCounter,
  TransferCounter,
}
import org.scalatest.wordspec.AsyncWordSpec

import java.util.UUID
import scala.annotation.nowarn
import scala.concurrent.{ExecutionContext, Future}

@nowarn("msg=match may not be exhaustive")
final class TransferOutProcessingStepsTest
    extends AsyncWordSpec
    with BaseTest
    with HasExecutorService
    with HasTestCloseContext {

  private implicit val ec: ExecutionContext = executorService

  private val testTopologyTimestamp = CantonTimestamp.Epoch

  private lazy val sourceDomain = SourceDomainId(
    DomainId(UniqueIdentifier.tryFromProtoPrimitive("source::domain"))
  )
  private lazy val sourceMediator = MediatorGroupRecipient(MediatorGroupIndex.tryCreate(100))
  private lazy val targetDomain = TargetDomainId(
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
    LfTemplateId.assertFromString("transferoutprocessingstepstestpackage:template:id")
  private lazy val packageName =
    LfPackageName.assertFromString("transferoutprocessingstepstestpackagename")

  private lazy val initialTransferCounter: TransferCounter = TransferCounter.Genesis

  private def submitterMetadata(submitter: LfPartyId): TransferSubmitterMetadata = {
    TransferSubmitterMetadata(
      submitter,
      submittingParticipant,
      LedgerCommandId.assertFromString("transfer-out-processing-steps-command-id"),
      submissionId = None,
      LedgerApplicationId.assertFromString("tests"),
      workflowId = None,
    )
  }

  private lazy val adminSubmitter: LfPartyId = submittingParticipant.adminParty.toLf

  private lazy val crypto =
    SymbolicCrypto.create(testedReleaseProtocolVersion, timeouts, loggerFactory)

  private lazy val multiDomainEventLog = mock[MultiDomainEventLog]
  private lazy val clock = new WallClock(timeouts, loggerFactory)
  private lazy val indexedStringStore = new InMemoryIndexedStringStore(minIndex = 1, maxIndex = 1)
  private lazy val persistentState =
    new InMemorySyncDomainPersistentState(
      submittingParticipant,
      clock,
      crypto,
      IndexedDomain.tryCreate(sourceDomain.unwrap, 1),
      testedProtocolVersion,
      enableAdditionalConsistencyChecks = true,
      indexedStringStore = indexedStringStore,
      loggerFactory,
      timeouts,
      futureSupervisor,
    )

  private def mkState: SyncDomainEphemeralState =
    new SyncDomainEphemeralState(
      submittingParticipant,
      persistentState,
      Eval.now(multiDomainEventLog),
      mock[InFlightSubmissionTracker],
      ProcessingStartingPoints.default,
      () => mock[DomainTimeTracker],
      ParticipantTestMetrics.domain,
      CachingConfigs.defaultSessionKeyCacheConfig,
      DefaultProcessingTimeouts.testing,
      loggerFactory,
      FutureSupervisor.Noop,
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

  private def createTransferCoordination(
      cryptoSnapshot: DomainSnapshotSyncCryptoApi = cryptoSnapshot
  ) =
    TestTransferCoordination(
      Set(TargetDomainId(sourceDomain.unwrap), targetDomain),
      CantonTimestamp.Epoch,
      Some(cryptoSnapshot),
      Some(None),
      loggerFactory,
      Seq(templateId.packageId),
    )(directExecutionContext)

  private lazy val coordination: TransferCoordination =
    createTransferCoordination()

  private def createOutProcessingSteps(transferCoordination: TransferCoordination = coordination) =
    new TransferOutProcessingSteps(
      sourceDomain,
      submittingParticipant,
      damle,
      transferCoordination,
      seedGenerator,
      defaultStaticDomainParameters,
      SourceProtocolVersion(testedProtocolVersion),
      loggerFactory,
    )(executorService)

  private lazy val outProcessingSteps: TransferOutProcessingSteps = createOutProcessingSteps()

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
      view: FullTransferOutTree,
      recipients: Recipients = RecipientsTest.testInstance,
      signatureO: Option[Signature] = None,
  ): ParsedTransferRequest[FullTransferOutTree] = ParsedTransferRequest(
    RequestCounter(1),
    CantonTimestamp.Epoch,
    SequencerCounter(1),
    view,
    recipients,
    signatureO,
    None,
    isFreshOwnTimelyRequest = true,
    transferringParticipant = true,
    Seq.empty,
    sourceMediator,
    cryptoSnapshot,
    cryptoSnapshot.ipsSnapshot.findDynamicDomainParameters().futureValue.value,
  )

  "TransferOutRequest.validated" should {
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

    def mkTxOutRes(
        stakeholders: Set[LfPartyId],
        sourceTopologySnapshot: TopologySnapshot,
        targetTopologySnapshot: TopologySnapshot,
    ): Either[TransferProcessorError, TransferOutRequestValidated] =
      TransferOutRequest
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
          sourceTopologySnapshot,
          targetTopologySnapshot,
          initialTransferCounter,
          logger,
        )
        .value
        .failOnShutdown
        .futureValue

    "fail if submitter is not a stakeholder" in {
      val stakeholders = Set(party1, party2)
      val result = mkTxOutRes(stakeholders, testingTopology, testingTopology)
      result.left.value shouldBe a[SubmittingPartyMustBeStakeholderOut]
    }

    "fail if submitting participant does not have submission permission" in {
      val ipsNoSubmissionPermission =
        createTestingTopologySnapshot(Map(submittingParticipant -> Map(submitter -> Confirmation)))

      val result = mkTxOutRes(Set(submitter), ipsNoSubmissionPermission, testingTopology)
      result.left.value shouldBe a[NoTransferSubmissionPermission]
    }

    "fail if a stakeholder cannot submit on target domain" in {
      val ipsNoSubmissionOnTarget = createTestingTopologySnapshot(
        Map(
          submittingParticipant -> Map(submitter -> Submission),
          participant1 -> Map(party1 -> Confirmation),
        )
      )

      val stakeholders = Set(submitter, party1)
      val result = mkTxOutRes(stakeholders, testingTopology, ipsNoSubmissionOnTarget)
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
      val result = mkTxOutRes(stakeholders, ipsConfirmationOnSource, ipsNoConfirmationOnTarget)
      result.left.value shouldBe a[PermissionErrors]
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
      val result = mkTxOutRes(stakeholders, testingTopology, ipsDifferentParticipant)
      result.left.value shouldBe a[PermissionErrors]
    }

    "fail if participant cannot confirm for admin party" in {
      val ipsAdminNoConfirmation = createTestingTopologySnapshot(
        Map(
          submittingParticipant -> Map(adminSubmitter -> Submission, submitter -> Submission),
          participant1 -> Map(party1 -> Observation),
        )
      )
      val result =
        loggerFactory.suppressWarningsAndErrors(
          mkTxOutRes(Set(submitter, party1), ipsAdminNoConfirmation, testingTopology)
        )
      result.left.value shouldBe a[PermissionErrors]
    }

    // TODO(i13201) This should ideally be covered in integration tests as well
    "fail if the package for the contract being transferred is unvetted on the target domain" in {
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
        mkTxOutRes(
          stakeholders = Set(submitter, adminSubmitter, admin1),
          sourceTopologySnapshot = sourceDomainTopology,
          targetTopologySnapshot = targetDomainTopology,
        )

      result.left.value shouldBe a[PackageIdUnknownOrUnvetted]
    }

    "fail if the package for the contract being transferred is unvetted on one non-transferring participant connected to the target domain" in {

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
        mkTxOutRes(
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
          participant2 -> Map(party1 -> Observation),
        )
      )
      val result =
        loggerFactory.suppressWarningsAndErrors(
          mkTxOutRes(Set(submitter, party1), ipsAdminNoConfirmation, testingTopology)
        )
      result.value shouldEqual
        TransferOutRequestValidated(
          TransferOutRequest(
            submitterMetadata = submitterMetadata(submitter),
            stakeholders = Set(submitter, party1),
            adminParties = Set(adminSubmitter, admin1),
            creatingTransactionId = creatingTransactionId,
            contract = contract,
            sourceDomain = sourceDomain,
            sourceProtocolVersion = SourceProtocolVersion(testedProtocolVersion),
            sourceMediator = sourceMediator,
            targetDomain = targetDomain,
            targetProtocolVersion = TargetProtocolVersion(testedProtocolVersion),
            targetTimeProof = timeEvent,
            transferCounter = initialTransferCounter,
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
      val result = mkTxOutRes(stakeholders, ipsSource, ipsTarget)
      result.value shouldEqual
        TransferOutRequestValidated(
          TransferOutRequest(
            submitterMetadata = submitterMetadata(submitter),
            stakeholders = stakeholders,
            adminParties = Set(adminSubmitter, admin3, admin4),
            creatingTransactionId = creatingTransactionId,
            contract = contract,
            sourceDomain = sourceDomain,
            sourceProtocolVersion = SourceProtocolVersion(testedProtocolVersion),
            sourceMediator = sourceMediator,
            targetDomain = targetDomain,
            targetProtocolVersion = TargetProtocolVersion(testedProtocolVersion),
            targetTimeProof = timeEvent,
            transferCounter = initialTransferCounter,
          ),
          Set(submittingParticipant, participant1, participant2, participant3, participant4),
        )
    }

    "allow admin parties as stakeholders" in {
      val stakeholders = Set(submitter, adminSubmitter, admin1)
      mkTxOutRes(stakeholders, testingTopology, testingTopology) shouldBe Right(
        TransferOutRequestValidated(
          TransferOutRequest(
            submitterMetadata = submitterMetadata(submitter),
            stakeholders = stakeholders,
            adminParties = Set(adminSubmitter, admin1),
            creatingTransactionId = creatingTransactionId,
            contract = contract,
            sourceDomain = sourceDomain,
            sourceProtocolVersion = SourceProtocolVersion(testedProtocolVersion),
            sourceMediator = sourceMediator,
            targetDomain = targetDomain,
            targetProtocolVersion = TargetProtocolVersion(testedProtocolVersion),
            targetTimeProof = timeEvent,
            transferCounter = initialTransferCounter,
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
        TransferOutProcessingSteps.SubmissionParam(
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
            Seq(contractId -> initialTransferCounter),
            TimeOfChange(RequestCounter(1), timeEvent.timestamp),
          )
          .value
        _ <-
          outProcessingSteps
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
      val submissionParam = TransferOutProcessingSteps.SubmissionParam(
        submitterMetadata = submitterMetadata(party1),
        contractId,
        TargetDomainId(sourceDomain.unwrap),
        TargetProtocolVersion(testedProtocolVersion),
      )

      for {
        _ <- state.contractStore.storeCreatedContract(
          RequestCounter(1),
          transactionId,
          contract,
        )
        submissionResult <- leftOrFailShutdown(
          outProcessingSteps.createSubmission(
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
    val outRequest = TransferOutRequest(
      submitterMetadata = submitterMetadata(party1),
      Set(party1),
      Set(party1),
      creatingTransactionId,
      contract,
      sourceDomain,
      SourceProtocolVersion(testedProtocolVersion),
      sourceMediator,
      targetDomain,
      TargetProtocolVersion(testedProtocolVersion),
      timeEvent,
      transferCounter = initialTransferCounter,
    )
    val outTree = makeFullTransferOutTree(outRequest)

    "succeed without errors" in {
      val sessionKeyStore = SessionKeyStore(CachingConfigs.defaultSessionKeyCacheConfig)
      for {
        encryptedOutRequest <- encryptTransferOutTree(outTree, sessionKeyStore)
        envelopes =
          NonEmpty(
            Seq,
            OpenEnvelope(encryptedOutRequest, RecipientsTest.testInstance)(testedProtocolVersion),
          )
        decrypted <-
          outProcessingSteps
            .decryptViews(envelopes, cryptoSnapshot, sessionKeyStore)
            .valueOrFailShutdown(
              "decrypt request failed"
            )
        activenessSet =
          outProcessingSteps
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
        transferOutProcessingSteps: TransferOutProcessingSteps
    ) = {
      val state = mkState
      val metadata = ContractMetadata.tryCreate(Set.empty, Set(party1), None)
      val contract = ExampleTransactionFactory.asSerializable(
        contractId,
        contractInstance = ExampleTransactionFactory.contractInstance(templateId = templateId),
        metadata = metadata,
      )
      val transactionId = ExampleTransactionFactory.transactionId(1)
      val outRequest = TransferOutRequest(
        submitterMetadata = submitterMetadata(party1),
        Set(party1),
        Set(submittingParticipant.adminParty.toLf),
        creatingTransactionId,
        contract,
        sourceDomain,
        SourceProtocolVersion(testedProtocolVersion),
        sourceMediator,
        targetDomain,
        TargetProtocolVersion(testedProtocolVersion),
        timeEvent,
        transferCounter = initialTransferCounter,
      )
      val fullTransferOutTree = makeFullTransferOutTree(outRequest)

      state.contractStore
        .storeCreatedContract(
          RequestCounter(1),
          transactionId,
          contract,
        )
        .futureValue

      transferOutProcessingSteps
        .constructPendingDataAndResponse(
          mkParsedRequest(fullTransferOutTree, Recipients.cc(submittingParticipant)),
          state.transferCache,
          FutureUnlessShutdown.pure(mkActivenessResult()),
          engineController =
            EngineController(submittingParticipant, RequestId(CantonTimestamp.Epoch), loggerFactory),
        )
        .value
        .onShutdown(fail("unexpected shutdown during a test"))
        .futureValue
    }

    "succeed without errors" in {
      constructPendingDataAndResponseWith(outProcessingSteps).valueOrFail(
        "construction of pending data and response failed"
      )
      succeed
    }

    // TODO(i13201) This should ideally be covered in integration tests as well
    "prevent the contract being transferred is not vetted on the target domain since version 5" in {
      val outProcessingStepsWithoutPackages = {
        val f = createCryptoFactory(packages = Seq.empty)
        val s = createCryptoSnapshot(f)
        val c = createTransferCoordination(s)
        createOutProcessingSteps(c)
      }

      constructPendingDataAndResponseWith(outProcessingStepsWithoutPackages).leftOrFail(
        "construction of pending data and response succeeded unexpectedly"
      ) shouldBe a[PackageIdUnknownOrUnvetted]
    }
  }

  "get commit set and contracts to be stored and event" should {
    "succeed without errors" in {
      val state = mkState
      val contractHash = ExampleTransactionFactory.lfHash(0)
      val transferId = TransferId(sourceDomain, CantonTimestamp.Epoch)
      val rootHash = TestHash.dummyRootHash
      val transferResult =
        ConfirmationResultMessage.create(
          sourceDomain.id,
          TransferOutViewType,
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
            transferResult,
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
          )
        }
        signedContent = SignedContent(
          deliver,
          SymbolicCrypto.emptySignature,
          None,
          testedProtocolVersion,
        )
        transferInExclusivity = domainParameters
          .transferExclusivityLimitFor(timeEvent.timestamp)
          .value
        pendingOut = PendingTransferOut(
          RequestId(CantonTimestamp.Epoch),
          RequestCounter(1),
          SequencerCounter(1),
          rootHash,
          WithContractHash(contractId, contractHash),
          TransferCounter.Genesis,
          templateId = templateId,
          packageName = packageName,
          transferringParticipant = false,
          submitterMetadata = submitterMetadata(submitter),
          transferId,
          targetDomain,
          Set(party1),
          Set(party1),
          timeEvent,
          Some(transferInExclusivity),
          MediatorGroupRecipient(MediatorGroupIndex.one),
          locallyRejectedF = FutureUnlessShutdown.pure(false),
          abortEngine = _ => (),
          engineAbortStatusF = FutureUnlessShutdown.pure(EngineAbortStatus.notAborted),
        )
        _ <- valueOrFail(
          outProcessingSteps
            .getCommitSetAndContractsToBeStoredAndEvent(
              NoOpeningErrors(signedContent),
              transferResult.verdict,
              pendingOut,
              state.pendingTransferOutSubmissions,
              crypto.pureCrypto,
            )
            .failOnShutdown
        )("get commit set and contract to be stored and event")
      } yield succeed
    }
  }

  def makeFullTransferOutTree(
      request: TransferOutRequest,
      uuid: UUID = new UUID(6L, 7L),
  ): FullTransferOutTree = {
    val seed = seedGenerator.generateSaltSeed()
    request.toFullTransferOutTree(crypto.pureCrypto, crypto.pureCrypto, seed, uuid)
  }

  def encryptTransferOutTree(
      tree: FullTransferOutTree,
      sessionKeyStore: SessionKeyStore,
  ): Future[EncryptedViewMessage[TransferOutViewType]] =
    EncryptedViewMessageFactory
      .create(TransferOutViewType)(tree, cryptoSnapshot, sessionKeyStore, testedProtocolVersion)(
        implicitly[TraceContext],
        executorService,
      )
      .valueOrFailShutdown("failed to encrypt transfer-out request")

  def makeRootHashMessage(
      request: FullTransferOutTree
  ): RootHashMessage[SerializedRootHashMessagePayload] =
    RootHashMessage(
      request.rootHash,
      sourceDomain.unwrap,
      testedProtocolVersion,
      TransferOutViewType,
      testTopologyTimestamp,
      SerializedRootHashMessagePayload.empty,
    )
}
