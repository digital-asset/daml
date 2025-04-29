// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.Eval
import cats.data.EitherT
import cats.implicits.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{CachingConfigs, DefaultProcessingTimeouts}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.provider.symbolic.{SymbolicCrypto, SymbolicPureCrypto}
import com.digitalasset.canton.data.*
import com.digitalasset.canton.data.ViewType.AssignmentViewType
import com.digitalasset.canton.lifecycle.{DefaultPromiseUnlessShutdownFactory, FutureUnlessShutdown}
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.participant.admin.PackageDependencyResolver
import com.digitalasset.canton.participant.event.RecordOrderPublisher
import com.digitalasset.canton.participant.ledger.api.{LedgerApiIndexer, LedgerApiStore}
import com.digitalasset.canton.participant.metrics.ParticipantTestMetrics
import com.digitalasset.canton.participant.protocol.EngineController.EngineAbortStatus
import com.digitalasset.canton.participant.protocol.conflictdetection.ConflictDetectionHelpers.{
  mkActivenessResult,
  mkActivenessSet,
}
import com.digitalasset.canton.participant.protocol.reassignment.AssignmentProcessingSteps.*
import com.digitalasset.canton.participant.protocol.reassignment.AssignmentValidation.*
import com.digitalasset.canton.participant.protocol.reassignment.AssignmentValidationError.ContractDataMismatch
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.*
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentValidationError.{
  ContractIdAuthenticationFailure,
  NotHostedOnParticipant,
  StakeholdersMismatch,
  SubmitterMustBeStakeholder,
}
import com.digitalasset.canton.participant.protocol.submission.EncryptedViewMessageFactory.{
  ViewHashAndRecipients,
  ViewKeyData,
}
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
import com.digitalasset.canton.participant.store.ReassignmentStoreTest.{coidAbs1, reassignment10}
import com.digitalasset.canton.participant.store.memory.*
import com.digitalasset.canton.participant.store.{
  AcsCounterParticipantConfigStore,
  ContractStore,
  ReassignmentStoreTest,
  SyncPersistentState,
}
import com.digitalasset.canton.participant.sync.SyncEphemeralState
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceAlarm
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.ExampleTransactionFactory.{pureCrypto, submitter}
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.store.memory.InMemoryIndexedStringStore
import com.digitalasset.canton.store.{
  ConfirmationRequestSessionKeyStore,
  IndexedSynchronizer,
  SessionKeyStoreWithInMemoryCache,
}
import com.digitalasset.canton.time.{SynchronizerTimeTracker, WallClock}
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
    with HasExecutionContext
    with FailOnShutdown {
  private lazy val sourceSynchronizer = Source(
    SynchronizerId(UniqueIdentifier.tryFromProtoPrimitive("synchronizer::source"))
  )
  private lazy val sourceMediator = MediatorGroupRecipient(MediatorGroupIndex.tryCreate(0))
  private lazy val targetSynchronizer = Target(
    SynchronizerId(UniqueIdentifier.tryFromProtoPrimitive("synchronizer::target"))
  )
  private lazy val targetMediator = MediatorGroupRecipient(MediatorGroupIndex.tryCreate(0))
  private lazy val anotherSynchronizer = SynchronizerId(
    UniqueIdentifier.tryFromProtoPrimitive("synchronizer::another")
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
    UniqueIdentifier.tryFromProtoPrimitive("bothsynchronizers::participant")
  )

  private def testMetadata(
      signatories: Set[LfPartyId] = Set(party1),
      stakeholders: Set[LfPartyId] = Set(party1),
      maybeKeyWithMaintainersVersioned: Option[LfVersioned[LfGlobalKeyWithMaintainers]] = None,
  ): ContractMetadata =
    ContractMetadata.tryCreate(
      stakeholders = stakeholders,
      signatories = signatories,
      maybeKeyWithMaintainersVersioned = maybeKeyWithMaintainersVersioned,
    )

  private lazy val contract = ExampleTransactionFactory.authenticatedSerializableContract(
    metadata = testMetadata()
  )

  private lazy val initialReassignmentCounter: ReassignmentCounter = ReassignmentCounter.One

  private def submitterInfo(submitter: LfPartyId): ReassignmentSubmitterMetadata =
    ReassignmentSubmitterMetadata(
      submitter,
      participant,
      LedgerCommandId.assertFromString("assignment-processing-steps-command-id"),
      submissionId = None,
      LedgerUserId.assertFromString("tests"),
      workflowId = None,
    )

  private lazy val clock = new WallClock(timeouts, loggerFactory)
  private lazy val crypto =
    SymbolicCrypto.create(testedReleaseProtocolVersion, timeouts, loggerFactory)

  private lazy val seedGenerator = new SeedGenerator(crypto.pureCrypto)

  private lazy val identityFactory = TestingTopology()
    .withSynchronizers(sourceSynchronizer.unwrap)
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
      .forOwnerAndSynchronizer(participant, sourceSynchronizer.unwrap)
      .currentSnapshotApproximation

  private lazy val assignmentProcessingSteps =
    testInstance(targetSynchronizer, cryptoSnapshot, None)

  private lazy val indexedStringStore = new InMemoryIndexedStringStore(minIndex = 1, maxIndex = 1)

  private def statefulDependencies: Future[(SyncPersistentState, SyncEphemeralState)] = {
    val ledgerApiIndexer = mock[LedgerApiIndexer]
    val contractStore = mock[ContractStore]
    val persistentState =
      new InMemorySyncPersistentState(
        participant,
        clock,
        crypto,
        IndexedSynchronizer.tryCreate(targetSynchronizer.unwrap, 1),
        defaultStaticSynchronizerParameters,
        enableAdditionalConsistencyChecks = true,
        indexedStringStore = indexedStringStore,
        contractStore = contractStore,
        acsCounterParticipantConfigStore = mock[AcsCounterParticipantConfigStore],
        packageDependencyResolver = mock[PackageDependencyResolver],
        ledgerApiStore = Eval.now(mock[LedgerApiStore]),
        loggerFactory = loggerFactory,
        exitOnFatalFailures = true,
        timeouts = timeouts,
        futureSupervisor = futureSupervisor,
      )

    (for {
      _ <- persistentState.parameterStore.setParameters(defaultStaticSynchronizerParameters)
    } yield {
      val state = new SyncEphemeralState(
        participant,
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
        loggerFactory = loggerFactory,
        FutureSupervisor.Noop,
        clock,
      )
      (persistentState, state)
    }).failOnShutdown
  }

  private lazy val reassignmentId = ReassignmentId(sourceSynchronizer, CantonTimestamp.Epoch)

  private lazy val reassignmentDataHelpers = ReassignmentDataHelpers(
    contract,
    reassignmentId.sourceSynchronizer,
    targetSynchronizer,
    identityFactory,
  )

  private lazy val unassignmentRequest = reassignmentDataHelpers.unassignmentRequest(
    party1,
    participant,
    sourceMediator,
  )()

  private lazy val reassignmentData =
    reassignmentDataHelpers.unassignmentData(reassignmentId, unassignmentRequest)

  private def mkParsedRequest(
      view: FullAssignmentTree,
      recipients: Recipients = RecipientsTest.testInstance,
  ): ParsedReassignmentRequest[FullAssignmentTree] = {
    val signature = cryptoSnapshot
      .sign(view.rootHash.unwrap, SigningKeyUsage.ProtocolOnly)
      .futureValueUS
      .value

    ParsedReassignmentRequest(
      RequestCounter(1),
      CantonTimestamp.Epoch,
      SequencerCounter(1),
      view,
      recipients,
      Some(signature),
      None,
      isFreshOwnTimelyRequest = true,
      Seq.empty,
      targetMediator,
      cryptoSnapshot,
      cryptoSnapshot.ipsSnapshot.findDynamicSynchronizerParameters().futureValueUS.value,
    )
  }

  "prepare submission" should {
    def setUpOrFail(
        reassignmentData: UnassignmentData,
        persistentState: SyncPersistentState,
    ): FutureUnlessShutdown[Unit] =
      for {
        _ <- valueOrFail(persistentState.reassignmentStore.addUnassignmentData(reassignmentData))(
          "add reassignment data failed"
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
        _ <- setUpOrFail(reassignmentData, persistentState).failOnShutdown
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

    "fail when a receiving party has no participant on the synchronizer" in {
      // metadataTransformer updates the contract metadata to inject receiving parties
      def test(metadataTransformer: ContractMetadata => ContractMetadata) = {
        val helpers = reassignmentDataHelpers
          .focus(_.contract.metadata)
          .modify(metadataTransformer)

        val unassignmentRequest = helpers.unassignmentRequest(
          party1,
          DefaultTestIdentities.participant1,
          sourceMediator,
        )()

        val reassignmentData2 =
          reassignmentDataHelpers.unassignmentData(reassignmentId, unassignmentRequest)

        for {
          deps <- statefulDependencies
          (persistentState, state) = deps
          _ <- setUpOrFail(reassignmentData2, persistentState).failOnShutdown
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

      for {
        _ <- test(metadata =>
          ContractMetadata.tryCreate(
            signatories = metadata.signatories,
            // party3 is a stakeholder and therefore a receiving party
            stakeholders = metadata.stakeholders + party3,
            maybeKeyWithMaintainersVersioned = None,
          )
        )

        _ <- test(metadata =>
          ContractMetadata.tryCreate(
            // party3 is a signatory and therefore a receiving party
            signatories = metadata.signatories + party3,
            stakeholders = metadata.stakeholders + party3,
            maybeKeyWithMaintainersVersioned = None,
          )
        )
      } yield succeed
    }

    "fail when submitting party is not a stakeholder" in {
      val submissionParam2 = SubmissionParam(
        submitterInfo(party2),
        reassignmentId,
      )

      for {
        deps <- statefulDependencies
        (persistentState, state) = deps
        _ <- setUpOrFail(reassignmentData, persistentState).failOnShutdown
        preparedSubmission <- leftOrFailShutdown(
          assignmentProcessingSteps.createSubmission(
            submissionParam2,
            targetMediator,
            state,
            cryptoSnapshot,
          )
        )("prepare submission did not return a left")
      } yield {
        preparedSubmission shouldBe SubmissionValidationError(
          s"Submission failed because: ${SubmitterMustBeStakeholder(ReassignmentRef(reassignmentId), party2, Set(party1)).message}"
        )
      }
    }

    "fail when submitting party not hosted on the participant" in {
      val submissionParam2 = SubmissionParam(
        submitterInfo(party3),
        reassignmentId,
      )

      // We need to change the contract instance otherwise we get another error (AssignmentSubmitterMustBeStakeholder)
      val contract = ExampleTransactionFactory.asSerializable(
        contractId = coidAbs1,
        contractInstance = ExampleTransactionFactory.contractInstance(),
        ledgerTime = CantonTimestamp.Epoch,
        metadata = ContractMetadata.tryCreate(Set(), Set(party3), None),
      )

      val reassignmentData2 = ReassignmentStoreTest.mkUnassignmentDataForSynchronizer(
        reassignmentId,
        sourceMediator,
        party3,
        targetSynchronizer,
        contract,
      )

      for {
        deps <- statefulDependencies
        (persistentState, ephemeralState) = deps
        _ <- setUpOrFail(reassignmentData2, persistentState).failOnShutdown
        preparedSubmission <- leftOrFailShutdown(
          assignmentProcessingSteps.createSubmission(
            submissionParam2,
            targetMediator,
            ephemeralState,
            cryptoSnapshot,
          )
        )("prepare submission did not return a left")
      } yield {
        preparedSubmission shouldBe SubmissionValidationError(
          s"Submission failed because: ${NotHostedOnParticipant(ReassignmentRef(reassignmentId), party3, participant).message}"
        )
      }
    }
  }

  "receive request" should {
    val assignmentTree = makeFullAssignmentTree()

    "succeed without errors" in {
      val sessionKeyStore =
        new SessionKeyStoreWithInMemoryCache(CachingConfigs.defaultSessionEncryptionKeyCacheConfig)
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
              ).copy(signatureO = signature)
            )
            .value
      } yield {
        decrypted.decryptionErrors shouldBe Seq.empty
        activenessSet shouldBe mkActivenessSet(assign = Set(contract.contractId))
      }
    }

    "fail when target synchronizer is not current synchronizer" in {
      val assignmentTree2 = makeFullAssignmentTree(
        targetSynchronizer = Target(anotherSynchronizer),
        targetMediator = anotherMediator,
      )
      val error =
        assignmentProcessingSteps.computeActivenessSet(mkParsedRequest(assignmentTree2)).left.value

      inside(error) { case UnexpectedSynchronizer(_, targetD, currentD) =>
        assert(targetD == anotherSynchronizer)
        assert(currentD == targetSynchronizer.unwrap)
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
              parsedRequest.synchronizerParameters,
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
    // Model conformance errors emits alarms.
    val modelConformanceError = LogEntry.assertLogSeq(
      Seq(
        (
          _.shouldBeCantonErrorCode(LocalRejectError.MalformedRejects.ModelConformance),
          "model conformance error",
        )
      )
    )
    "succeed without errors" in {
      for {
        deps <- statefulDependencies
        (persistentState, ephemeralState) = deps

        _ <- valueOrFail(
          persistentState.reassignmentStore.addUnassignmentData(reassignmentData)
        )(
          "add reassignment data failed"
        ).failOnShutdown

        fullAssignmentTree = makeFullAssignmentTree(
          reassignmentData.reassignmentId,
          reassigningParticipants = Set(participant),
        )
        result <- valueOrFail(
          assignmentProcessingSteps
            .constructPendingDataAndResponse(
              mkParsedRequest(fullAssignmentTree),
              ephemeralState.reassignmentCache,
              FutureUnlessShutdown.pure(mkActivenessResult()),
              engineController =
                EngineController(participant, RequestId(CantonTimestamp.Epoch), loggerFactory),
            )
        )("construction of pending data and response failed").failOnShutdown
      } yield {
        result.confirmationResponsesF.futureValueUS.value
          .valueOrFail("no response")
          ._1
          .responses should matchPattern { case Seq(ConfirmationResponse(_, LocalApprove(), _)) =>
        }
        result.pendingData.assignmentValidationResult.isSuccessfulF.futureValueUS shouldBe true
        succeed
      }
    }

    "fail when wrong metadata is given" in {
      def test(testContract: SerializableContract) =
        for {
          deps <- statefulDependencies
          (persistentState, ephemeralState) = deps

          _ <- valueOrFail(persistentState.reassignmentStore.addUnassignmentData(reassignmentData))(
            "add reassignment data failed"
          ).failOnShutdown

          fullAssignmentTree = makeFullAssignmentTree(
            reassignmentData.reassignmentId,
            party1,
            testContract,
            targetSynchronizer,
            targetMediator,
            reassigningParticipants = Set(participant),
          )

          result <-
            assignmentProcessingSteps
              .constructPendingDataAndResponse(
                mkParsedRequest(fullAssignmentTree),
                ephemeralState.reassignmentCache,
                FutureUnlessShutdown.pure(mkActivenessResult()),
                engineController =
                  EngineController(participant, RequestId(CantonTimestamp.Epoch), loggerFactory),
              )
              .failOnShutdown
          confirmationResponse <- result.confirmationResponsesF.failOnShutdown

        } yield {
          confirmationResponse.valueOrFail("no response")._1.responses should matchPattern {
            case Seq(ConfirmationResponse(_, LocalReject(_, true), _)) =>
          }
          val assignmentValidationResult = result.pendingData.assignmentValidationResult
          val modelConformanceError =
            assignmentValidationResult.metadataResultET.value.futureValueUS

          modelConformanceError.left.value match {
            case ContractIdAuthenticationFailure(ref, reason, contractId) =>
              ref shouldBe fullAssignmentTree.reassignmentRef
              contractId shouldBe testContract.contractId
              reason should startWith("Mismatching contract id suffixes")
            case other => fail(s"Did not expect $other")
          }

          assignmentValidationResult.validationErrors shouldBe Seq(
            ContractDataMismatch(reassignmentId)
          )
        }

      val baseMetadata = testMetadata()

      // party2 is incorrectly registered as a stakeholder
      val contractWrongStakeholders: SerializableContract =
        ExampleTransactionFactory
          .authenticatedSerializableContract(
            metadata = baseMetadata
          )
          .focus(_.metadata)
          .replace(
            testMetadata(stakeholders = baseMetadata.stakeholders + party2)
          )

      // party2 is incorrectly registered as a signatory
      val contractWrongSignatories: SerializableContract =
        ExampleTransactionFactory
          .authenticatedSerializableContract(
            metadata = testMetadata(stakeholders = baseMetadata.stakeholders + party2)
          )
          .focus(_.metadata)
          .replace(
            testMetadata(
              stakeholders = baseMetadata.stakeholders + party2,
              signatories = baseMetadata.signatories + party2,
            )
          )

      val incorrectKey = ExampleTransactionFactory.globalKeyWithMaintainers(
        ExampleTransactionFactory.defaultGlobalKey,
        Set(party1),
      )

      // Metadata has incorrect key
      val contractWrongKey: SerializableContract =
        ExampleTransactionFactory
          .authenticatedSerializableContract(
            metadata = testMetadata(stakeholders = baseMetadata.stakeholders + party2)
          )
          .focus(_.metadata)
          .replace(
            testMetadata(
              maybeKeyWithMaintainersVersioned = Some(incorrectKey)
            )
          )

      for {
        _ <- loggerFactory.assertLoggedWarningsAndErrorsSeq(
          test(contractWrongStakeholders),
          modelConformanceError,
        )
        _ <- loggerFactory.assertLoggedWarningsAndErrorsSeq(
          test(contractWrongSignatories),
          modelConformanceError,
        )

        _ <- loggerFactory.assertLoggedWarningsAndErrorsSeq(
          test(contractWrongKey),
          modelConformanceError,
        )
      } yield succeed
    }

    "fail when inconsistent stakeholders are given" in {
      /*
      We construct in this test an inconsistent `inconsistentTree: FullAssignmentTree` :
      - inconsistentTree.tree.commonData.stakeholders is incorrect
      - inconsistentTree.view.contract.metadata is correct
       */

      val incorrectMetadata = ContractMetadata.tryCreate(Set(party1), Set(party1, party2), None)
      val incorrectStakeholders = Stakeholders(incorrectMetadata)

      val expectedMetadata = contract.metadata
      val expectedStakeholders = Stakeholders(expectedMetadata)

      val expectedError = StakeholdersMismatch(
        reassignmentRef = ReassignmentRef(reassignmentId),
        declaredViewStakeholders = incorrectStakeholders,
        expectedStakeholders = expectedStakeholders,
      )

      val correctViewTree = makeFullAssignmentTree(reassignmentId)
      val incorrectViewTree = makeFullAssignmentTree(
        reassignmentId,
        contract = contract.copy(metadata = incorrectMetadata),
        reassigningParticipants = Set(participant),
      )

      val inconsistentTree = FullAssignmentTree(
        AssignmentViewTree(
          commonData = incorrectViewTree.tree.commonData,
          view = correctViewTree.tree.view,
          Target(testedProtocolVersion),
          pureCrypto,
        )
      )

      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        (for {
          deps <- statefulDependencies
          (persistentState, ephemeralState) = deps

          _ <- valueOrFail(
            persistentState.reassignmentStore.addUnassignmentData(reassignmentData)
          )(
            "add reassignment data failed"
          ).failOnShutdown

          result <-
            valueOrFail(
              assignmentProcessingSteps
                .constructPendingDataAndResponse(
                  mkParsedRequest(inconsistentTree),
                  ephemeralState.reassignmentCache,
                  FutureUnlessShutdown.pure(mkActivenessResult()),
                  engineController =
                    EngineController(participant, RequestId(CantonTimestamp.Epoch), loggerFactory),
                )
            )("construction of pending data and response failed").failOnShutdown

          metadataCheck =
            result.pendingData.assignmentValidationResult.metadataResultET.futureValueUS
        } yield {
          metadataCheck.left.value shouldBe expectedError
          result.confirmationResponsesF.futureValueUS.value
            .valueOrFail("no response")
            ._1
            .responses should matchPattern {
            case Seq(ConfirmationResponse(_, LocalReject(_, true), _)) =>
          }
        }).futureValue,
        modelConformanceError,
      )
    }
  }

  "get commit set and contracts to be stored and event" should {
    "succeed without errors" in {
      val contractId = ExampleTransactionFactory.suffixedId(10, 0)
      val contract =
        ExampleTransactionFactory.asSerializable(
          contractId,
          contractInstance = ExampleTransactionFactory.contractInstance(),
          metadata = ContractMetadata.tryCreate(Set(party1), Set(party1), None),
        )
      val reassignmentId = ReassignmentId(sourceSynchronizer, CantonTimestamp.Epoch)
      val rootHash = mock[RootHash]
      when(rootHash.asLedgerTransactionId).thenReturn(LedgerTransactionId.fromString("id1"))
      val pendingRequestData = AssignmentProcessingSteps.PendingAssignment(
        RequestId(CantonTimestamp.Epoch),
        RequestCounter(1),
        SequencerCounter(1),
        assignmentValidationResult = AssignmentValidationResult(
          rootHash,
          contract,
          initialReassignmentCounter,
          submitterInfo(submitter),
          reassignmentId,
          isReassigningParticipant = false,
          hostedStakeholders = contract.metadata.stakeholders,
          validationResult = AssignmentValidationResult.ValidationResult(
            activenessResult = mkActivenessResult(),
            authenticationErrorO = None,
            metadataResultET = EitherT.rightT(()),
            validationErrors = Seq.empty,
          ),
        ),
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
              Verdict.Approve(testedProtocolVersion),
              pendingRequestData,
              state.pendingAssignmentSubmissions,
              crypto.pureCrypto,
            )
            .failOnShutdown
        )("get commit set and contracts to be stored and event failed")
      } yield succeed
    }
  }

  "verify the submitting participant signature" should {
    val assignmentTree = makeFullAssignmentTree()

    "succeed when the signature is correct" in {
      for {
        signature <- cryptoSnapshot
          .sign(assignmentTree.rootHash.unwrap, SigningKeyUsage.ProtocolOnly)
          .valueOrFailShutdown("signing failed")

        parsed = mkParsedRequest(
          assignmentTree
        ).copy(signatureO = Some(signature))
        authenticationError <-
          AuthenticationValidator.verifyViewSignature(parsed).failOnShutdown
      } yield authenticationError shouldBe None
    }

    "fail when the signature is missing" in {
      val parsed = mkParsedRequest(
        assignmentTree
      ).copy(signatureO = None)
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
          assignmentTree
        ).copy(signatureO = Some(signature))
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

  private def testInstance(
      targetSynchronizer: Target[SynchronizerId],
      snapshotOverride: SynchronizerSnapshotSyncCryptoApi,
      awaitTimestampOverride: Option[Future[Unit]],
  ) = {

    val pureCrypto = new SymbolicPureCrypto
    val seedGenerator = new SeedGenerator(pureCrypto)

    new AssignmentProcessingSteps(
      targetSynchronizer,
      participant,
      TestReassignmentCoordination.apply(
        Set(),
        CantonTimestamp.Epoch,
        Some(snapshotOverride),
        Some(awaitTimestampOverride),
        loggerFactory,
      ),
      seedGenerator,
      ContractAuthenticator(pureCrypto),
      Target(defaultStaticSynchronizerParameters),
      Target(testedProtocolVersion),
      loggerFactory = loggerFactory,
    )
  }

  private def makeFullAssignmentTree(
      reassignmentId: ReassignmentId = reassignment10,
      submitter: LfPartyId = party1,
      contract: SerializableContract = contract,
      targetSynchronizer: Target[SynchronizerId] = targetSynchronizer,
      targetMediator: MediatorGroupRecipient = targetMediator,
      uuid: UUID = new UUID(4L, 5L),
      reassigningParticipants: Set[ParticipantId] = Set.empty,
  ): FullAssignmentTree = {
    val seed = seedGenerator.generateSaltSeed()

    valueOrFail(
      AssignmentProcessingSteps.makeFullAssignmentTree(
        crypto.pureCrypto,
        seed,
        reassignmentId,
        submitterInfo(submitter),
        contract,
        initialReassignmentCounter,
        targetSynchronizer,
        targetMediator,
        uuid,
        Target(testedProtocolVersion),
        reassigningParticipants = reassigningParticipants,
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
