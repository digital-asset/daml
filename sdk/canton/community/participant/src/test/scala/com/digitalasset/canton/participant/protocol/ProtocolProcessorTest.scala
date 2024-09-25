// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import cats.Eval
import cats.data.EitherT
import com.daml.metrics.api.MetricsContext
import com.daml.nonempty.NonEmpty
import com.daml.test.evidence.scalatest.ScalaTestSupport.TagContainer
import com.daml.test.evidence.tag.EvidenceTag
import com.daml.test.evidence.tag.Security.{Attack, SecurityTest, SecurityTestSuite}
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.config.{
  BatchingConfig,
  CachingConfigs,
  CommunityStorageConfig,
  DefaultProcessingTimeouts,
  ProcessingTimeout,
  TestingConfigInternal,
}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.data.DeduplicationPeriod.DeduplicationDuration
import com.digitalasset.canton.data.PeanoQueue.{BeforeHead, NotInserted}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.participant.state.{CompletionInfo, Update}
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.participant.DefaultParticipantStateValues
import com.digitalasset.canton.participant.admin.PackageDependencyResolver
import com.digitalasset.canton.participant.config.LedgerApiServerConfig
import com.digitalasset.canton.participant.ledger.api.LedgerApiIndexer
import com.digitalasset.canton.participant.metrics.ParticipantTestMetrics
import com.digitalasset.canton.participant.protocol.EngineController.EngineAbortStatus
import com.digitalasset.canton.participant.protocol.Phase37Synchronizer.RequestOutcome
import com.digitalasset.canton.participant.protocol.ProcessingSteps.{CleanReplayData, Wrapped}
import com.digitalasset.canton.participant.protocol.ProtocolProcessor.*
import com.digitalasset.canton.participant.protocol.RequestJournal.RequestState
import com.digitalasset.canton.participant.protocol.TestProcessingSteps.{
  TestPendingRequestData,
  TestPendingRequestDataType,
  TestProcessorError,
  TestViewType,
}
import com.digitalasset.canton.participant.protocol.conflictdetection.ConflictDetectionHelpers.*
import com.digitalasset.canton.participant.protocol.submission.InFlightSubmissionTracker.InFlightSubmissionTrackerDomainState
import com.digitalasset.canton.participant.protocol.submission.*
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.store.memory.*
import com.digitalasset.canton.participant.sync.ParticipantEventPublisher
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceAlarm
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.resource.MemoryStorage
import com.digitalasset.canton.sequencing.AsyncResult
import com.digitalasset.canton.sequencing.client.SendResult.Success
import com.digitalasset.canton.sequencing.client.{
  SendAsyncClientError,
  SendCallback,
  SequencerClientSend,
}
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.sequencing.traffic.TrafficReceipt
import com.digitalasset.canton.store.IndexedDomain
import com.digitalasset.canton.store.memory.InMemoryIndexedStringStore
import com.digitalasset.canton.time.{DomainTimeTracker, NonNegativeFiniteDuration, WallClock}
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.util.PekkoUtil.FutureQueue
import com.digitalasset.canton.version.HasTestCloseContext
import com.digitalasset.canton.{
  BaseTest,
  DefaultDamlValues,
  HasExecutionContext,
  RequestCounter,
  SequencerCounter,
}
import com.google.protobuf.ByteString
import org.apache.pekko.Done
import org.mockito.ArgumentMatchers.eq as isEq
import org.scalatest.Tag
import org.scalatest.wordspec.AnyWordSpec

import java.time.Duration
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference
import scala.collection.concurrent
import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions

class ProtocolProcessorTest
    extends AnyWordSpec
    with BaseTest
    with SecurityTestSuite
    with HasExecutionContext
    with HasTestCloseContext {
  // Workaround to avoid false errors reported by IDEA.
  private implicit def tagToContainer(tag: EvidenceTag): Tag = new TagContainer(tag)

  private lazy val authenticity: SecurityTest =
    SecurityTest(SecurityTest.Property.Authenticity, "virtual shared ledger")

  private def authenticityAttack(threat: String, mitigation: String)(implicit
      lineNo: sourcecode.Line,
      fileName: sourcecode.File,
  ): SecurityTest =
    authenticity.setAttack(Attack("a malicious network participant", threat, mitigation))

  private val participant = ParticipantId(
    UniqueIdentifier.tryFromProtoPrimitive("participant::participant")
  )
  private val otherParticipant = ParticipantId(
    UniqueIdentifier.tryFromProtoPrimitive("participant::other-participant")
  )
  private val party = PartyId(UniqueIdentifier.tryFromProtoPrimitive("party::participant"))
  private val domain = DefaultTestIdentities.domainId
  private val topology: TestingTopology = TestingTopology(
    Set(domain),
    Map(
      party.toLf -> Map(
        participant -> ParticipantPermission.Submission
      )
    ),
    Set(
      MediatorGroup(
        NonNegativeInt.zero,
        Seq(DefaultTestIdentities.daMediator),
        Seq(),
        PositiveInt.one,
      )
    ),
  )
  private val crypto =
    TestingIdentityFactory(topology, loggerFactory, TestDomainParameters.defaultDynamic)
      .forOwnerAndDomain(participant, domain)
  private val mockSequencerClient = mock[SequencerClientSend]
  when(
    mockSequencerClient.sendAsync(
      any[Batch[DefaultOpenEnvelope]],
      any[Option[CantonTimestamp]],
      any[CantonTimestamp],
      any[MessageId],
      any[Option[AggregationRule]],
      any[SendCallback],
      any[Boolean],
    )(anyTraceContext, any[MetricsContext])
  )
    .thenAnswer {
      (
          batch: Batch[DefaultOpenEnvelope],
          _: Option[CantonTimestamp],
          _: CantonTimestamp,
          messageId: MessageId,
          _: Option[AggregationRule],
          callback: SendCallback,
      ) =>
        callback(
          UnlessShutdown.Outcome(
            Success(
              Deliver.create(
                SequencerCounter(0),
                CantonTimestamp.Epoch,
                domain,
                Some(messageId),
                Batch.filterOpenEnvelopesFor(batch, participant, Set.empty),
                None,
                testedProtocolVersion,
                Option.empty[TrafficReceipt],
              )
            )
          )
        )
        EitherTUtil.unitUS
    }

  private val mockInFlightSubmissionTracker = mock[InFlightSubmissionTracker]
  when(
    mockInFlightSubmissionTracker.observeSequencedRootHash(
      any[RootHash],
      any[SequencedSubmission],
    )(anyTraceContext)
  ).thenAnswer(Future.unit)

  private val trm = mock[ConfirmationResultMessage]
  when(trm.pretty).thenAnswer(Pretty.adHocPrettyInstance[ConfirmationResultMessage])
  when(trm.verdict).thenAnswer(Verdict.Approve(testedProtocolVersion))
  when(trm.rootHash).thenAnswer(rootHash)
  when(trm.domainId).thenAnswer(DefaultTestIdentities.domainId)

  private val requestId = RequestId(CantonTimestamp.Epoch)
  private val requestSc = SequencerCounter(0)
  private val resultSc = SequencerCounter(1)
  private val rc = RequestCounter(0)
  private val parameters = DynamicDomainParametersWithValidity(
    DynamicDomainParameters.initialValues(NonNegativeFiniteDuration.Zero, testedProtocolVersion),
    CantonTimestamp.MinValue,
    None,
    domain,
  )

  private val sessionKeyMapTest = NonEmpty(
    Seq,
    new AsymmetricEncrypted[SecureRandomness](
      ByteString.EMPTY,
      // this is only a placeholder, the data is not encrypted
      crypto.pureCrypto.defaultEncryptionAlgorithmSpec,
      Fingerprint.tryCreate("dummy"),
    ),
  )

  private type TestInstance =
    ProtocolProcessor[
      Int,
      Unit,
      TestViewType,
      TestProcessingSteps.TestProcessingError,
    ]

  private def waitForAsyncResult(asyncResult: AsyncResult) = asyncResult.unwrap.unwrap.futureValue

  private def testProcessingSteps(
      overrideConstructedPendingRequestDataO: Option[TestPendingRequestData] = None,
      startingPoints: ProcessingStartingPoints = ProcessingStartingPoints.default,
      pendingSubmissionMap: concurrent.Map[Int, Unit] = TrieMap[Int, Unit](),
      sequencerClient: SequencerClientSend = mockSequencerClient,
      crypto: DomainSyncCryptoClient = crypto,
      overrideInFlightSubmissionTrackerO: Option[InFlightSubmissionTracker] = None,
      submissionDataForTrackerO: Option[SubmissionTracker.SubmissionData] = None,
      overrideInFlightSubmissionStoreO: Option[InFlightSubmissionStore] = None,
  ): (
      TestInstance,
      SyncDomainPersistentState,
      SyncDomainEphemeralState,
      ParticipantNodeEphemeralState,
  ) = {

    val packageDependencyResolver = mock[PackageDependencyResolver]
    val clock = new WallClock(timeouts, loggerFactory)

    val nodePersistentState = timeouts.default.await("creating node persistent state")(
      ParticipantNodePersistentState
        .create(
          new MemoryStorage(loggerFactory, timeouts),
          CommunityStorageConfig.Memory(),
          exitOnFatalFailures = true,
          None,
          BatchingConfig(),
          testedReleaseProtocolVersion,
          ParticipantTestMetrics,
          participant.toLf,
          LedgerApiServerConfig(),
          timeouts,
          futureSupervisor,
          loggerFactory,
        )
        .failOnShutdown
    )

    val persistentState =
      new InMemorySyncDomainPersistentState(
        participant,
        clock,
        crypto.crypto,
        IndexedDomain.tryCreate(domain, 1),
        defaultStaticDomainParameters,
        enableAdditionalConsistencyChecks = true,
        new InMemoryIndexedStringStore(minIndex = 1, maxIndex = 1), // only one domain needed
        exitOnFatalFailures = true,
        packageDependencyResolver,
        Eval.now(nodePersistentState.ledgerApiStore),
        loggerFactory,
        timeouts,
        futureSupervisor,
      )

    val ephemeralState = new AtomicReference[SyncDomainEphemeralState]()

    val ledgerApiIndexer = mock[LedgerApiIndexer]
    when(ledgerApiIndexer.queue).thenAnswer(
      new FutureQueue[Traced[Update]] {
        override def offer(elem: Traced[Update]): Future[Done] = Future.successful(Done)

        override def shutdown(): Unit = ()

        override def done: Future[Done] = Future.successful(Done)
      }
    )

    val eventPublisher = new ParticipantEventPublisher(
      participant,
      Eval.now(ledgerApiIndexer),
      clock,
      exitOnFatalFailures = true,
      timeouts,
      futureSupervisor,
      loggerFactory,
    )
    val inFlightSubmissionTracker = overrideInFlightSubmissionTrackerO.getOrElse {
      val tracker = new InFlightSubmissionTracker(
        Eval.now(
          overrideInFlightSubmissionStoreO.getOrElse(nodePersistentState.inFlightSubmissionStore)
        ),
        new NoCommandDeduplicator(),
        DefaultProcessingTimeouts.testing,
        loggerFactory,
      )
      tracker.registerDomainStateLookup(_ =>
        Option(ephemeralState.get())
          .map(InFlightSubmissionTrackerDomainState.fromSyncDomainState)
      )
      tracker
    }
    val participantNodeEphemeralState =
      new ParticipantNodeEphemeralState(eventPublisher, inFlightSubmissionTracker)
    val timeTracker = mock[DomainTimeTracker]

    ephemeralState.set(
      new SyncDomainEphemeralState(
        participant,
        participantNodeEphemeralState,
        persistentState,
        ledgerApiIndexer,
        startingPoints,
        () => timeTracker,
        ParticipantTestMetrics.domain,
        exitOnFatalFailures = true,
        CachingConfigs.defaultSessionKeyCacheConfig,
        timeouts,
        loggerFactory,
        FutureSupervisor.Noop,
        clock,
      )
    )

    val steps = new TestProcessingSteps(
      pendingSubmissionMap = pendingSubmissionMap,
      overrideConstructedPendingRequestDataO,
      informeesOfView = _ => Set(party.toLf),
      submissionDataForTrackerO = submissionDataForTrackerO,
    )

    val sut: ProtocolProcessor[
      Int,
      Unit,
      TestViewType,
      TestProcessingSteps.TestProcessingError,
    ] =
      new ProtocolProcessor(
        steps,
        inFlightSubmissionTracker,
        ephemeralState.get(),
        crypto,
        sequencerClient,
        domainId = DefaultTestIdentities.domainId,
        defaultStaticDomainParameters,
        testedProtocolVersion,
        loggerFactory,
        FutureSupervisor.Noop,
        FlagCloseable.withCloseContext(logger, timeouts),
      )(directExecutionContext: ExecutionContext) {
        override def testingConfig: TestingConfigInternal = TestingConfigInternal()

        override def participantId: ParticipantId = participant

        override def timeouts: ProcessingTimeout = ProtocolProcessorTest.this.timeouts

        override protected def metricsContextForSubmissionParam(
            submissionParam: Int
        ): MetricsContext = MetricsContext.Empty
      }

    (sut, persistentState, ephemeralState.get(), participantNodeEphemeralState)
  }

  private lazy val rootHash = RootHash(TestHash.digest(1))
  private lazy val testTopologyTimestamp = CantonTimestamp.Epoch
  private lazy val viewHash = ViewHash(TestHash.digest(2))
  private lazy val encryptedView =
    EncryptedView(TestViewType)(Encrypted.fromByteString(rootHash.toProtoPrimitive))
  private lazy val viewMessage: EncryptedViewMessage[TestViewType] = EncryptedViewMessage(
    submittingParticipantSignature = None,
    viewHash = viewHash,
    sessionKeys = sessionKeyMapTest,
    encryptedView = encryptedView,
    domainId = DefaultTestIdentities.domainId,
    SymmetricKeyScheme.Aes128Gcm,
    testedProtocolVersion,
  )
  private lazy val rootHashMessage = RootHashMessage(
    rootHash,
    DefaultTestIdentities.domainId,
    testedProtocolVersion,
    TestViewType,
    testTopologyTimestamp,
    SerializedRootHashMessagePayload.empty,
  )
  private lazy val someRecipients = Recipients.cc(participant)
  private lazy val someRequestBatch = RequestAndRootHashMessage(
    NonEmpty(Seq, OpenEnvelope(viewMessage, someRecipients)(testedProtocolVersion)),
    rootHashMessage,
    MediatorGroupRecipient(MediatorGroupIndex.zero),
    isReceipt = false,
  )

  private lazy val subId = DefaultDamlValues.submissionId()
  private lazy val changeId = DefaultParticipantStateValues.changeId(Set.empty)
  private lazy val changeIdHash = ChangeIdHash(changeId)

  private lazy val unsequencedSubmission = InFlightSubmission(
    changeIdHash = changeIdHash,
    submissionId = Some(subId),
    submissionDomain = domain,
    messageUuid = UUID.randomUUID(),
    rootHashO = Some(rootHash),
    sequencingInfo = UnsequencedSubmission(
      requestId.unwrap,
      TransactionSubmissionTrackingData(
        CompletionInfo(
          List.empty,
          changeId.applicationId,
          changeId.commandId,
          None,
          Some(subId),
          None,
        ),
        TransactionSubmissionTrackingData.TimeoutCause,
        domain,
        testedProtocolVersion,
      ),
    ),
    TraceContext.empty,
  )

  "submit" should {

    "succeed without errors" in {
      val submissionMap = TrieMap[Int, Unit]()
      val (sut, _persistent, _ephemeral, _) =
        testProcessingSteps(pendingSubmissionMap = submissionMap)
      sut
        .submit(0)
        .valueOrFailShutdown("submission")
        .futureValue
        .failOnShutdown("shutting down while test is running")
        .futureValue shouldBe ()
      submissionMap.get(0) shouldBe Some(()) // store the pending submission
    }

    "clean up the pending submissions when send request fails" in {
      val submissionMap = TrieMap[Int, Unit]()
      val failingSequencerClient = mock[SequencerClientSend]
      val sendError = SendAsyncClientError.RequestFailed("no thank you")
      when(
        failingSequencerClient.sendAsync(
          any[Batch[DefaultOpenEnvelope]],
          any[Option[CantonTimestamp]],
          any[CantonTimestamp],
          any[MessageId],
          any[Option[AggregationRule]],
          any[SendCallback],
          any[Boolean],
        )(anyTraceContext, any[MetricsContext])
      )
        .thenReturn(EitherT.leftT[FutureUnlessShutdown, Unit](sendError))
      val (sut, _persistent, _ephemeral, _) =
        testProcessingSteps(
          sequencerClient = failingSequencerClient,
          pendingSubmissionMap = submissionMap,
        )

      val submissionResult = loggerFactory.assertLogs(
        sut.submit(0).onShutdown(fail("submission shutdown")).value.futureValue,
        _.warningMessage should include(s"Failed to submit submission due to"),
      )

      submissionResult shouldEqual Left(TestProcessorError(SequencerRequestError(sendError)))
      submissionMap.get(0) shouldBe None // remove the pending submission
    }

    "clean up the pending submissions when no request is received" in {
      val submissionMap = TrieMap[Int, Unit]()
      val (sut, _persistent, _ephemeral, _) =
        testProcessingSteps(pendingSubmissionMap = submissionMap)

      sut
        .submit(1)
        .valueOrFailShutdown("submission")
        .futureValue
        .failOnShutdown("shutting down while test is running")
        .futureValue shouldBe ()
      submissionMap.get(1) shouldBe Some(())
      val afterDecisionTime = parameters.decisionTimeFor(CantonTimestamp.Epoch).value.plusMillis(1)
      val asyncRes = sut
        .processRequest(afterDecisionTime, rc, requestSc, someRequestBatch)
        .onShutdown(fail())
        .futureValue
      waitForAsyncResult(asyncRes)
      eventually() {
        submissionMap.get(1) shouldBe None
      }
    }

    "fail if there is no active mediator" in {
      val crypto2 = TestingIdentityFactory(
        TestingTopology(mediatorGroups = Set.empty),
        loggerFactory,
        parameters.parameters,
      ).forOwnerAndDomain(participant, domain)
      val (sut, persistent, ephemeral, _) = testProcessingSteps(crypto = crypto2)
      val res = sut.submit(1).onShutdown(fail("submission shutdown")).value.futureValue
      res shouldBe Left(TestProcessorError(NoMediatorError(CantonTimestamp.Epoch)))
    }
  }

  "process request" should {

    "succeed without errors" in {
      val (sut, _persistent, _ephemeral, _) = testProcessingSteps()
      val asyncRes = sut
        .processRequest(requestId.unwrap, rc, requestSc, someRequestBatch)
        .onShutdown(fail())
        .futureValue
      waitForAsyncResult(asyncRes)
      succeed
    }

    "transit to confirmed" in {
      val pd = TestPendingRequestData(
        rc,
        requestSc,
        MediatorGroupRecipient(MediatorGroupIndex.one),
        locallyRejectedF = FutureUnlessShutdown.pure(false),
        abortEngine = _ => (),
        engineAbortStatusF = FutureUnlessShutdown.pure(EngineAbortStatus.notAborted),
      )
      val (sut, _persistent, ephemeral, _) =
        testProcessingSteps(overrideConstructedPendingRequestDataO = Some(pd))
      val before = ephemeral.requestJournal.query(rc).value.futureValue
      before shouldEqual None

      val requestTs = CantonTimestamp.Epoch
      val asyncRes = sut
        .processRequest(requestTs, rc, requestSc, someRequestBatch)
        .onShutdown(fail())
        .futureValue
      waitForAsyncResult(asyncRes)
      val requestState = ephemeral.requestJournal.query(rc).value.futureValue
      requestState.value.state shouldEqual RequestState.Pending
      ephemeral.phase37Synchronizer
        .awaitConfirmed(TestPendingRequestDataType)(RequestId(requestTs))
        .futureValueUS shouldBe RequestOutcome.Success(Wrapped(pd))
    }

    "leave the request state unchanged when doing a clean replay" in {
      val pendingData =
        TestPendingRequestData(
          rc,
          requestSc,
          MediatorGroupRecipient(MediatorGroupIndex.one),
          locallyRejectedF = FutureUnlessShutdown.pure(false),
          abortEngine = _ => (),
          engineAbortStatusF = FutureUnlessShutdown.pure(EngineAbortStatus.notAborted),
        )
      val (sut, _persistent, ephemeral, _) =
        testProcessingSteps(
          overrideConstructedPendingRequestDataO = Some(pendingData),
          startingPoints = ProcessingStartingPoints.tryCreate(
            cleanReplay = MessageCleanReplayStartingPoint(
              rc,
              requestSc,
              CantonTimestamp.Epoch.minusSeconds(20),
            ),
            processing = MessageProcessingStartingPoint(
              rc + 1,
              requestSc + 1,
              CantonTimestamp.Epoch.minusSeconds(10),
            ),
          ),
        )

      val before = ephemeral.requestJournal.query(rc).value.futureValue
      before shouldEqual None

      val asyncRes = sut
        .processRequest(requestId.unwrap, rc, requestSc, someRequestBatch)
        .onShutdown(fail())
        .futureValue
      waitForAsyncResult(asyncRes)
      val requestState = ephemeral.requestJournal.query(rc).value.futureValue
      requestState shouldEqual None
    }

    "trigger a timeout when the result doesn't arrive" in {
      val pd = TestPendingRequestData(
        rc,
        requestSc,
        MediatorGroupRecipient(MediatorGroupIndex.one),
        locallyRejectedF = FutureUnlessShutdown.pure(false),
        abortEngine = _ => (),
        engineAbortStatusF = FutureUnlessShutdown.pure(EngineAbortStatus.notAborted),
      )
      val (sut, _persistent, ephemeral, _) =
        testProcessingSteps(overrideConstructedPendingRequestDataO = Some(pd))

      val journal = ephemeral.requestJournal

      val initialSTate = journal.query(rc).value.futureValue
      initialSTate shouldEqual None

      // Process a request but never a corresponding response
      val asyncRes = sut
        .processRequest(CantonTimestamp.Epoch, rc, requestSc, someRequestBatch)
        .onShutdown(fail())
        .futureValue
      waitForAsyncResult(asyncRes)

      ephemeral.requestTracker.taskScheduler.readSequencerCounterQueue(
        requestSc
      ) shouldBe BeforeHead

      // The request remains at Pending until the timeout is triggered
      always() {
        journal.query(rc).value.futureValue.value.state shouldEqual RequestState.Pending
      }

      // Trigger the timeout for the request
      ephemeral.requestTracker.tick(
        requestSc + 1,
        parameters.decisionTimeFor(requestId.unwrap).value,
      )

      eventually() {
        val state = journal.query(rc).value.futureValue
        state.value.state shouldEqual RequestState.Clean
      }
    }

    "log wrong root hashes" in {
      val wrongRootHash = RootHash(TestHash.digest(3))
      val viewHash1 = ViewHash(TestHash.digest(2))
      val encryptedViewWrongRH =
        EncryptedView(TestViewType)(Encrypted.fromByteString(wrongRootHash.toProtoPrimitive))
      val viewMessageWrongRH = EncryptedViewMessage(
        submittingParticipantSignature = None,
        viewHash = viewHash1,
        sessionKeys = sessionKeyMapTest,
        encryptedView = encryptedViewWrongRH,
        domainId = DefaultTestIdentities.domainId,
        SymmetricKeyScheme.Aes128Gcm,
        testedProtocolVersion,
      )
      val requestBatchWrongRH = RequestAndRootHashMessage(
        NonEmpty(
          Seq,
          OpenEnvelope(viewMessage, someRecipients)(testedProtocolVersion),
          OpenEnvelope(viewMessageWrongRH, someRecipients)(testedProtocolVersion),
        ),
        rootHashMessage,
        MediatorGroupRecipient(MediatorGroupIndex.zero),
        isReceipt = false,
      )

      val (sut, _persistent, _ephemeral, _) = testProcessingSteps()
      val asyncRes = loggerFactory
        .assertLogs(
          sut
            .processRequest(requestId.unwrap, rc, requestSc, requestBatchWrongRH)
            .onShutdown(fail()),
          _.warningMessage should include(
            s"Request $rc: Found malformed payload: WrongRootHash"
          ),
        )
        .futureValue
      waitForAsyncResult(asyncRes)
    }

    "log decryption errors" in {
      val viewMessageDecryptError: EncryptedViewMessage[TestViewType] = EncryptedViewMessage(
        submittingParticipantSignature = None,
        viewHash = viewHash,
        sessionKeys = sessionKeyMapTest,
        encryptedView = EncryptedView(TestViewType)(Encrypted.fromByteString(ByteString.EMPTY)),
        domainId = DefaultTestIdentities.domainId,
        viewEncryptionScheme = SymmetricKeyScheme.Aes128Gcm,
        protocolVersion = testedProtocolVersion,
      )

      val requestBatchDecryptError = RequestAndRootHashMessage(
        NonEmpty(
          Seq,
          OpenEnvelope(viewMessageDecryptError, someRecipients)(testedProtocolVersion),
        ),
        rootHashMessage,
        MediatorGroupRecipient(MediatorGroupIndex.zero),
        isReceipt = false,
      )

      val (sut, _persistent, _ephemeral, _) = testProcessingSteps()
      val asyncRes = loggerFactory
        .assertLogs(
          sut
            .processRequest(requestId.unwrap, rc, requestSc, requestBatchDecryptError)
            .onShutdown(fail()),
          _.warningMessage should include(
            s"Request $rc: Decryption error: SyncCryptoDecryptError("
          ),
        )
        .futureValue
      waitForAsyncResult(asyncRes)
      succeed
    }

    "check the declared mediator ID against the root hash message mediator" taggedAs {
      authenticityAttack(
        threat =
          "the mediator in the common metadata is not the recipient of the root hash message",
        mitigation = "all honest participants roll back the request",
      )
    } in {
      // Instead of rolling back the request in Phase 7, it is discarded in Phase 3. This has the same effect.

      val otherMediatorGroup = MediatorGroupRecipient(MediatorGroupIndex.one)
      val requestBatch = RequestAndRootHashMessage(
        NonEmpty(Seq, OpenEnvelope(viewMessage, someRecipients)(testedProtocolVersion)),
        rootHashMessage,
        otherMediatorGroup,
        isReceipt = false,
      )

      val (sut, _persistent, _ephemeral, _) = testProcessingSteps()
      loggerFactory
        .assertLogs(
          sut
            .processRequest(requestId.unwrap, rc, requestSc, requestBatch)
            .onShutdown(fail()),
          _.errorMessage should include(
            s"Mediator ${MediatorGroupRecipient(MediatorGroupIndex.zero)} declared in views is not the recipient $otherMediatorGroup of the root hash message"
          ),
        )
        .futureValue

    }

    "check that the mediator is active" taggedAs {
      authenticityAttack(
        threat = "the mediator in the common metadata is not active",
        mitigation = "all honest participants roll back the request",
      )
    } in {
      // Instead of rolling back the request in Phase 7, it is discarded in Phase 3. This has the same effect.

      val testCrypto = TestingIdentityFactory(
        topology.copy(mediatorGroups = Set.empty), // Topology without any mediator active
        loggerFactory,
        parameters.parameters,
      ).forOwnerAndDomain(participant, domain)

      val (sut, _persistent, _ephemeral, _) = testProcessingSteps(crypto = testCrypto)
      loggerFactory
        .assertLogs(
          sut
            .processRequest(requestId.unwrap, rc, requestSc, someRequestBatch)
            .onShutdown(fail()),
          _.shouldBeCantonError(
            SyncServiceAlarm,
            _ shouldBe s"Request $rc: Chosen mediator ${MediatorGroupRecipient(MediatorGroupIndex.zero)} is inactive at ${requestId.unwrap}. Skipping this request.",
          ),
        )
        .futureValue
    }

    "notify the in-flight submission tracker with the root hash when necessary" in {
      val (sut, _persistent, _ephemeral, _) =
        testProcessingSteps(
          overrideInFlightSubmissionTrackerO = Some(mockInFlightSubmissionTracker),
          submissionDataForTrackerO = Some(
            SubmissionTracker.SubmissionData(
              submittingParticipant = participant,
              maxSequencingTime = requestId.unwrap.plusSeconds(10),
            )
          ),
        )

      val asyncRes = sut
        .processRequest(requestId.unwrap, rc, requestSc, someRequestBatch)
        .onShutdown(fail())
        .futureValue
      waitForAsyncResult(asyncRes)

      verify(mockInFlightSubmissionTracker).observeSequencedRootHash(
        isEq(someRequestBatch.rootHashMessage.rootHash),
        isEq(SequencedSubmission(requestSc, requestId.unwrap)),
      )(anyTraceContext)
    }

    "not notify the in-flight submission tracker when the message is a receipt" in {
      val (sut, _persistent, _ephemeral, _) =
        testProcessingSteps(
          overrideInFlightSubmissionTrackerO = Some(mockInFlightSubmissionTracker),
          submissionDataForTrackerO = Some(
            SubmissionTracker.SubmissionData(
              submittingParticipant = participant,
              maxSequencingTime = requestId.unwrap.plusSeconds(10),
            )
          ),
        )

      val asyncRes = sut
        .processRequest(requestId.unwrap, rc, requestSc, someRequestBatch.copy(isReceipt = true))
        .onShutdown(fail())
        .futureValue
      waitForAsyncResult(asyncRes)

      verifyZeroInteractions(mockInFlightSubmissionTracker)
    }

    "not notify the in-flight submission tracker when not submitting participant" in {
      val (sut, _persistent, _ephemeral, _) =
        testProcessingSteps(
          overrideInFlightSubmissionTrackerO = Some(mockInFlightSubmissionTracker),
          submissionDataForTrackerO = Some(
            SubmissionTracker.SubmissionData(
              submittingParticipant = otherParticipant,
              maxSequencingTime = requestId.unwrap.plusSeconds(10),
            )
          ),
        )

      val asyncRes = sut
        .processRequest(requestId.unwrap, rc, requestSc, someRequestBatch)
        .onShutdown(fail())
        .futureValue
      waitForAsyncResult(asyncRes)

      verifyZeroInteractions(mockInFlightSubmissionTracker)
    }

    "override the sequencing time when a preplay loses the race with the normal notification" in {
      // This test reproduces the scenario of a submission request preplayed without a messageId, i.e.
      // the participant receives first the preplay without messageId, and later the actual submission receipt.
      //
      // Background:
      // -----------
      // The submission tracker ensures only the first sequenced request produces a command completion, and
      // accordingly the in-flight submission tracker must track the preplay as sequenced.
      // The in-flight submission tracker is normally notified of the sequencing when it receives a request
      // with the messageId corresponding to the submission.
      // However, to handle replays, it is also notified when receiving a matching request (based on the
      // root hash), without messageId, but for which the participant is the submitter.
      // The normal notification happens in the synchronous request processing, while the other takes place
      // during the asynchronous part. Consequently, a race happens as to which notification happens first.
      // To handle this, the non-normal notification is allowed to override the in-flight submission tracker
      // information if the sequencing time of its request is earlier.
      //
      // This test checks this situation when the non-normal notification loses this race.

      val inFlightSubmissionStore = new InMemoryInFlightSubmissionStore(loggerFactory)
      val (sut, _persistent, ephemeral, nodeEphemeral) = testProcessingSteps(
        submissionDataForTrackerO = Some(
          SubmissionTracker.SubmissionData(
            submittingParticipant = participant,
            maxSequencingTime = requestId.unwrap.plusSeconds(10),
          )
        ),
        overrideInFlightSubmissionStoreO = Some(inFlightSubmissionStore),
      )

      val ifst = nodeEphemeral.inFlightSubmissionTracker
      val subF = for {
        // The participant registers the submission in the in-flight submission tracker
        _ <- ifst
          .register(unsequencedSubmission, DeduplicationDuration(Duration.ofSeconds(10)))
          .value
          .onShutdown(fail())

        // Even though sequenced later, the normal notification wins the race
        _ <- ifst.observeSequencing(
          domain,
          Map(
            unsequencedSubmission.messageId -> SequencedSubmission(
              SequencerCounter(1),
              requestId.unwrap.plusSeconds(1),
            )
          ),
        )

        // The preplay gets processed and notifies the in-flight submission tracker
        _ <- sut
          .processRequest(requestId.unwrap, rc, requestSc, someRequestBatch)
          .onShutdown(fail())
          .futureValue
          .unwrap
          .unwrap

        // Retrieve the submission from the in-flight submission tracker store to check its info
        sub <- inFlightSubmissionStore.lookup(changeIdHash).getOrElse(fail())
      } yield sub

      val sub = subF.futureValue
      sub.sequencingInfo.isSequenced shouldBe true
      sub.sequencingInfo match {
        // The information corresponds to the preplay
        case SequencedSubmission(`requestSc`, ts) => ts shouldBe requestId.unwrap
        case _ => fail(s"Bad information in in-flight submission tracker:\n$sub")
      }
    }
  }

  "perform result processing" should {

    val requestTimestamp = requestId.unwrap
    val activenessTimestamp = CantonTimestamp.Epoch
    val activenessSet = mkActivenessSet()

    def addRequestState(
        ephemeral: SyncDomainEphemeralState,
        decisionTime: CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(60),
    ): Unit =
      ephemeral.requestTracker
        .addRequest(
          rc,
          requestSc,
          requestTimestamp,
          activenessTimestamp,
          decisionTime,
          activenessSet,
        )
        .value
        .failOnShutdown
        .futureValue

    def setUpOrFail(
        persistent: SyncDomainPersistentState,
        ephemeral: SyncDomainEphemeralState,
    ): Unit = {

      val setupF = for {
        _ <- persistent.parameterStore.setParameters(defaultStaticDomainParameters)

        _ <- ephemeral.requestJournal.insert(rc, CantonTimestamp.Epoch)
      } yield ephemeral.phase37Synchronizer
        .registerRequest(TestPendingRequestDataType)(requestId)
        .complete(
          Some(
            Wrapped(
              TestPendingRequestData(
                rc,
                requestSc,
                MediatorGroupRecipient(MediatorGroupIndex.one),
                locallyRejectedF = FutureUnlessShutdown.pure(false),
                abortEngine = _ => (),
                engineAbortStatusF = FutureUnlessShutdown.pure(EngineAbortStatus.notAborted),
              )
            )
          )
        )
      setupF.futureValue
    }

    def performResultProcessing(
        timestamp: CantonTimestamp,
        sut: TestInstance,
    ): EitherT[Future, sut.steps.ResultError, Unit] = {
      val mockSignedProtocolMessage = mock[SignedProtocolMessage[ConfirmationResultMessage]]
      when(mockSignedProtocolMessage.message).thenReturn(trm)
      when(
        mockSignedProtocolMessage
          .verifyMediatorSignatures(any[SyncCryptoApi], any[MediatorGroupIndex])(anyTraceContext)
      )
        .thenReturn(EitherT.rightT(()))
      sut
        .processResultInternal1(
          NoOpeningErrors(
            SignedContent(
              mock[Deliver[DefaultOpenEnvelope]],
              Signature.noSignature,
              None,
              testedProtocolVersion,
            )
          ),
          mockSignedProtocolMessage,
          requestId,
          timestamp,
          resultSc,
        )
        .map(_.onShutdown(fail("Unexpected shutdown")))
        .flatMap(identity)
    }

    "succeed without errors and transit to clean" in {
      val (sut, persistent, ephemeral, _) = testProcessingSteps()
      addRequestState(ephemeral)

      val taskScheduler = ephemeral.requestTracker.taskScheduler
      // Check the initial state is clean
      taskScheduler.readSequencerCounterQueue(resultSc) shouldBe NotInserted(None, None)

      setUpOrFail(persistent, ephemeral)
      valueOrFail(performResultProcessing(CantonTimestamp.Epoch.plusSeconds(10), sut))(
        "result processing failed"
      ).futureValue

      val finalState = ephemeral.requestJournal.query(rc).value.futureValue
      finalState.value.state shouldEqual RequestState.Clean

      taskScheduler.readSequencerCounterQueue(resultSc) shouldBe BeforeHead
    }

    "wait for request processing to finish" in {
      val (sut, _persistent, ephemeral, _) = testProcessingSteps()

      val taskScheduler = ephemeral.requestTracker.taskScheduler
      val requestJournal = ephemeral.requestJournal

      // Register request is called before request processing is triggered
      val handle =
        ephemeral.phase37Synchronizer
          .registerRequest(sut.steps.requestType)(
            requestId
          )
      ephemeral.submissionTracker
        .register(rootHash, requestId)
        .discard

      // Process the result message before the request
      val processF = performResultProcessing(CantonTimestamp.Epoch.plusSeconds(10), sut)

      // Processing should not complete as the request processing has not finished
      always()(processF.value.isCompleted shouldEqual false)

      // Check the result processing has not modified the request state
      taskScheduler.readSequencerCounterQueue(resultSc) shouldBe NotInserted(None, None)
      requestJournal.query(rc).value.futureValue shouldBe None

      // Now process the request message. This should trigger the completion of the result processing.
      sut
        .processRequestInternal(
          requestId.unwrap,
          rc,
          requestSc,
          someRequestBatch,
          handle,
          freshOwnTimelyTxF = FutureUnlessShutdown.pure(true),
        )
        .onShutdown(fail())
        .futureValue

      eventually() {
        processF.value.futureValue shouldEqual Right(())
        taskScheduler.readSequencerCounterQueue(resultSc) shouldBe BeforeHead
        requestJournal.query(rc).value.futureValue.value.state shouldBe RequestState.Clean
      }
    }

    "succeed without errors on clean replay, not changing the request state" in {

      val (sut, _persistent, ephemeral, _) = testProcessingSteps(
        startingPoints = ProcessingStartingPoints.tryCreate(
          MessageCleanReplayStartingPoint(rc, requestSc, CantonTimestamp.Epoch.minusSeconds(1)),
          MessageProcessingStartingPoint(
            rc + 5,
            requestSc + 10,
            CantonTimestamp.Epoch.plusSeconds(30),
          ),
        )
      )

      addRequestState(ephemeral)
      ephemeral.phase37Synchronizer
        .registerRequest(TestPendingRequestDataType)(requestId)
        .complete(
          Some(
            CleanReplayData(
              rc,
              requestSc,
              MediatorGroupRecipient(MediatorGroupIndex.one),
              locallyRejectedF = FutureUnlessShutdown.pure(false),
              abortEngine = _ => (),
              engineAbortStatusF = FutureUnlessShutdown.pure(EngineAbortStatus.notAborted),
            )
          )
        )

      val before = ephemeral.requestJournal.query(rc).value.futureValue
      before shouldEqual None
      val taskScheduler = ephemeral.requestTracker.taskScheduler
      taskScheduler.readSequencerCounterQueue(resultSc) shouldBe NotInserted(None, None)

      valueOrFail(performResultProcessing(CantonTimestamp.Epoch.plusSeconds(10), sut))(
        "result processing failed"
      ).futureValue

      val requestState = ephemeral.requestJournal.query(rc).value.futureValue
      requestState shouldEqual None
      taskScheduler.readSequencerCounterQueue(resultSc) shouldBe BeforeHead
    }

    "give an error when decision time has elapsed" in {
      val (sut, persistent, ephemeral, _) = testProcessingSteps()

      addRequestState(ephemeral, decisionTime = CantonTimestamp.Epoch.plusSeconds(5))

      val taskScheduler = ephemeral.requestTracker.taskScheduler
      taskScheduler.readSequencerCounterQueue(resultSc) shouldBe NotInserted(None, None)

      setUpOrFail(persistent, ephemeral)
      val result = loggerFactory.suppressWarningsAndErrors(
        leftOrFail(performResultProcessing(CantonTimestamp.Epoch.plusSeconds(5 * 60), sut))(
          "result processing did not return a left"
        ).futureValue
      )

      result match {
        case TestProcessorError(DecisionTimeElapsed(_, _)) =>
          taskScheduler.readSequencerCounterQueue(resultSc) shouldBe BeforeHead

        case _ => fail()
      }
    }
  }

}
