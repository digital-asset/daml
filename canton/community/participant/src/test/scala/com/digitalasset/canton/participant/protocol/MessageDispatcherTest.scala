// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import cats.syntax.flatMap.*
import cats.syntax.option.*
import com.daml.metrics.api.{MetricName, MetricsContext}
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.CantonRequireTypes.String255
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.crypto.{
  AsymmetricEncrypted,
  Encrypted,
  Fingerprint,
  SecureRandomness,
  SymmetricKeyScheme,
  TestHash,
}
import com.digitalasset.canton.data.*
import com.digitalasset.canton.error.MediatorError
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.pretty.PrettyUtil
import com.digitalasset.canton.logging.{LogEntry, NamedLoggerFactory}
import com.digitalasset.canton.metrics.CantonLabeledMetricsFactory.NoOpMetricsFactory
import com.digitalasset.canton.participant.event.RecordOrderPublisher
import com.digitalasset.canton.participant.metrics.SyncDomainMetrics
import com.digitalasset.canton.participant.protocol.MessageDispatcher.{AcsCommitment as _, *}
import com.digitalasset.canton.participant.protocol.conflictdetection.RequestTracker
import com.digitalasset.canton.participant.protocol.submission.{
  InFlightSubmissionTracker,
  SequencedSubmission,
}
import com.digitalasset.canton.participant.pruning.AcsCommitmentProcessor
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceAlarm
import com.digitalasset.canton.protocol.messages.EncryptedView.CompressedView
import com.digitalasset.canton.protocol.messages.TopologyTransactionsBroadcastX.Broadcast
import com.digitalasset.canton.protocol.messages.Verdict.MediatorReject
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.protocol.{
  RequestAndRootHashMessage,
  RequestId,
  RequestProcessor,
  RootHash,
  ViewHash,
  v30 as protocolv30,
}
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.sequencing.{
  HandlerResult,
  PossiblyIgnoredProtocolEvent,
  RawProtocolEvent,
  SequencerTestUtils,
}
import com.digitalasset.canton.store.SequencedEventStore.OrdinarySequencedEvent
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.processing.{SequencedTime, TopologyTransactionTestFactoryX}
import com.digitalasset.canton.tracing.Traced
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{ErrorUtil, MonadUtil}
import com.digitalasset.canton.version.*
import com.digitalasset.canton.{
  BaseTest,
  DiscardOps,
  HasExecutorService,
  ProtoDeserializationError,
  RequestCounter,
  SequencerCounter,
}
import com.google.protobuf.ByteString
import org.mockito.ArgumentMatchers.eq as isEq
import org.scalatest.Assertion
import org.scalatest.wordspec.AnyWordSpec

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

trait MessageDispatcherTest {
  this: AnyWordSpec & BaseTest & HasExecutorService =>

  implicit lazy val executionContext: ExecutionContext = executorService

  import MessageDispatcherTest.*

  private val domainId = DomainId.tryFromString("messageDispatcher::domain")
  private val participantId =
    ParticipantId.tryFromProtoPrimitive("PAR::messageDispatcher::participant")
  private val mediatorGroup = MediatorsOfDomain(MediatorGroupIndex.zero)
  private val mediatorGroup2 = MediatorsOfDomain(MediatorGroupIndex.one)

  private val encryptedRandomnessTest =
    Encrypted.fromByteString[SecureRandomness](ByteString.EMPTY).value
  private val sessionKeyMapTest = NonEmpty(
    Seq,
    new AsymmetricEncrypted[SecureRandomness](ByteString.EMPTY, Fingerprint.tryCreate("dummy")),
  )

  case class Fixture(
      messageDispatcher: MessageDispatcher,
      requestTracker: RequestTracker,
      testProcessor: RequestProcessor[TestViewType],
      otherTestProcessor: RequestProcessor[OtherTestViewType],
      topologyProcessor: (
          SequencerCounter,
          SequencedTime,
          Traced[List[DefaultOpenEnvelope]],
      ) => HandlerResult,
      acsCommitmentProcessor: AcsCommitmentProcessor.ProcessorType,
      requestCounterAllocator: RequestCounterAllocator,
      recordOrderPublisher: RecordOrderPublisher,
      badRootHashMessagesRequestProcessor: BadRootHashMessagesRequestProcessor,
      repairProcessor: RepairProcessor,
      inFlightSubmissionTracker: InFlightSubmissionTracker,
  )

  object Fixture {
    def mk(
        mkMd: (
            ProtocolVersion,
            DomainId,
            ParticipantId,
            RequestTracker,
            RequestProcessors,
            (SequencerCounter, SequencedTime, Traced[List[DefaultOpenEnvelope]]) => HandlerResult,
            AcsCommitmentProcessor.ProcessorType,
            RequestCounterAllocator,
            RecordOrderPublisher,
            BadRootHashMessagesRequestProcessor,
            RepairProcessor,
            InFlightSubmissionTracker,
            NamedLoggerFactory,
            SyncDomainMetrics,
        ) => MessageDispatcher,
        initRc: RequestCounter = RequestCounter(0),
        cleanReplaySequencerCounter: SequencerCounter = SequencerCounter(0),
        badRootHashMessagesRequestProcessorF: => FutureUnlessShutdown[Unit] =
          FutureUnlessShutdown.unit,
        processingRequestHandlerF: => HandlerResult = HandlerResult.done,
        processingResultHandlerF: => HandlerResult = HandlerResult.done,
    ): Fixture = {
      val requestTracker = mock[RequestTracker]

      def mockMethods[VT <: ViewType](processor: RequestProcessor[VT]): Unit = {
        when(
          processor.processRequest(
            any[CantonTimestamp],
            any[RequestCounter],
            any[SequencerCounter],
            any[RequestAndRootHashMessage[OpenEnvelope[EncryptedViewMessage[VT]]]],
          )(anyTraceContext)
        )
          .thenReturn(processingRequestHandlerF)
        when(
          processor.processResult(
            any[Either[
              EventWithErrors[Deliver[DefaultOpenEnvelope]],
              SignedContent[Deliver[DefaultOpenEnvelope]],
            ]]
          )(anyTraceContext)
        )
          .thenReturn(processingResultHandlerF)
        when(
          processor.processMalformedMediatorRequestResult(
            any[CantonTimestamp],
            any[SequencerCounter],
            any[Either[
              EventWithErrors[Deliver[DefaultOpenEnvelope]],
              SignedContent[Deliver[DefaultOpenEnvelope]],
            ]],
          )(anyTraceContext)
        )
          .thenReturn(processingResultHandlerF)
      }

      val testViewProcessor = mock[RequestProcessor[TestViewType]]
      mockMethods(testViewProcessor)

      val otherTestViewProcessor = mock[RequestProcessor[OtherTestViewType]]
      mockMethods(otherTestViewProcessor)

      val identityProcessor =
        mock[
          (SequencerCounter, SequencedTime, Traced[List[DefaultOpenEnvelope]]) => HandlerResult
        ]
      when(
        identityProcessor.apply(
          any[SequencerCounter],
          any[SequencedTime],
          any[Traced[List[DefaultOpenEnvelope]]],
        )
      )
        .thenReturn(HandlerResult.done)

      val acsCommitmentProcessor = mock[AcsCommitmentProcessor.ProcessorType]
      when(
        acsCommitmentProcessor.apply(
          any[CantonTimestamp],
          any[Traced[List[OpenEnvelope[SignedProtocolMessage[AcsCommitment]]]]],
        )
      )
        .thenReturn(FutureUnlessShutdown.unit)

      val requestCounterAllocator =
        new RequestCounterAllocatorImpl(initRc, cleanReplaySequencerCounter, loggerFactory)
      val recordOrderPublisher = mock[RecordOrderPublisher]

      val badRootHashMessagesRequestProcessor = mock[BadRootHashMessagesRequestProcessor]
      when(
        badRootHashMessagesRequestProcessor.sendRejectionAndTerminate(
          any[SequencerCounter],
          any[CantonTimestamp],
          any[RootHash],
          any[MediatorsOfDomain],
          any[LocalReject],
        )(anyTraceContext)
      )
        .thenReturn(badRootHashMessagesRequestProcessorF)

      val repairProcessor = mock[RepairProcessor]

      val inFlightSubmissionTracker = mock[InFlightSubmissionTracker]
      when(
        inFlightSubmissionTracker.observeSequencing(
          any[DomainId],
          any[Map[MessageId, SequencedSubmission]],
        )(anyTraceContext)
      )
        .thenReturn(Future.unit)
      when(inFlightSubmissionTracker.observeDeliverError(any[DeliverError])(anyTraceContext))
        .thenReturn(Future.unit)

      val protocolProcessors = new RequestProcessors {
        override protected def getInternal[P](
            viewType: ViewType { type Processor = P }
        ): Option[P] = viewType match {
          case TestViewType => Some(testViewProcessor)
          case OtherTestViewType => Some(otherTestViewProcessor)
          case _ => None
        }
      }

      val syncDomainMetrics = new SyncDomainMetrics(
        MetricName("test"),
        NoOpMetricsFactory,
      )(MetricsContext.Empty)

      val messageDispatcher = mkMd(
        testedProtocolVersion,
        domainId,
        participantId,
        requestTracker,
        protocolProcessors,
        identityProcessor,
        acsCommitmentProcessor,
        requestCounterAllocator,
        recordOrderPublisher,
        badRootHashMessagesRequestProcessor,
        repairProcessor,
        inFlightSubmissionTracker,
        loggerFactory,
        syncDomainMetrics,
      )

      Fixture(
        messageDispatcher,
        requestTracker,
        testViewProcessor,
        otherTestViewProcessor,
        identityProcessor,
        acsCommitmentProcessor,
        requestCounterAllocator,
        recordOrderPublisher,
        badRootHashMessagesRequestProcessor,
        repairProcessor,
        inFlightSubmissionTracker,
      )
    }
  }

  private def mkDeliver(
      batch: Batch[DefaultOpenEnvelope],
      sc: SequencerCounter = SequencerCounter(0),
      ts: CantonTimestamp = CantonTimestamp.Epoch,
      messageId: Option[MessageId] = None,
  ): Deliver[DefaultOpenEnvelope] =
    Deliver.create(sc, ts, domainId, messageId, batch, testedProtocolVersion)

  private def rootHash(index: Int): RootHash = RootHash(TestHash.digest(index))

  private def signEvent[Env <: Envelope[_]](
      event: SequencedEvent[Env]
  ): SignedContent[SequencedEvent[Env]] =
    SequencerTestUtils.sign(event)

  private val dummySignature = SymbolicCrypto.emptySignature

  private def emptyEncryptedViewTree =
    Encrypted.fromByteString[CompressedView[MockViewTree]](ByteString.EMPTY).value

  private val encryptedTestView = EncryptedView(TestViewType)(emptyEncryptedViewTree)
  private val encryptedTestViewMessage =
    EncryptedViewMessage(
      None,
      ViewHash(TestHash.digest(9000)),
      randomness = encryptedRandomnessTest,
      sessionKey = sessionKeyMapTest,
      encryptedTestView,
      domainId,
      SymmetricKeyScheme.Aes128Gcm,
      testedProtocolVersion,
    )

  private val encryptedOtherTestView = EncryptedView(OtherTestViewType)(emptyEncryptedViewTree)
  private val encryptedOtherTestViewMessage =
    EncryptedViewMessage(
      submittingParticipantSignature = None,
      viewHash = ViewHash(TestHash.digest(9001)),
      randomness = encryptedRandomnessTest,
      sessionKey = sessionKeyMapTest,
      encryptedView = encryptedOtherTestView,
      domainId = domainId,
      viewEncryptionScheme = SymmetricKeyScheme.Aes128Gcm,
      protocolVersion = testedProtocolVersion,
    )

  private val requestId = RequestId(CantonTimestamp.Epoch)
  private val testMediatorResult =
    SignedProtocolMessage.from(
      TestRegularMediatorResult(
        TestViewType,
        domainId,
        Verdict.Approve(testedProtocolVersion),
        requestId,
      ),
      testedProtocolVersion,
      dummySignature,
    )
  private val otherTestMediatorResult =
    SignedProtocolMessage.from(
      TestRegularMediatorResult(
        OtherTestViewType,
        domainId,
        Verdict.Approve(testedProtocolVersion),
        requestId,
      ),
      testedProtocolVersion,
      dummySignature,
    )

  protected def messageDispatcher(
      mkMd: (
          ProtocolVersion,
          DomainId,
          ParticipantId,
          RequestTracker,
          RequestProcessors,
          (SequencerCounter, SequencedTime, Traced[List[DefaultOpenEnvelope]]) => HandlerResult,
          AcsCommitmentProcessor.ProcessorType,
          RequestCounterAllocator,
          RecordOrderPublisher,
          BadRootHashMessagesRequestProcessor,
          RepairProcessor,
          InFlightSubmissionTracker,
          NamedLoggerFactory,
          SyncDomainMetrics,
      ) => MessageDispatcher
  ) = {

    type AnyProcessor = RequestProcessor[_ <: ViewType]
    type ProcessorOfFixture = Fixture => AnyProcessor

    def mk(
        initRc: RequestCounter = RequestCounter(0),
        cleanReplaySequencerCounter: SequencerCounter = SequencerCounter(0),
    ): Fixture =
      Fixture.mk(mkMd, initRc, cleanReplaySequencerCounter)

    val factoryX =
      new TopologyTransactionTestFactoryX(loggerFactory, initEc = executionContext)
    val idTx = TopologyTransactionsBroadcastX.create(
      domainId,
      Seq(
        Broadcast(
          String255.tryCreate("some request"),
          List(factoryX.ns1k1_k1),
        )
      ),
      testedProtocolVersion,
    )

    val rawCommitment = mock[AcsCommitment]
    when(rawCommitment.domainId).thenReturn(domainId)
    when(rawCommitment.representativeProtocolVersion).thenReturn(
      AcsCommitment.protocolVersionRepresentativeFor(testedProtocolVersion)
    )
    when(rawCommitment.pretty).thenReturn(PrettyUtil.prettyOfString(_ => "test"))

    val commitment =
      SignedProtocolMessage.from(rawCommitment, testedProtocolVersion, dummySignature)

    def malformedVerdict(protocolVersion: ProtocolVersion): Verdict.MediatorReject =
      MediatorReject.tryCreate(
        MediatorError.MalformedMessage.Reject("").rpcStatusWithoutLoggingContext(),
        protocolVersion,
      )

    val reject = malformedVerdict(testedProtocolVersion)
    val malformedMediatorRequestResult =
      SignedProtocolMessage.from(
        MalformedMediatorRequestResult.tryCreate(
          RequestId(CantonTimestamp.MinValue),
          domainId,
          TestViewType,
          reject,
          testedProtocolVersion,
        ),
        testedProtocolVersion,
        dummySignature,
      )

    def checkTickTopologyProcessor(
        sut: Fixture,
        sc: SequencerCounter = SequencerCounter(0),
        ts: CantonTimestamp = CantonTimestamp.Epoch,
    ): Assertion = {
      verify(sut.topologyProcessor).apply(
        isEq(sc),
        isEq(SequencedTime(ts)),
        any[Traced[List[DefaultOpenEnvelope]]],
      )
      succeed
    }

    def checkTickRequestTracker(
        sut: Fixture,
        sc: SequencerCounter = SequencerCounter(0),
        ts: CantonTimestamp = CantonTimestamp.Epoch,
    ): Assertion = {
      verify(sut.requestTracker).tick(isEq(sc), isEq(ts))(anyTraceContext)
      succeed
    }

    def checkTickRecordOrderPublisher(
        sut: Fixture,
        sc: SequencerCounter,
        ts: CantonTimestamp,
    ): Assertion = {
      verify(sut.recordOrderPublisher).tick(isEq(sc), isEq(ts))(anyTraceContext)
      succeed
    }

    def checkObserveSequencing(
        sut: Fixture,
        expected: Map[MessageId, SequencedSubmission],
    ): Assertion = {
      verify(sut.inFlightSubmissionTracker).observeSequencing(isEq(domainId), isEq(expected))(
        anyTraceContext
      )
      succeed
    }

    def checkObserveDeliverError(sut: Fixture, expected: DeliverError): Assertion = {
      verify(sut.inFlightSubmissionTracker).observeDeliverError(isEq(expected))(anyTraceContext)
      succeed
    }

    def checkTicks(
        sut: Fixture,
        sc: SequencerCounter = SequencerCounter(0),
        ts: CantonTimestamp = CantonTimestamp.Epoch,
    ): Assertion = {
      checkTickTopologyProcessor(sut, sc, ts)
      checkTickRequestTracker(sut, sc, ts)
      checkTickRecordOrderPublisher(sut, sc, ts)
    }

    def checkProcessRequest[VT <: ViewType](
        processor: RequestProcessor[VT],
        ts: CantonTimestamp,
        rc: RequestCounter,
        sc: SequencerCounter,
    ): Assertion = {
      verify(processor).processRequest(
        isEq(ts),
        isEq(rc),
        isEq(sc),
        any[RequestAndRootHashMessage[OpenEnvelope[EncryptedViewMessage[VT]]]],
      )(anyTraceContext)
      succeed
    }

    def checkNotProcessRequest[VT <: ViewType](processor: RequestProcessor[VT]): Assertion = {
      verify(processor, never).processRequest(
        any[CantonTimestamp],
        any[RequestCounter],
        any[SequencerCounter],
        any[RequestAndRootHashMessage[OpenEnvelope[EncryptedViewMessage[VT]]]],
      )(anyTraceContext)
      succeed
    }

    def checkProcessResult(processor: AnyProcessor): Assertion = {
      verify(processor).processResult(
        any[Either[
          EventWithErrors[Deliver[DefaultOpenEnvelope]],
          SignedContent[Deliver[DefaultOpenEnvelope]],
        ]]
      )(
        anyTraceContext
      )
      succeed
    }

    def signAndTrace(event: RawProtocolEvent): Traced[Seq[Either[Traced[
      EventWithErrors[SequencedEvent[OpenEnvelope[ProtocolMessage]]]
    ], PossiblyIgnoredProtocolEvent]]] =
      Traced(Seq(Right(OrdinarySequencedEvent(signEvent(event), None)(traceContext))))

    def handle(sut: Fixture, event: RawProtocolEvent)(checks: => Assertion): Future[Assertion] = {
      for {
        _ <- sut.messageDispatcher
          .handleAll(signAndTrace(event))
          .onShutdown(fail(s"Encountered shutdown while handling $event"))
        _ <- sut.messageDispatcher.flush()
      } yield {
        checks
      }
    }

    "handling a deliver event" should {
      "call the transaction processor after having informed the identity processor and tick the request tracker" in {
        val sut = mk()
        val sc = SequencerCounter(1)
        val ts = CantonTimestamp.Epoch
        val prefix = TimeProof.timeEventMessageIdPrefix
        val deliver = SequencerTestUtils.mockDeliver(
          sc.v,
          ts,
          domainId,
          messageId = Some(
            MessageId
              .tryCreate(s"$prefix testing")
          ),
        )
        // Check that we're calling the topology manager before we're publishing the deliver event and ticking the
        // request tracker
        when(
          sut.recordOrderPublisher.scheduleEmptyAcsChangePublication(
            any[SequencerCounter],
            any[CantonTimestamp],
          )
        )
          .thenAnswer {
            checkTickTopologyProcessor(sut, sc, ts).discard
          }
        when(sut.requestTracker.tick(any[SequencerCounter], any[CantonTimestamp])(anyTraceContext))
          .thenAnswer {
            checkTickTopologyProcessor(sut, sc, ts).discard
          }

        handle(sut, deliver) {
          verify(sut.recordOrderPublisher).scheduleEmptyAcsChangePublication(isEq(sc), isEq(ts))
          checkTicks(sut, sc, ts)
        }.futureValue
      }
    }

    "topology transactions" should {
      "be passed to the identity processor" in {
        val sut = mk()
        val sc = SequencerCounter(1)
        val ts = CantonTimestamp.ofEpochSecond(1)
        val event =
          mkDeliver(Batch.of(testedProtocolVersion, idTx -> Recipients.cc(participantId)), sc, ts)
        handle(sut, event) {
          checkTicks(sut, sc, ts)
        }.futureValue
      }
    }

    "ACS commitments" should {
      "be passed to the ACS commitment processor" in {
        val sut = mk()
        val sc = SequencerCounter(2)
        val ts = CantonTimestamp.ofEpochSecond(2)
        val event = mkDeliver(
          Batch.of(testedProtocolVersion, commitment -> Recipients.cc(participantId)),
          sc,
          ts,
        )
        handle(sut, event) {
          verify(sut.acsCommitmentProcessor)
            .apply(isEq(ts), any[Traced[List[OpenEnvelope[SignedProtocolMessage[AcsCommitment]]]]])
          checkTicks(sut, sc, ts)
        }
      }.futureValue
    }

    "synchronous shutdown propagates" in {
      val sut = mk()
      val sc = SequencerCounter(3)
      val ts = CantonTimestamp.ofEpochSecond(3)

      // Overwrite the mocked identity processor so that it aborts synchronously
      when(
        sut.topologyProcessor
          .apply(
            any[SequencerCounter],
            any[SequencedTime],
            any[Traced[List[DefaultOpenEnvelope]]],
          )
      )
        .thenReturn(HandlerResult.synchronous(FutureUnlessShutdown.abortedDueToShutdown))
      when(
        sut.acsCommitmentProcessor.apply(
          any[CantonTimestamp],
          any[Traced[List[OpenEnvelope[SignedProtocolMessage[AcsCommitment]]]]],
        )
      )
        .thenReturn(FutureUnlessShutdown.unit)

      val event = mkDeliver(
        Batch.of[ProtocolMessage](testedProtocolVersion, idTx -> Recipients.cc(participantId)),
        sc,
        ts,
      )

      val result = sut.messageDispatcher.handleAll(signAndTrace(event)).unwrap.futureValue
      sut.messageDispatcher.flush().futureValue

      result shouldBe UnlessShutdown.AbortedDueToShutdown
      verify(sut.acsCommitmentProcessor, never)
        .apply(
          any[CantonTimestamp],
          any[Traced[List[OpenEnvelope[SignedProtocolMessage[AcsCommitment]]]]],
        )
      succeed
    }

    "asynchronous shutdown propagates" in {
      val sut = mk()
      val sc = SequencerCounter(3)
      val ts = CantonTimestamp.ofEpochSecond(3)

      // Overwrite the mocked identity processor so that it aborts asynchronously
      when(
        sut.topologyProcessor
          .apply(
            any[SequencerCounter],
            any[SequencedTime],
            any[Traced[List[DefaultOpenEnvelope]]],
          )
      )
        .thenReturn(HandlerResult.asynchronous(FutureUnlessShutdown.abortedDueToShutdown))

      val event = mkDeliver(
        Batch.of[ProtocolMessage](testedProtocolVersion, idTx -> Recipients.cc(participantId)),
        sc,
        ts,
      )

      val result = sut.messageDispatcher.handleAll(signAndTrace(event)).unwrap.futureValue
      sut.messageDispatcher.flush().futureValue
      val abort = result.traverse(_.unwrap).unwrap.futureValue

      abort.flatten shouldBe UnlessShutdown.AbortedDueToShutdown
      // Since the shutdown happened asynchronously, we cannot enforce whether the other handlers are called.

    }

    "complain about unknown view types in a request" in {
      val sut = mk(initRc = RequestCounter(-12))
      val encryptedUnknownTestView = EncryptedView(UnknownTestViewType)(emptyEncryptedViewTree)
      val encryptedUnknownTestViewMessage =
        EncryptedViewMessage(
          None,
          ViewHash(TestHash.digest(9002)),
          randomness = encryptedRandomnessTest,
          sessionKey = sessionKeyMapTest,
          encryptedUnknownTestView,
          domainId,
          SymmetricKeyScheme.Aes128Gcm,
          testedProtocolVersion,
        )
      val rootHashMessage =
        RootHashMessage(
          rootHash(1),
          domainId,
          testedProtocolVersion,
          UnknownTestViewType,
          SerializedRootHashMessagePayload.empty,
        )
      val event = mkDeliver(
        Batch.of[ProtocolMessage](
          testedProtocolVersion,
          encryptedUnknownTestViewMessage -> Recipients.cc(participantId),
          rootHashMessage -> Recipients.cc(MemberRecipient(participantId), mediatorGroup),
        ),
        SequencerCounter(11),
        CantonTimestamp.ofEpochSecond(11),
      )

      val error = loggerFactory
        .assertLogs(
          sut.messageDispatcher.handleAll(signAndTrace(event)).failed,
          loggerFactory.checkLogsInternalError[IllegalArgumentException](
            _.getMessage should include(show"No processor for view type $UnknownTestViewType")
          ),
          _.errorMessage should include("event processing failed."),
        )
        .futureValue

      error shouldBe a[IllegalArgumentException]
      error.getMessage should include(show"No processor for view type $UnknownTestViewType")
    }

    "complain about unknown view types in a result" in {
      val sut = mk(initRc = RequestCounter(-11))
      val unknownTestMediatorResult =
        SignedProtocolMessage.from(
          TestRegularMediatorResult(
            UnknownTestViewType,
            domainId,
            Verdict.Approve(testedProtocolVersion),
            requestId,
          ),
          testedProtocolVersion,
          dummySignature,
        )
      val event =
        mkDeliver(
          Batch.of[ProtocolMessage](
            testedProtocolVersion,
            unknownTestMediatorResult -> Recipients.cc(participantId),
          ),
          SequencerCounter(12),
          CantonTimestamp.ofEpochSecond(11),
        )

      val error = loggerFactory
        .assertLogs(
          sut.messageDispatcher.handleAll(signAndTrace(event)).failed,
          loggerFactory.checkLogsInternalError[IllegalArgumentException](
            _.getMessage should include(show"No processor for view type $UnknownTestViewType")
          ),
          _.errorMessage should include("processing failed"),
        )
        .futureValue

      error shouldBe a[IllegalArgumentException]
      error.getMessage should include(show"No processor for view type $UnknownTestViewType")

    }

    def request(
        view: EncryptedViewMessage[ViewType],
        processor: ProcessorOfFixture,
        wrongView: EncryptedViewMessage[ViewType],
    ): Unit = {
      val viewType = view.viewType
      val wrongViewType = wrongView.viewType

      s"be passed to the $viewType processor" in {
        val initRc = RequestCounter(2)
        val sut = mk(initRc = initRc)
        val sc = SequencerCounter(2)
        val ts = CantonTimestamp.ofEpochSecond(2)
        val rootHashMessage =
          RootHashMessage(
            rootHash(1),
            domainId,
            testedProtocolVersion,
            viewType,
            SerializedRootHashMessagePayload.empty,
          )
        val event =
          mkDeliver(
            Batch.of[ProtocolMessage](
              testedProtocolVersion,
              view -> Recipients.cc(participantId),
              rootHashMessage -> Recipients.cc(MemberRecipient(participantId), mediatorGroup),
            ),
            sc,
            ts,
          )
        handle(sut, event) {
          checkProcessRequest(processor(sut), ts, initRc, sc)
          checkTickTopologyProcessor(sut, sc, ts)
          checkTickRequestTracker(sut, sc, ts)
          sut.requestCounterAllocator.peek shouldBe initRc + 1
        }.futureValue
      }

      "expect a valid root hash message" in {
        val rootHashMessage =
          RootHashMessage(
            rootHash(1),
            domainId,
            testedProtocolVersion,
            viewType,
            SerializedRootHashMessagePayload.empty,
          )
        val otherParticipant = ParticipantId.tryFromProtoPrimitive("PAR::other::participant")
        // Batch -> expected alarms -> expected reaction
        val badBatches = List(
          Batch.of[ProtocolMessage](testedProtocolVersion, view -> Recipients.cc(participantId)) ->
            Seq("No valid root hash message in batch") -> DoNotExpectMediatorResult,
          Batch.of[ProtocolMessage](
            testedProtocolVersion,
            view -> Recipients.cc(participantId),
            rootHashMessage -> Recipients.cc(participantId),
          ) -> Seq(
            "Received root hash messages that were not sent to a mediator",
            "No valid root hash message in batch",
          ) -> DoNotExpectMediatorResult,
          Batch.of[ProtocolMessage](
            testedProtocolVersion,
            view -> Recipients.cc(participantId),
            rootHashMessage -> Recipients
              .cc(MemberRecipient(participantId), MemberRecipient(otherParticipant), mediatorGroup2),
          ) -> Seq(
            "Received root hash message with invalid recipients"
          ) -> ExpectMalformedMediatorRequestResult,
          Batch.of[ProtocolMessage](
            testedProtocolVersion,
            view -> Recipients.cc(participantId),
            rootHashMessage -> Recipients.cc(
              MemberRecipient(participantId),
              MemberRecipient(otherParticipant),
              mediatorGroup2,
            ),
            rootHashMessage -> Recipients.cc(MemberRecipient(participantId), mediatorGroup2),
          ) -> Seq("Multiple root hash messages in batch") -> ExpectMalformedMediatorRequestResult,
          Batch.of[ProtocolMessage](
            testedProtocolVersion,
            view -> Recipients.cc(participantId),
            rootHashMessage
              .copy(viewType = wrongViewType) -> Recipients
              .cc(MemberRecipient(participantId), mediatorGroup),
          ) -> Seq(
            show"Received no encrypted view message of type $wrongViewType",
            show"Expected view type $wrongViewType, but received view types $viewType",
          ) -> SendMalformedAndExpectMediatorResult(
            rootHashMessage.rootHash,
            mediatorGroup,
            show"Received no encrypted view message of type $wrongViewType",
          ),
          Batch.of[ProtocolMessage](
            testedProtocolVersion,
            rootHashMessage -> Recipients.cc(MemberRecipient(participantId), mediatorGroup),
          ) -> Seq(
            show"Received no encrypted view message of type $viewType"
          ) -> SendMalformedAndExpectMediatorResult(
            rootHashMessage.rootHash,
            mediatorGroup,
            show"Received no encrypted view message of type $viewType",
          ),
          Batch.of[ProtocolMessage](
            testedProtocolVersion,
            wrongView -> Recipients.cc(participantId),
            rootHashMessage -> Recipients.cc(MemberRecipient(participantId), mediatorGroup),
          ) -> Seq(
            show"Expected view type $viewType, but received view types $wrongViewType",
            show"Received no encrypted view message of type $viewType",
          ) -> SendMalformedAndExpectMediatorResult(
            rootHashMessage.rootHash,
            mediatorGroup,
            show"Received no encrypted view message of type $viewType",
          ),
        )

        // sequentially process the test cases so that the log messages don't interfere
        forAll(badBatches.zipWithIndex) { case (((batch, alarms), reaction), index) =>
          val initRc = RequestCounter(index)
          val sut = mk(initRc = initRc)
          val sc = SequencerCounter(index)
          val ts = CantonTimestamp.ofEpochSecond(index.toLong)
          withClueF(s"at batch $index:") {
            loggerFactory.assertLogsUnordered(
              handle(sut, mkDeliver(batch, sc, ts)) {
                // never tick the request counter
                sut.requestCounterAllocator.peek shouldBe initRc
                checkNotProcessRequest(processor(sut))
                reaction match {
                  case DoNotExpectMediatorResult => checkTicks(sut, sc, ts)
                  case ExpectMalformedMediatorRequestResult => checkTicks(sut, sc, ts)
                  case SendMalformedAndExpectMediatorResult(rootHash, mediatorId, reason) =>
                    verify(sut.badRootHashMessagesRequestProcessor)
                      .sendRejectionAndTerminate(
                        eqTo(sc),
                        eqTo(ts),
                        eqTo(rootHash),
                        eqTo(mediatorId),
                        eqTo(
                          LocalReject.MalformedRejects.BadRootHashMessages
                            .Reject(reason, testedProtocolVersion)
                        ),
                      )(anyTraceContext)
                    checkTickTopologyProcessor(sut, sc, ts)
                    checkTickRequestTracker(sut, sc, ts)
                }
                succeed
              },
              alarms.map(alarm =>
                (entry: LogEntry) => {
                  entry.shouldBeCantonErrorCode(SyncServiceAlarm)
                  entry.warningMessage should include(alarm)
                }
              )*
            )
          }.futureValue
        }
      }

      "crash upon root hash messages for multiple mediators" in {
        val rootHashMessage =
          RootHashMessage(
            rootHash(1),
            domainId,
            testedProtocolVersion,
            viewType,
            SerializedRootHashMessagePayload.empty,
          )
        val fatalBatches = List(
          Batch.of[ProtocolMessage](
            testedProtocolVersion,
            view -> Recipients.cc(participantId),
            rootHashMessage -> Recipients.cc(MemberRecipient(participantId), mediatorGroup),
            rootHashMessage -> Recipients.cc(MemberRecipient(participantId), mediatorGroup2),
          ),
          Batch.of[ProtocolMessage](
            testedProtocolVersion,
            view -> Recipients.cc(participantId),
            rootHashMessage -> Recipients
              .cc(MemberRecipient(participantId), mediatorGroup, mediatorGroup2),
          ),
          Batch.of[ProtocolMessage](
            testedProtocolVersion,
            view -> Recipients.cc(participantId),
            rootHashMessage -> Recipients.recipientGroups(
              NonEmpty.mk(
                Seq,
                NonEmpty.mk(Set, MemberRecipient(participantId), mediatorGroup),
                NonEmpty.mk(Set, MemberRecipient(participantId), mediatorGroup2),
              )
            ),
          ),
        )

        // sequentially process the test cases so that the log messages don't interfere
        MonadUtil
          .sequentialTraverse_(fatalBatches.zipWithIndex) { case (batch, index) =>
            val initRc = RequestCounter(index.toLong)
            val sut = mk(initRc = initRc)
            val sc = SequencerCounter(index.toLong)
            val ts = CantonTimestamp.ofEpochSecond(index.toLong)
            withClueF(s"at batch $index") {
              loggerFactory.assertThrowsAndLogsAsync[IllegalArgumentException](
                handle(sut, mkDeliver(batch, sc, ts))(succeed),
                _.getMessage should include(
                  "Received batch with encrypted views and root hash messages addressed to multiple mediators"
                ),
                _.errorMessage should include(ErrorUtil.internalErrorMessage),
                _.errorMessage should include("event processing failed."),
              )
            }
          }
          .futureValue
      }

      "not get confused about additional envelopes" in {
        val rootHashMessage =
          RootHashMessage(
            rootHash(1),
            domainId,
            testedProtocolVersion,
            viewType,
            SerializedRootHashMessagePayload.empty,
          )
        val badBatches = List(
          Batch.of[ProtocolMessage](
            testedProtocolVersion,
            view -> Recipients.cc(participantId),
            rootHashMessage -> Recipients.cc(participantId),
            rootHashMessage -> Recipients.cc(MemberRecipient(participantId), mediatorGroup),
          ) -> Seq("Received root hash messages that were not sent to a mediator"),
          Batch.of[ProtocolMessage](
            testedProtocolVersion,
            view -> Recipients.cc(participantId),
            rootHashMessage -> Recipients.cc(MemberRecipient(participantId), mediatorGroup),
            commitment -> Recipients.cc(participantId),
            // We used to include a DomainTopologyTransactionMessage which no longer exist in 3.x
          ) -> Seq(),
          Batch.of[ProtocolMessage](
            testedProtocolVersion,
            view -> Recipients.cc(participantId),
            rootHashMessage -> Recipients.cc(MemberRecipient(participantId), mediatorGroup),
            wrongView -> Recipients.cc(participantId),
          ) -> Seq(show"Expected view type $viewType, but received view types $wrongViewType"),
          Batch.of[ProtocolMessage](
            testedProtocolVersion,
            view -> Recipients.cc(participantId),
            rootHashMessage -> Recipients.cc(MemberRecipient(participantId), mediatorGroup),
            malformedMediatorRequestResult -> Recipients.cc(participantId),
          ) -> Seq(
            show"Received unexpected $MalformedMediatorRequestMessage for ${malformedMediatorRequestResult.message.requestId}"
          ),
        )

        // sequentially process the test cases so that the log messages don't interfere
        MonadUtil
          .sequentialTraverse_(badBatches.zipWithIndex) { case ((batch, alarms), index) =>
            val initRc = RequestCounter(index)
            val sut = mk(initRc = initRc)
            val sc = SequencerCounter(index)
            val ts = CantonTimestamp.ofEpochSecond(index.toLong)
            withClueF(s"at batch $index") {
              loggerFactory.assertLogsUnordered(
                handle(sut, mkDeliver(batch, sc, ts)) {
                  checkProcessRequest(processor(sut), ts, initRc, sc)
                  checkTickTopologyProcessor(sut, sc, ts)
                  checkTickRequestTracker(sut, sc, ts)
                  // do tick the request counter
                  sut.requestCounterAllocator.peek shouldBe initRc + 1
                },
                alarms.map(alarm =>
                  (entry: LogEntry) => {
                    entry.shouldBeCantonErrorCode(SyncServiceAlarm)
                    entry.warningMessage should include(alarm)
                  }
                )*
              )
            }
          }
          .futureValue
      }

      "be skipped if they precede the clean replay starting point" in {
        val initRc = RequestCounter(2)
        val initSc = SequencerCounter(50)
        val sut = mk(initRc = initRc, cleanReplaySequencerCounter = initSc)
        val sc = initSc - 1L
        val ts = CantonTimestamp.ofEpochSecond(2)
        val rootHashMessage =
          RootHashMessage(
            rootHash(1),
            domainId,
            testedProtocolVersion,
            viewType,
            SerializedRootHashMessagePayload.empty,
          )
        val event =
          mkDeliver(
            Batch.of[ProtocolMessage](
              testedProtocolVersion,
              view -> Recipients.cc(participantId),
              rootHashMessage -> Recipients.cc(MemberRecipient(participantId), mediatorGroup),
            ),
            sc,
            ts,
          )
        handle(sut, event) {
          checkNotProcessRequest(processor(sut))
          checkTickTopologyProcessor(sut, sc, ts)
          checkTickRequestTracker(sut, sc, ts)
          sut.requestCounterAllocator.peek shouldBe initRc
        }.futureValue
      }
    }

    "Test requests" should {
      behave like request(encryptedTestViewMessage, _.testProcessor, encryptedOtherTestViewMessage)
    }

    "Other test requests" should {
      behave like request(
        encryptedOtherTestViewMessage,
        _.otherTestProcessor,
        encryptedTestViewMessage,
      )
    }

    "Mediator results" should {
      "be sent to the right processor" in {
        def check(result: ProtocolMessage, processor: ProcessorOfFixture): Future[Assertion] = {
          val sut = mk()
          val batch = Batch.of(testedProtocolVersion, result -> Recipients.cc(participantId))
          handle(sut, mkDeliver(batch)) {
            checkTickTopologyProcessor(sut)
            checkTickRequestTracker(sut)
            checkProcessResult(processor(sut))
          }
        }

        (for {
          _ <- check(testMediatorResult, _.testProcessor)
          _ <- check(otherTestMediatorResult, _.otherTestProcessor)
        } yield succeed).futureValue
      }

      "come one at a time" in {
        val batch = Batch.of[ProtocolMessage](
          testedProtocolVersion,
          testMediatorResult -> Recipients.cc(participantId),
          otherTestMediatorResult -> Recipients.cc(participantId),
        )
        val sut = mk()
        loggerFactory
          .assertLogsUnordered(
            handle(sut, mkDeliver(batch)) {
              checkTicks(sut)
            },
            _.warningMessage should include(
              show"Received unexpected ${RequestKind(TestViewType)} for $requestId"
            ),
            _.warningMessage should include(
              show"Received unexpected ${RequestKind(OtherTestViewType)} for $requestId"
            ),
          )
          .futureValue
      }

      "malformed mediator requests be sent to the right processor" in {
        def malformed(viewType: ViewType, processor: ProcessorOfFixture): Future[Assertion] = {
          val result =
            SignedProtocolMessage.from(
              MalformedMediatorRequestResult.tryCreate(
                RequestId(CantonTimestamp.MinValue),
                domainId,
                viewType,
                reject,
                testedProtocolVersion,
              ),
              testedProtocolVersion,
              dummySignature,
            )
          val batch = Batch.of(testedProtocolVersion, result -> Recipients.cc(participantId))
          val sut = mk()
          withClueF(show"for $viewType") {
            handle(sut, mkDeliver(batch)) {
              verify(processor(sut)).processMalformedMediatorRequestResult(
                isEq(CantonTimestamp.Epoch),
                isEq(SequencerCounter(0)),
                any[Either[
                  EventWithErrors[Deliver[DefaultOpenEnvelope]],
                  SignedContent[Deliver[DefaultOpenEnvelope]],
                ]],
              )(anyTraceContext)
              checkTickTopologyProcessor(sut)
              checkTickRequestTracker(sut)
            }
          }
        }

        (for {
          _ <- malformed(TestViewType, _.testProcessor)
          _ <- malformed(OtherTestViewType, _.otherTestProcessor)
        } yield succeed).futureValue
      }
    }

    "receipts and deliver errors" should {
      "trigger in-flight submission tracking" in {
        val sut = mk()
        val messageId1 = MessageId.fromUuid(new UUID(0, 1))
        val messageId2 = MessageId.fromUuid(new UUID(0, 2))
        val messageId3 = MessageId.fromUuid(new UUID(0, 3))

        val dummyBatch = Batch.of(
          testedProtocolVersion,
          malformedMediatorRequestResult -> Recipients.cc(participantId),
        )
        val deliver1 =
          mkDeliver(dummyBatch, SequencerCounter(0), CantonTimestamp.Epoch, messageId1.some)
        val deliver2 = mkDeliver(
          dummyBatch,
          SequencerCounter(1),
          CantonTimestamp.ofEpochSecond(1),
          messageId2.some,
        )
        val deliver3 = mkDeliver(dummyBatch, SequencerCounter(2), CantonTimestamp.ofEpochSecond(2))
        val deliverError4 = DeliverError.create(
          SequencerCounter(3),
          CantonTimestamp.ofEpochSecond(3),
          domainId,
          messageId3,
          SequencerErrors.SubmissionRequestMalformed("invalid batch"),
          testedProtocolVersion,
        )

        val sequencedEvents = Seq(deliver1, deliver2, deliver3, deliverError4).map(event =>
          Right(OrdinarySequencedEvent(signEvent(event), None)(traceContext))
        )

        sut.messageDispatcher
          .handleAll(Traced(sequencedEvents))
          .onShutdown(fail("Encountered shutdown while handling batch of sequenced events"))
          .futureValue
          .discard

        checkObserveSequencing(
          sut,
          Map(
            messageId1 -> SequencedSubmission(SequencerCounter(0), CantonTimestamp.Epoch),
            messageId2 -> SequencedSubmission(
              SequencerCounter(1),
              CantonTimestamp.ofEpochSecond(1),
            ),
          ),
        )
        checkObserveDeliverError(sut, deliverError4)

      }

      "handle duplicate messageIds properly" in {
        val sut = mk()
        val messageId1 = MessageId.fromUuid(new UUID(0, 1))
        val messageId2 = MessageId.fromUuid(new UUID(0, 2))

        val dummyBatch = Batch.of(
          testedProtocolVersion,
          malformedMediatorRequestResult -> Recipients.cc(participantId),
        )
        val deliver1 = mkDeliver(
          dummyBatch,
          SequencerCounter(0),
          CantonTimestamp.Epoch,
          messageId1.some,
        )
        val deliver2 = mkDeliver(
          dummyBatch,
          SequencerCounter(1),
          CantonTimestamp.ofEpochSecond(1),
          messageId2.some,
        )

        // Same messageId as `deliver1` but sequenced later
        val deliver3 = mkDeliver(
          dummyBatch,
          SequencerCounter(2),
          CantonTimestamp.ofEpochSecond(2),
          messageId1.some,
        )

        val sequencedEvents = Seq(deliver1, deliver2, deliver3).map(event =>
          Right(OrdinarySequencedEvent(signEvent(event), None)(traceContext))
        )

        loggerFactory
          .assertLogs(
            sut.messageDispatcher
              .handleAll(Traced(sequencedEvents))
              .onShutdown(fail("Encountered shutdown while handling batch of sequenced events")),
            _.warningMessage should include("Ignoring duplicate"),
          )
          .futureValue
          .discard

        checkObserveSequencing(
          sut,
          Map(
            messageId1 -> SequencedSubmission(
              SequencerCounter(0),
              CantonTimestamp.Epoch,
            ),
            messageId2 -> SequencedSubmission(
              SequencerCounter(1),
              CantonTimestamp.ofEpochSecond(1),
            ),
          ),
        )

      }
    }
  }
}

private[protocol] object MessageDispatcherTest {

  final case class DisabledTransferTestData[A <: ViewType](
      inOut: String,
      viewType: ViewType.TransferViewType,
      view: EncryptedView[A],
  )

  // The message dispatcher only sees encrypted view trees, so there's no point in implementing the methods.
  sealed trait MockViewTree extends ViewTree with HasVersionedToByteString

  trait AbstractTestViewType extends ViewTypeTest {
    override type View = MockViewTree

    override def toProtoEnum: protocolv30.ViewType =
      throw new UnsupportedOperationException(
        s"${this.getClass.getSimpleName} cannot be serialized"
      )
  }

  case object TestViewType extends AbstractTestViewType
  type TestViewType = TestViewType.type

  case object OtherTestViewType extends AbstractTestViewType
  type OtherTestViewType = OtherTestViewType.type

  case object UnknownTestViewType extends AbstractTestViewType
  type UnknownTestViewType = OtherTestViewType.type

  final case class TestRegularMediatorResult(
      override val viewType: ViewType,
      override val domainId: DomainId,
      override val verdict: Verdict,
      override val requestId: RequestId,
  ) extends RegularMediatorResult {
    def representativeProtocolVersion
        : RepresentativeProtocolVersion[TestRegularMediatorResult.type] =
      TestRegularMediatorResult.protocolVersionRepresentativeFor(
        BaseTest.testedProtocolVersion
      )

    override def toProtoTypedSomeSignedProtocolMessage
        : protocolv30.TypedSignedProtocolMessageContent.SomeSignedProtocolMessage =
      throw new UnsupportedOperationException(
        s"${this.getClass.getSimpleName} cannot be serialized"
      )

    override def deserializedFrom: Option[ByteString] = None
    override protected[this] def toByteStringUnmemoized: ByteString = ByteString.EMPTY

    override protected val companionObj: TestRegularMediatorResult.type = TestRegularMediatorResult
  }

  object TestRegularMediatorResult
      extends HasProtocolVersionedWrapperCompanion[TestRegularMediatorResult, Nothing] {
    override type Deserializer = Unit
    val name: String = "TestRegularMediatorResult"

    val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
      ProtoVersion(30) -> UnsupportedProtoCodec(ProtocolVersion.v30)
    )

    override protected def deserializationErrorK(error: ProtoDeserializationError): Unit = ()
  }
}

class DefaultMessageDispatcherTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutorService
    with MessageDispatcherTest {

  "DefaultMessageDispatcher" should {
    behave like messageDispatcher(MessageDispatcher.DefaultFactory.create)
  }
}
