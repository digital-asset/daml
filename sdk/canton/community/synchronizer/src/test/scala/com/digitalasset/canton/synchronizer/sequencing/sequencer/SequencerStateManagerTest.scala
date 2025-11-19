// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{
  BatchingConfig,
  CachingConfigs,
  DefaultProcessingTimeouts,
  TopologyConfig,
}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown}
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.protocol.TestSynchronizerParameters
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.synchronizer.HasTopologyTransactionTestFactory
import com.digitalasset.canton.synchronizer.block.*
import com.digitalasset.canton.synchronizer.block.LedgerBlockEvent.*
import com.digitalasset.canton.synchronizer.block.data.memory.InMemorySequencerBlockStore
import com.digitalasset.canton.synchronizer.block.update.BlockUpdateGeneratorImpl
import com.digitalasset.canton.synchronizer.metrics.{SequencerMetrics, SequencerTestMetrics}
import com.digitalasset.canton.synchronizer.sequencer.Sequencer.SignedSubmissionRequest
import com.digitalasset.canton.synchronizer.sequencer.block.BlockSequencerFactory.OrderingTimeFixMode
import com.digitalasset.canton.synchronizer.sequencer.config.SequencerNodeParameterConfig
import com.digitalasset.canton.synchronizer.sequencer.errors.SequencerError.{
  InvalidAcknowledgementSignature,
  InvalidAcknowledgementTimestamp,
}
import com.digitalasset.canton.synchronizer.sequencer.store.{
  InMemorySequencerStore,
  SequencerMemberValidator,
}
import com.digitalasset.canton.synchronizer.sequencer.{
  BlockSequencerStreamInstrumentationConfig,
  SequencerInitialState,
  SequencerIntegration,
  SequencerPruningStatus,
  SequencerSnapshot,
}
import com.digitalasset.canton.synchronizer.sequencing.traffic.RateLimitManagerTesting
import com.digitalasset.canton.synchronizer.sequencing.traffic.store.memory.InMemoryTrafficConsumedStore
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.StoreBasedSynchronizerTopologyClient
import com.digitalasset.canton.topology.processing.{ApproximateTime, EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.TopologyStoreId.SynchronizerStore
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.topology.store.{NoPackageDependencies, ValidatedTopologyTransaction}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.{NoReportingTracerProvider, TraceContext, Traced}
import com.digitalasset.canton.util.PekkoUtil
import com.digitalasset.canton.{
  BaseTest,
  HasExecutionContext,
  HasExecutorService,
  ProtocolVersionChecksAsyncWordSpec,
  config,
}
import org.apache.pekko.Done
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Keep, Sink, Source}
import org.scalatest.wordspec.AsyncWordSpec

import java.util.concurrent.atomic.AtomicReference
import scala.annotation.unused
import scala.concurrent.duration.*
import scala.concurrent.{Await, Future}
import scala.util.Try

// TODO(#21094): Review this test for: ensure that test cases are covered by other tests of unified/db sequencer, then delete this test
class SequencerStateManagerTest
    extends AsyncWordSpec
    with BaseTest
    with ProtocolVersionChecksAsyncWordSpec
    with HasExecutorService
    with HasExecutionContext
    with HasTopologyTransactionTestFactory
    with RateLimitManagerTesting {

  private val suppressingLoggerF = loggerFactory
  private val initialHeight = SequencerDriver.DefaultInitialBlockHeight + 1

  private lazy val alice = topologyTransactionFactory.participant1
  private lazy val bob = ParticipantId(
    UniqueIdentifier.tryCreate("bob", Namespace(SigningKeys.key4.id))
  )
  private lazy val synchronizerId = DefaultTestIdentities.physicalSynchronizerId
  private lazy val sequencer = DefaultTestIdentities.sequencerId
  private val ts2 = ts1.plusSeconds(5)
  private val ts3 = ts2.plusSeconds(1)
  private val ts4 = ts3.plusSeconds(1)
  private val ts5 = ts4.plusSeconds(1)
  private val ts6 = ts5.plusSeconds(1)
  private val ts7 = ts6.plusSeconds(1)
  @unused
  private val ts8 = ts7.plusSeconds(1)
  private val ts100 = ts0.plusSeconds(100)
  private val ts101 = ts0.plusSeconds(101)

  private lazy val aggregationRule1 =
    AggregationRule(NonEmpty(Seq, alice, bob), PositiveInt.tryCreate(2), testedProtocolVersion)

  private def aggregationRequestFor(
      sender: Member,
      rule: AggregationRule,
      maxSequencingTime: CantonTimestamp = ts100,
  ): Future[SignedSubmissionRequest] =
    senderSignedSubmissionRequest(
      sender,
      Recipients.cc(alice),
      topologyTimestamp = Some(ts1),
      maxSequencingTime = maxSequencingTime,
      aggregationRule = Some(rule),
    )

  private lazy val aggregationRequest1Bob = aggregationRequestFor(bob, aggregationRule1).futureValue
  @unused
  private lazy val aggregationId1 =
    aggregationRequest1Bob.content
      .aggregationId(topologyTransactionFactory.syncCryptoClient.crypto.pureCrypto)
      .value

  private lazy val aggregationRule2 =
    AggregationRule(NonEmpty(Seq, alice), PositiveInt.one, testedProtocolVersion)
  private lazy val aggregationRequest2 = aggregationRequestFor(alice, aggregationRule2).futureValue
  @unused
  private lazy val aggregationId2 =
    aggregationRequest2.content
      .aggregationId(topologyTransactionFactory.syncCryptoClient.crypto.pureCrypto)
      .value

  private lazy val aggregationRequest3Alice =
    aggregationRequestFor(alice, aggregationRule1, ts101).futureValue
  @unused
  private lazy val aggregationId3 =
    aggregationRequest3Alice.content
      .aggregationId(topologyTransactionFactory.syncCryptoClient.crypto.pureCrypto)
      .value

  private def withEnv[T](
      initial: Option[SequencerInitialState] = None
  )(test: Environment => Future[T]): Future[T] = {
    val env = new Environment(initial)
    complete {
      test(env)
    } lastly env.close()
  }

  private def newTimestamp() = CantonTimestamp.now()

  val testedUseUnifiedSequencer = true

  "SequencerStateManager" can {

    "process blocks" should {
      "fail if getting blocks out of order" onlyRunWhen (!testedUseUnifiedSequencer) in withEnv() {
        implicit env =>
          import env.*

          for {
            submissionReq1 <- senderSignedSubmissionRequest(alice)
            _ = handleBlock(initialHeight, Send(newTimestamp(), submissionReq1, sequencer))
            _ <- suppressingLoggerF.assertThrowsAndLogsAsync[SequencerUnexpectedStateChange](
              {
                handleBlock(initialHeight + 2)
                processingTermination
              },
              _ => succeed,
              _.errorMessage should include(
                s"Received block of height ${initialHeight + 2} while the last processed block only had height $initialHeight"
              ),
            )
          } yield succeed
      }

      "allow first block ever received to have an arbitrary initial height" onlyRunWhen (!testedUseUnifiedSequencer) in withEnv() {
        implicit env =>
          import env.*
          handleBlock(initialHeight + 2)
          succeed
      }
    }

    "acknowledge" should {
      val sequencerInitialStateAtT1 =
        SequencerInitialState(
          synchronizerId,
          SequencerSnapshot(
            ts1,
            UninitializedBlockHeight,
            Map.empty,
            SequencerPruningStatus(CantonTimestamp.Epoch, ts1, Set.empty),
            Map.empty,
            None,
            testedProtocolVersion,
            Seq.empty,
            Seq.empty,
          ),
          None,
          None,
        )

      "resolve all earlier valid acknowledgements" onlyRunWhen (!testedUseUnifiedSequencer) in withEnv(
        Some(sequencerInitialStateAtT1)
      ) { implicit env =>
        import env.*
        for {
          submissionReq1 <- senderSignedSubmissionRequest(alice)
          _ = handleBlock(initialHeight, Send(newTimestamp(), submissionReq1, sequencer))
          wait1F = stateManager.waitForAcknowledgementToComplete(
            alice,
            CantonTimestamp.MinValue.immediateSuccessor,
          )
          ts1 = stateManager.getHeadState.block.lastTs.immediatePredecessor
          ack <- signedAcknowledgement(alice, ts1)
          wait2F = stateManager.waitForAcknowledgementToComplete(alice, ts1)
          _ = handleBlock(initialHeight + 1, ack)
          _ <- wait2F
          _ <- wait1F
        } yield succeed
      }

      "error on acknowledgements with timestamps in the future" onlyRunWhen (!testedUseUnifiedSequencer) in withEnv(
        Some(sequencerInitialStateAtT1)
      ) { implicit env =>
        import env.*
        for {
          submissionReq1 <- senderSignedSubmissionRequest(alice)
          _ = handleBlock(initialHeight, Send(newTimestamp(), submissionReq1, sequencer))
          ts1 = stateManager.getHeadState.block.lastTs
          ts0 = ts1.immediatePredecessor
          ts2 = ts1.immediateSuccessor
          wait0F = stateManager.waitForAcknowledgementToComplete(alice, ts0)
          wait1F = stateManager.waitForAcknowledgementToComplete(alice, ts1)
          wait2F = stateManager.waitForAcknowledgementToComplete(alice, ts2)
          signedAck0 <- signedAcknowledgement(alice, ts0)
          signedAck2 <- signedAcknowledgement(alice, ts2)
          _ = loggerFactory.assertLogs(
            handleBlock(initialHeight + 1, signedAck0, signedAck2),
            _.warningMessage should include(InvalidAcknowledgementTimestamp.code.id),
          )
          _ <- wait0F
          err <- wait2F.failed
          _ = wait1F.isCompleted shouldBe false
        } yield {
          err.getMessage should include(LogEntry.SECURITY_SENSITIVE_MESSAGE_ON_API)
        }
      }

      "error on acknowledgements with invalid signatures" onlyRunWhen (!testedUseUnifiedSequencer) in withEnv(
        Some(sequencerInitialStateAtT1)
      ) { implicit env =>
        import env.*
        for {
          submissionReq1 <- senderSignedSubmissionRequest(alice)
          _ = handleBlock(initialHeight, Send(newTimestamp(), submissionReq1, sequencer))
          ts1 = stateManager.getHeadState.block.lastTs
          ts0 = ts1.immediatePredecessor
          wait0F = stateManager.waitForAcknowledgementToComplete(alice, ts0)
          wait1F = stateManager.waitForAcknowledgementToComplete(alice, ts1)
          signedAck0 <- signedAcknowledgement(alice, ts0)
          signedAck1 = badSigAcknowledgement(alice, ts1)
          _ = loggerFactory.assertLogs(
            handleBlock(initialHeight + 1, signedAck0, signedAck1),
            _.warningMessage should include(InvalidAcknowledgementSignature.code.id),
          )
          _ <- wait0F
          err <- wait1F.failed
        } yield err.getMessage should include(LogEntry.SECURITY_SENSITIVE_MESSAGE_ON_API)
      }
    }

  }

  private class Environment(
      initial: Option[SequencerInitialState] = None
  ) extends AutoCloseable {
    private val actorSystem = ActorSystem()
    implicit val materializer: Materializer = Materializer(actorSystem)

    val synchronizerId: PhysicalSynchronizerId =
      topologyTransactionFactory.synchronizerId1.toPhysical
    val sequencer1: SequencerId = topologyTransactionFactory.sequencer1
    val topologyStore =
      new InMemoryTopologyStore(
        SynchronizerStore(synchronizerId),
        testedProtocolVersion,
        loggerFactory,
        timeouts,
      )

    val initialSynchronizerParametersMapping =
      SynchronizerParametersState(synchronizerId.logical, TestSynchronizerParameters.defaultDynamic)

    topologyStore
      .update(
        SequencedTime(CantonTimestamp.Epoch),
        EffectiveTime(CantonTimestamp.Epoch),
        removals = Map.empty,
        additions = Seq(
          topologyTransactionFactory.ns1k1_k1,
          topologyTransactionFactory.okmS1k7_k1,
          topologyTransactionFactory.dtcp1_k1,
          topologyTransactionFactory.mkAdd(
            initialSynchronizerParametersMapping,
            topologyTransactionFactory.SigningKeys.key1,
          )(executorService),
          topologyTransactionFactory.okm1bk5k1E_k1, // this one to allow verification of the sender's signature
        ).map(ValidatedTopologyTransaction(_, rejectionReason = None)),
      )
      .futureValueUS

    val topologyClient = new StoreBasedSynchronizerTopologyClient(
      mock[Clock],
      defaultStaticSynchronizerParameters,
      topologyStore,
      NoPackageDependencies,
      topologyConfig = TopologyConfig(),
      DefaultProcessingTimeouts.testing,
      FutureSupervisor.Noop,
      loggerFactory,
    )
    topologyClient.updateHead(
      SequencedTime(CantonTimestamp.Epoch),
      EffectiveTime(CantonTimestamp.Epoch),
      ApproximateTime(CantonTimestamp.Epoch),
    )
    private val cryptoApi = SynchronizerCryptoClient.create(
      member = sequencer1,
      synchronizerId.logical,
      topologyClient,
      defaultStaticSynchronizerParameters,
      topologyTransactionFactory.syncCryptoClient.crypto,
      BatchingConfig().parallelism,
      CachingConfigs.defaultPublicKeyConversionCache,
      DefaultProcessingTimeouts.testing,
      FutureSupervisor.Noop,
      loggerFactory,
    )

    private val store = new InMemorySequencerBlockStore(
      new InMemorySequencerStore(
        testedProtocolVersion,
        sequencer1,
        blockSequencerMode = true,
        loggerFactory,
        sequencerMetrics = SequencerMetrics.noop("sequencer-state-manager-test"),
      ),
      loggerFactory,
    )
    private val trafficConsumedStore = new InMemoryTrafficConsumedStore(loggerFactory)
    config
      .NonNegativeFiniteDuration(1.second)
      .awaitUS("set initial state")(
        initial.fold(FutureUnlessShutdown.unit)(store.setInitialState(_, None))
      )

    val closeContext = CloseContext(cryptoApi)
    private val updateGenerator = new BlockUpdateGeneratorImpl(
      testedProtocolVersion,
      cryptoApi,
      sequencer1,
      defaultRateLimiter,
      orderingTimeFixMode = OrderingTimeFixMode.MakeStrictlyIncreasing,
      sequencingTimeLowerBoundExclusive =
        SequencerNodeParameterConfig.DefaultSequencingTimeLowerBoundExclusive,
      useTimeProofsToObserveEffectiveTime = true,
      SequencerTestMetrics,
      loggerFactory,
      memberValidator = new SequencerMemberValidator {
        override def isMemberRegisteredAt(member: Member, time: CantonTimestamp)(implicit
            tc: TraceContext
        ): FutureUnlessShutdown[Boolean] = ???
      },
    )(closeContext, NoReportingTracerProvider.tracer)

    def signedAcknowledgement(
        member: Member,
        timestamp: CantonTimestamp,
    ): Future[Acknowledgment] = {
      val ack = AcknowledgeRequest(member, timestamp, testedProtocolVersion)
      val hash =
        topologyTransactionFactory.syncCryptoClient.crypto.pureCrypto.digest(
          HashPurpose.AcknowledgementSignature,
          ack.getCryptographicEvidence,
        )
      topologyTransactionFactory.syncCryptoClient.crypto.privateCrypto
        .sign(
          hash,
          participant1Key.fingerprint,
          SigningKeyUsage.ProtocolOnly,
        )
        .map(signature =>
          SignedContent(
            ack,
            signature,
            Some(ts1),
            testedProtocolVersion,
          )
        )
        .leftMap(_.toString)
        .value
        .failOnShutdown
        .map(_.value)
        .map(Acknowledgment(newTimestamp(), _))
    }

    def badSigAcknowledgement(member: Member, timestamp: CantonTimestamp): Acknowledgment =
      Acknowledgment(
        newTimestamp(),
        SignedContent(
          AcknowledgeRequest(member, timestamp, testedProtocolVersion),
          Signature.noSignature,
          None,
          testedProtocolVersion,
        ),
      )

    val stateManager: BlockSequencerStateManager =
      BlockSequencerStateManager
        .create(
          topologyTransactionFactory.synchronizerId1.toPhysical,
          store,
          trafficConsumedStore,
          asyncWriterParameters = AsyncWriterParameters(enabled = true),
          enableInvariantCheck = true,
          timeouts,
          futureSupervisor,
          loggerFactory,
          BlockSequencerStreamInstrumentationConfig(),
          SequencerTestMetrics.block,
        )(executorService, traceContext)
        .onShutdown(fail("Shutting down"))

    private val processingTimestampWatermark =
      new AtomicReference[CantonTimestamp](CantonTimestamp.MinValue)
    val (processingQueue, processingDone) =
      PekkoUtil.runSupervised(
        Source
          .queue[Traced[BlockEvents]](10)
          .via(stateManager.processBlock(updateGenerator))
          .via(stateManager.applyBlockUpdate(SequencerIntegration.Noop))
          .map(ts => processingTimestampWatermark.updateAndGet(_ max ts.value))
          .toMat(Sink.ignore)(Keep.both),
        errorLogMessagePrefix = "",
      )
    def processingTermination: Future[Done] = processingDone

    def handleBlock(height: Long, events: LedgerBlockEvent*): Unit = {
      logger.info(s"Processing block $height")
      val maxTs = events
        .collect { case Send(ts, _, _, _) => ts }
        .maxOption
        .getOrElse(processingTimestampWatermark.get())
      processingQueue.offer(
        Traced(
          BlockEvents(
            height,
            CantonTimestamp.Epoch, // Not relevant for the test
            events.map(Traced(_)),
          )
        )
      )
      eventually() {
        processingTimestampWatermark.get() should be >= maxTs
      }
    }

    override def close(): Unit = {
      // If the processing pipeline fails, we'll get an exception here that we don't care about.
      Try {
        processingQueue.complete()
        processingDone.futureValue
      }
      stateManager.close()
      topologyClient.close()
      topologyStore.close()
      materializer.shutdown()
      Await.result(actorSystem.terminate(), 10.seconds)
    }
  }
}
