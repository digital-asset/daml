// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import cats.data.EitherT
import cats.syntax.option.*
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.resource.{MemoryStorage, Storage}
import com.digitalasset.canton.sequencing.protocol.{
  Batch,
  MessageId,
  SequencerErrors,
  SubmissionRequest,
}
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.SequencerWriter.RunningSequencerWriterFlow
import com.digitalasset.canton.synchronizer.sequencer.store.{
  InMemorySequencerStore,
  SequencerStore,
  SequencerWriterStore,
}
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.topology.{
  DefaultTestIdentities,
  Member,
  SequencerId,
  UniqueIdentifier,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil
import org.scalatest.Assertion
import org.scalatest.wordspec.AsyncWordSpec

import java.util.UUID
import scala.collection.mutable
import scala.concurrent.{Future, Promise}

class SequencerWriterTest extends AsyncWordSpec with BaseTest {
  def ts(epochSeconds: Int): CantonTimestamp =
    CantonTimestamp.Epoch.plusSeconds(epochSeconds.toLong)

  class MockRunningWriterFlow {
    val doneP = Promise[Unit]()
    val writerFlow = {
      val queues = mock[SequencerWriterQueues]
      new RunningSequencerWriterFlow(queues, doneP.future)
    }
  }

  class MockWriterStoreFactory() extends SequencerWriterStoreFactory {
    override def close(): Unit = ()

    override def create(storage: Storage, generalStore: SequencerStore)(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, WriterStartupError, SequencerWriterStore] =
      EitherT.pure(
        SequencerWriterStore.singleInstance(generalStore)
      )
  }

  def withEnv(blockSequencerMode: Boolean)(f: Env => Future[Assertion]): Future[Assertion] =
    f(new Env(blockSequencerMode))

  class Env(blockSequencerMode: Boolean) {
    val clock = new SimClock(loggerFactory = loggerFactory)
    val runningFlows = mutable.Buffer[MockRunningWriterFlow]()
    val storage = new MemoryStorage(loggerFactory, timeouts)
    val sequencerMember: Member = SequencerId(
      UniqueIdentifier.tryFromProtoPrimitive("sequencer::namespace")
    )

    val store = new InMemorySequencerStore(
      protocolVersion = testedProtocolVersion,
      sequencerMember = sequencerMember,
      blockSequencerMode = blockSequencerMode,
      loggerFactory = loggerFactory,
      sequencerMetrics = SequencerMetrics.noop("sequencer-writer-test"),
    )
    val instanceIndex = 0
    val storageFactory = new MockWriterStoreFactory()

    val writer =
      new SequencerWriter(
        writerStoreFactory = storageFactory,
        writerFlowFactory = TestSequencerWriterFlowFactory,
        storage = storage,
        rateLimitManagerO = None,
        generalStore = store,
        clock = clock,
        expectedCommitMode = CommitMode.Default.some,
        blockSequencerMode = blockSequencerMode,
        timeouts = timeouts,
        loggerFactory = loggerFactory,
      )
    def numberOfFlowsCreated: Int = runningFlows.size

    def latestRunningWriterFlowPromise: Promise[Unit] =
      runningFlows.lastOption
        .getOrElse(fail("there is no latest running writer flow"))
        .doneP

    /** Any futures enqueued on the sequential scalatest execution context before queuing this will
      * be completed before this one is (doesn't matter that we're not doing anything).
      */
    def allowScheduledFuturesToComplete: Future[Unit] = Future.unit

    object TestSequencerWriterFlowFactory extends SequencerWriter.SequencerWriterFlowFactory {
      override def create(
          store: SequencerWriterStore
      )(implicit traceContext: TraceContext): RunningSequencerWriterFlow = {
        val mockFlow = new MockRunningWriterFlow
        runningFlows.append(mockFlow)
        mockFlow.writerFlow
      }
    }
  }

  "starting" should {
    "wait for online timestamp to be reached" in withEnv(blockSequencerMode = false) { env =>
      import env.*

      val targetTime = clock.now.plusSeconds(10)
      val startET = writer
        .start(
          None,
          SequencerWriter.ResetWatermarkToTimestamp(targetTime),
        )
        .failOnShutdown

      for {
        _ <- allowScheduledFuturesToComplete
        _ = writer.isRunning shouldBe false
        _ <- allowScheduledFuturesToComplete
        _ = writer.isRunning shouldBe false
        _ = startET.value.isCompleted shouldBe false
        _ <- valueOrFail(startET)("Starting Sequencer Writer")
        _ = clock.now should be >= targetTime
      } yield writer.isRunning shouldBe true
    }

    "not wait for online timestamp to be reached for block sequencers" in withEnv(
      blockSequencerMode = true
    ) { env =>
      import env.*

      val targetTime = clock.now.plusSeconds(60)
      val startET = writer
        .start(
          None,
          SequencerWriter.ResetWatermarkToTimestamp(targetTime),
        )
        .failOnShutdown

      for {
        _ <- allowScheduledFuturesToComplete
        _ = writer.isRunning shouldBe false
        _ <- allowScheduledFuturesToComplete
        _ = writer.isRunning shouldBe false
        _ = startET.value.isCompleted shouldBe false
        _ <- valueOrFail(startET)("Starting Sequencer Writer")
        _ = clock.now should be < targetTime
      } yield writer.isRunning shouldBe true
    }

  }

  "getting knocked offline" should {
    "run recovery and restart" in withEnv(blockSequencerMode = false) { env =>
      import env.*

      val mockSubmissionRequest = SubmissionRequest.tryCreate(
        DefaultTestIdentities.participant1,
        MessageId.fromUuid(new UUID(1L, 1L)),
        Batch.empty(testedProtocolVersion),
        maxSequencingTime = CantonTimestamp.MaxValue,
        topologyTimestamp = None,
        aggregationRule = None,
        submissionCost = None,
        testedProtocolVersion,
      )

      for {
        _ <- valueOrFail(writer.start(None, SequencerWriter.ResetWatermarkToClockNow))(
          "Starting writer"
        ).failOnShutdown
        _ = writer.isRunning shouldBe true

        // have the writer flow blow up with an exception saying we've been knocked offline
        _ = latestRunningWriterFlowPromise.failure(new SequencerOfflineException(42))
        _ <- allowScheduledFuturesToComplete
        _ = writer.isRunning shouldBe false
        // attempting to write at this point should return unavailable errors that will eventually be used to signal to the
        // load balancers
        sendError <- leftOrFail(writer.send(mockSubmissionRequest))(
          "send when unavailable"
        ).failOnShutdown
        _ = sendError.code.id shouldBe SequencerErrors.Unavailable.id
        // there may be a number of future hops to work its way through completing the second flow which we currently
        // can't capture via flushes, so just check it eventually happens
        _ <- MonadUtil.sequentialTraverse(0 until 10)(_ => allowScheduledFuturesToComplete)
        _ = writer.isRunning shouldBe true
      } yield {
        numberOfFlowsCreated shouldBe 2
        writer.isRunning shouldBe true
      }
    }
  }
}
