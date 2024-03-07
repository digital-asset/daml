// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import cats.data.EitherT
import cats.syntax.option.*
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.sequencer.store.{
  InMemorySequencerStore,
  SequencerStore,
  SequencerWriterStore,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.resource.{MemoryStorage, Storage}
import com.digitalasset.canton.sequencing.protocol.{
  Batch,
  MessageId,
  SendAsyncError,
  SubmissionRequest,
}
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.topology.DefaultTestIdentities
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil
import org.mockito.ArgumentMatchers
import org.scalatest.FutureOutcome
import org.scalatest.wordspec.FixtureAsyncWordSpec

import java.util.UUID
import scala.collection.mutable
import scala.concurrent.{Future, Promise}

class SequencerWriterTest extends FixtureAsyncWordSpec with BaseTest {
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

  class Env {
    val clock = new SimClock(loggerFactory = loggerFactory)
    val runningFlows = mutable.Buffer[MockRunningWriterFlow]()
    val storage = new MemoryStorage(loggerFactory, timeouts)
    val store = new InMemorySequencerStore(testedProtocolVersion, loggerFactory)
    val storeSpy = spy(store)
    val instanceIndex = 0
    val storageFactory = new MockWriterStoreFactory()

    val writer =
      new SequencerWriter(
        storageFactory,
        createWriterFlow,
        storage,
        clock,
        CommitMode.Default.some,
        timeouts,
        loggerFactory,
        // Unused because the store is overridden below
        testedProtocolVersion,
        PositiveInt.tryCreate(5),
      ) {
        override val generalStore: SequencerStore = store
      }

    def setupNextGoOnlineTimestamp(ts: CantonTimestamp): Unit =
      when(
        storeSpy.goOnline(ArgumentMatchers.eq(instanceIndex), any[CantonTimestamp])(anyTraceContext)
      )
        .thenReturn(Future.successful(ts))
    def numberOfFlowsCreated: Int = runningFlows.size

    def latestRunningWriterFlowPromise: Promise[Unit] =
      runningFlows.lastOption
        .getOrElse(fail("there is no latest running writer flow"))
        .doneP

    /** Any futures enqueued on the sequential scalatest execution context before queuing this will be completed
      * before this one is (doesn't matter that we're not doing anything).
      */
    def allowScheduledFuturesToComplete: Future[Unit] = Future.unit

    def close(): Unit = ()

    private def createWriterFlow(
        store: SequencerWriterStore,
        traceContext: TraceContext,
    ): RunningSequencerWriterFlow = {
      val mockFlow = new MockRunningWriterFlow
      runningFlows.append(mockFlow)
      mockFlow.writerFlow
    }
  }

  override type FixtureParam = Env

  override def withFixture(test: OneArgAsyncTest): FutureOutcome = {
    val env = new Env

    complete {
      withFixture(test.toNoArgAsyncTest(env))
    } lastly {
      env.close()
    }
  }

  "starting" should {
    "wait for online timestamp to be reached" in { env =>
      import env.*

      // set our time to ts2 but the returned goOnline timestamp to ts4
      clock.advanceTo(ts(3))
      setupNextGoOnlineTimestamp(ts(4))

      val startET = writer.start()

      for {
        _ <- allowScheduledFuturesToComplete
        _ = writer.isRunning shouldBe false
        _ = clock.advanceTo(ts(3)) // still not reached our online time
        _ <- allowScheduledFuturesToComplete
        _ = writer.isRunning shouldBe false
        _ = startET.value.isCompleted shouldBe false
        // finally reach our online timestamp
        _ = clock.advanceTo(ts(4))
        _ <- valueOrFail(startET)("Starting Sequencer Writer")
      } yield writer.isRunning shouldBe true
    }
  }

  "getting knocked offline" should {
    "run recovery and restart" in { env =>
      import env.*

      val mockSubmissionRequest = SubmissionRequest.tryCreate(
        DefaultTestIdentities.participant1,
        MessageId.fromUuid(new UUID(1L, 1L)),
        isRequest = false,
        Batch.empty(testedProtocolVersion),
        maxSequencingTime = CantonTimestamp.MaxValue,
        topologyTimestamp = None,
        aggregationRule = None,
        testedProtocolVersion,
      )

      for {
        _ <- valueOrFail(writer.start())("Starting writer")
        _ = writer.isRunning shouldBe true
        // set the next goOffline timestamp to way in the future to delay the recovery so we can run assertions
        _ = setupNextGoOnlineTimestamp(ts(10))
        // have the writer flow blow up with an exception saying we've been knocked offline
        _ = latestRunningWriterFlowPromise.failure(new SequencerOfflineException(42))
        _ <- allowScheduledFuturesToComplete
        _ = writer.isRunning shouldBe false
        // attempting to write at this point should return unavailable errors that will eventually be used to signal to the
        // load balancers
        sendError <- leftOrFail(writer.send(mockSubmissionRequest))("send when unavailable")
        _ = sendError shouldBe SendAsyncError.Unavailable("Unavailable")
        // now progress to allow crash recovery to complete
        _ = clock.advanceTo(ts(10))
        // there may be a number of future hops to work its way through completing the second flow which we currently
        // can't capture via flushes, so just check it eventually happens
        _ <- MonadUtil.sequentialTraverse(0 until 10)(_ => allowScheduledFuturesToComplete)
      } yield {
        numberOfFlowsCreated shouldBe 2
        writer.isRunning shouldBe true
      }
    }
  }
}
