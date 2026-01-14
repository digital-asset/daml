// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.synchronizer.block.BlockFormat
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.admin.SequencerBftAdminData.WriteReadiness
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.UnitTestContext.DelayCount
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.mempool.{
  MempoolModule,
  MempoolModuleConfig,
  MempoolState,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.OrderingRequest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.Membership
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.{
  Availability,
  Mempool,
  SequencerNode,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{Env, ModuleRef}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.{
  BftSequencerBaseTest,
  fakeCellModule,
  fakeModuleExpectingSilence,
}
import com.digitalasset.canton.tracing.{NoReportingTracerProvider, TraceContext, Traced}
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.FiniteDuration

class MempoolModuleTest extends AnyWordSpec with BftSequencerBaseTest {

  import MempoolModuleTest.*

  private val AnOrderRequest = Mempool.OrderRequest(
    Traced(OrderingRequest(BlockFormat.SendTag, messageId = "", ByteString.copyFromUtf8("b")))
  )

  private val requestRefusedHandler = Some(new ModuleRef[SequencerNode.Message] {
    override def asyncSend(msg: SequencerNode.Message)(implicit
        traceContext: TraceContext,
        metricsContext: MetricsContext,
    ): Unit =
      msg match {
        case SequencerNode.RequestAccepted => fail("the request should fail")
        case _ => ()
      }
  })

  private implicit val unitTestContext: UnitTestEnv#ActorContextT[Mempool.Message] =
    new UnitTestContextWithTraceContext()

  "the mempool module" when {

    "the queue is full" should {
      "refuse the request" in {
        val mempool =
          createMempool[UnitTestEnv](fakeModuleExpectingSilence, maxMempoolQueueSize = 0)
        mempool.receiveInternal(
          Mempool.OrderRequest(
            Traced(OrderingRequest(BlockFormat.SendTag, messageId = "", ByteString.EMPTY)),
            requestRefusedHandler,
          )
        )
        succeed
      }
    }

    "the request payload size is too big" should {
      "refuse the request" in {
        val mempool =
          createMempool[UnitTestEnv](fakeModuleExpectingSilence, maxRequestPayloadBytes = 0)
        suppressProblemLogs(
          mempool.receiveInternal(
            Mempool.OrderRequest(
              Traced(
                OrderingRequest(BlockFormat.SendTag, messageId = "", ByteString.copyFromUtf8("c"))
              ),
              requestRefusedHandler,
            )
          )
        )
        succeed
      }
    }

    "the request tag it invalid" should {
      "refuse the request" in {
        val mempool =
          createMempool[UnitTestEnv](fakeModuleExpectingSilence, maxRequestPayloadBytes = 0)
        suppressProblemLogs(
          mempool.receiveInternal(
            Mempool.OrderRequest(
              Traced(OrderingRequest("invalidTag", messageId = "", ByteString.copyFromUtf8("c"))),
              requestRefusedHandler,
            )
          )
        )
        succeed
      }
    }

    "there is no P2P-connected dissemination quorum" should {
      "refuse the request" in {
        val mempoolState = createMempoolState()

        val mempool =
          createMempool[UnitTestEnv](fakeModuleExpectingSilence, mempoolState = mempoolState)
        mempool.receiveInternal(AP2PConnectivityUpdate)

        mempool.receiveInternal(
          Mempool.OrderRequest(
            Traced(
              OrderingRequest(BlockFormat.SendTag, messageId = "", ByteString.copyFromUtf8("c"))
            ),
            requestRefusedHandler,
          )
        )
        succeed
      }
    }

    "a batch request is received but there are no client requests" should {
      "remember it" in {
        val mempoolState = createMempoolState()
        val mempool =
          createMempool[UnitTestEnv](fakeModuleExpectingSilence, mempoolState = mempoolState)
        mempool.receiveInternal(Mempool.CreateLocalBatches(1))
        mempoolState.toBeProvidedToAvailability shouldBe 1
      }

      "remember multiple" in {
        val mempoolState = createMempoolState()
        val mempool =
          createMempool[UnitTestEnv](fakeModuleExpectingSilence, mempoolState = mempoolState)
        mempool.receiveInternal(Mempool.CreateLocalBatches(3))
        mempoolState.toBeProvidedToAvailability shouldBe 3
      }

      "always overwrite previous request" in {
        val mempoolState = createMempoolState()
        val mempool =
          createMempool[UnitTestEnv](fakeModuleExpectingSilence, mempoolState = mempoolState)
        mempool.receiveInternal(Mempool.CreateLocalBatches(3))
        mempoolState.toBeProvidedToAvailability shouldBe 3

        mempool.receiveInternal(Mempool.CreateLocalBatches(4))
        mempoolState.toBeProvidedToAvailability shouldBe 4
      }
    }

    "a client request is received but there are no batch requests" should {
      "wait" in {
        val mempoolState = createMempoolState()
        val mempool =
          createMempool[UnitTestEnv](fakeModuleExpectingSilence, mempoolState = mempoolState)
        sendRequest(mempool)
        mempoolState.receivedOrderRequests.map(_._1) should contain only AnOrderRequest
      }
    }

    "both a batch request and a client request have been received" should {
      "produce and provide a new batch if availability requests after request is available" in {
        val batchCreatedCell =
          new AtomicReference[Option[Availability.LocalDissemination.LocalBatchCreated]](None)
        val mempoolState = createMempoolState()
        val mempool = createMempool[UnitTestEnv](
          availability = fakeCellModule[Availability.Message[
            UnitTestEnv
          ], Availability.LocalDissemination.LocalBatchCreated](batchCreatedCell),
          mempoolState = mempoolState,
        )

        sendRequest(mempool)
        mempool.receiveInternal(Mempool.CreateLocalBatches(1))

        val batchCreated = batchCreatedCell
          .get()
          .getOrElse(fail("No batch sent"))
        batchCreated.requests.size should be(1)
        mempoolState.receivedOrderRequests shouldBe empty
        mempoolState.toBeProvidedToAvailability shouldBe 0
      }

      "produce and provide a new batch when reaching min requests in a batch if answering queued request" in {
        val minRequestsInBatch = 2

        val batchCreatedCell =
          new AtomicReference[Option[Availability.LocalDissemination.LocalBatchCreated]](None)
        val mempoolState = createMempoolState()
        val mempool = createMempool[UnitTestEnv](
          availability = fakeCellModule[Availability.Message[
            UnitTestEnv
          ], Availability.LocalDissemination.LocalBatchCreated](batchCreatedCell),
          mempoolState = mempoolState,
          minRequestsInBatch = minRequestsInBatch.toShort,
        )

        mempool.receiveInternal(Mempool.CreateLocalBatches(1))
        mempoolState.toBeProvidedToAvailability shouldBe 1

        sendRequest(mempool)
        batchCreatedCell.get() shouldBe empty
        mempoolState.receivedOrderRequests.size shouldBe 1

        sendRequest(mempool)
        val batchCreated = batchCreatedCell
          .get()
          .getOrElse(fail("No batch sent"))
        batchCreated.requests.size should be(2)
        mempoolState.receivedOrderRequests shouldBe empty
        mempoolState.toBeProvidedToAvailability shouldBe 0
      }

      "produce and provide a new batch when receiving batch creation clock tick if answering queued request" in {
        val minRequestsInBatch = 2

        val batchCreatedCell =
          new AtomicReference[Option[Availability.LocalDissemination.LocalBatchCreated]](None)
        val mempoolState = createMempoolState()

        val timerCell = new AtomicReference[Option[(DelayCount, Mempool.Message)]](None)
        implicit val timeCellContext
            : FakeTimerCellUnitTestContextWithTraceContext[Mempool.Message] =
          new FakeTimerCellUnitTestContextWithTraceContext(timerCell)

        val mempool = createMempool[FakeTimerCellUnitTestEnv](
          availability = fakeCellModule[Availability.Message[
            FakeTimerCellUnitTestEnv
          ], Availability.LocalDissemination.LocalBatchCreated](batchCreatedCell),
          mempoolState = mempoolState,
          minRequestsInBatch = minRequestsInBatch.toShort,
        )

        mempool.receiveInternal(Mempool.Start)
        // initial tick is scheduled
        timerCell.get() should contain((1, Mempool.MempoolBatchCreationClockTick))

        mempool.receiveInternal(Mempool.CreateLocalBatches(1))
        mempoolState.toBeProvidedToAvailability shouldBe 1

        mempool.receiveInternal(AnOrderRequest)
        batchCreatedCell.get() shouldBe empty
        mempoolState.receivedOrderRequests.size shouldBe 1

        // when tick is received, a batch is created no matter if minRequestsInBatch has been reached
        mempool.receiveInternal(Mempool.MempoolBatchCreationClockTick)
        val batchCreated = batchCreatedCell
          .get()
          .getOrElse(fail("No batch sent"))
        batchCreated.requests.size should be(1)
        mempoolState.receivedOrderRequests shouldBe empty
        mempoolState.toBeProvidedToAvailability shouldBe 0

        // another tick gets scheduled
        timerCell.get() should contain((2, Mempool.MempoolBatchCreationClockTick))
      }
    }
  }

  "there are more requests than the max size of a batch request" should {
    "produce and provide a new batch respecting the max size" in {
      val batchCreatedCell =
        new AtomicReference[Option[Availability.LocalDissemination.LocalBatchCreated]](
          None
        )
      val availability =
        fakeCellModule[Availability.Message[
          UnitTestEnv
        ], Availability.LocalDissemination.LocalBatchCreated](
          batchCreatedCell
        )
      val mempoolState = createMempoolState()
      val mempool =
        createMempool(availability, maxRequestsInBatch = 1, mempoolState = mempoolState)

      sendRequest(mempool)
      sendRequest(mempool)
      mempool.receiveInternal(Mempool.CreateLocalBatches(1))

      val batchCreated = batchCreatedCell
        .get()
        .getOrElse(fail("No batch sent"))
      batchCreated.requests.size should be(1)
      mempoolState.receivedOrderRequests.map(_._1) should contain only AnOrderRequest
      mempoolState.toBeProvidedToAvailability shouldBe 0
    }
  }

  "there are less requests than the max size of a batch request" should {
    "produce and provide a new batch of less than the max size" in {
      val batchCreatedCell =
        new AtomicReference[Option[Availability.LocalDissemination.LocalBatchCreated]](
          None
        )
      val availability =
        fakeCellModule[Availability.Message[
          UnitTestEnv
        ], Availability.LocalDissemination.LocalBatchCreated](
          batchCreatedCell
        )
      val mempoolState = createMempoolState()
      val mempool =
        createMempool(availability, maxRequestsInBatch = 2, mempoolState = mempoolState)

      sendRequest(mempool)
      mempool.receiveInternal(Mempool.CreateLocalBatches(1))

      val batchCreated = batchCreatedCell
        .get()
        .getOrElse(fail("No batch sent"))
      batchCreated.requests.size should be(1)
      mempoolState.receivedOrderRequests shouldBe empty
      mempoolState.toBeProvidedToAvailability shouldBe 0
    }
  }

  "it receives a P2P connectivity update" should {
    "update its state" in {
      val mempoolState = createMempoolState()
      val mempool =
        createMempool[UnitTestEnv](fakeModuleExpectingSilence, mempoolState = mempoolState)
      mempool.receiveInternal(AP2PConnectivityUpdate)

      import mempoolState.*

      weakQuorum shouldBe 3
      authenticatedCount shouldBe 2
      canDisseminate shouldBe false
    }
  }

  "it receives a send service readiness inquiry" should {
    "reply with the current readiness" in {
      val mempoolState = createMempoolState()
      val mempool =
        createMempool[UnitTestEnv](fakeModuleExpectingSilence, mempoolState = mempoolState)
      mempool.receiveInternal(AP2PConnectivityUpdate)

      val readinessCell = new AtomicReference[Option[WriteReadiness]](None)
      mempool.receiveInternal(
        Mempool.Admin.GetWriteReadiness(readiness => readinessCell.set(Some(readiness)))
      )

      {
        val readiness = readinessCell
          .get()
          .getOrElse(fail("No readiness reply received"))
        readiness shouldBe WriteReadiness.P2PNotReady(
          WriteReadiness.P2P(authenticatedPeersCount = 2, requiredQuorum = 3)
        )
      }

      mempool.receiveInternal(AnotherP2PConnectivityUpdate)
      mempool.receiveInternal(
        Mempool.Admin.GetWriteReadiness(readiness => readinessCell.set(Some(readiness)))
      )

      {
        val readiness = readinessCell
          .get()
          .getOrElse(fail("No readiness reply received"))
        readiness shouldBe WriteReadiness.Ready(
          WriteReadiness.P2P(authenticatedPeersCount = 3, requiredQuorum = 3)
        )
      }
    }
  }

  private def createMempool[E <: Env[E]](
      availability: ModuleRef[Availability.Message[E]],
      mempoolState: MempoolState = createMempoolState(),
      maxMempoolQueueSize: Int = BftBlockOrdererConfig.DefaultMaxMempoolQueueSize,
      maxRequestPayloadBytes: Int = BftBlockOrdererConfig.DefaultMaxRequestPayloadBytes,
      maxRequestsInBatch: Short = BftBlockOrdererConfig.DefaultMaxRequestsInBatch,
      minRequestsInBatch: Short = BftBlockOrdererConfig.DefaultMinRequestsInBatch,
      maxBatchCreationInterval: FiniteDuration =
        BftBlockOrdererConfig.DefaultMaxBatchCreationInterval,
  ): MempoolModule[E] = {
    val config = MempoolModuleConfig(
      maxMempoolQueueSize,
      maxRequestPayloadBytes,
      maxRequestsInBatch,
      minRequestsInBatch,
      maxBatchCreationInterval,
    )
    val mempool = new MempoolModule[E](
      config,
      mempoolState,
      SequencerMetrics.noop(getClass.getSimpleName).bftOrdering,
      availability,
      loggerFactory,
      timeouts,
    )(MetricsContext.Empty, NoReportingTracerProvider.tracer)
    mempool
  }

  private def createMempoolState(): MempoolState =
    new MempoolState(weakQuorumSize = 1)

  private def sendRequest(mempool: MempoolModule[UnitTestEnv]): Unit =
    mempool.receiveInternal(AnOrderRequest)
}

private object MempoolModuleTest {

  val AP2PConnectivityUpdate =
    Mempool.P2PConnectivityUpdate(
      Membership.forTesting(BftNodeId("myself"), (1 to 7).map(i => BftNodeId(i.toString)).toSet),
      2,
    )
  val AnotherP2PConnectivityUpdate =
    Mempool.P2PConnectivityUpdate(
      Membership.forTesting(BftNodeId("myself"), (1 to 7).map(i => BftNodeId(i.toString)).toSet),
      3,
    )
}
