// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.block

import cats.data.{Chain, EitherT}
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.crypto.TestHash
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.lifecycle.{
  FutureUnlessShutdown,
  PromiseUnlessShutdown,
  UnlessShutdown,
}
import com.digitalasset.canton.sequencing.protocol.{AggregationId, AggregationRule}
import com.digitalasset.canton.sequencing.traffic.TrafficConsumed
import com.digitalasset.canton.synchronizer.block.data.{BlockInfo, SequencerBlockStore}
import com.digitalasset.canton.synchronizer.sequencer.InFlightAggregation.AggregationBySender
import com.digitalasset.canton.synchronizer.sequencer.{
  AggregatedSender,
  FreshInFlightAggregation,
  InFlightAggregationUpdate,
  InFlightAggregationUpdates,
}
import com.digitalasset.canton.synchronizer.sequencing.traffic.store.TrafficConsumedStore
import com.digitalasset.canton.topology.{DefaultTestIdentities, Member}
import com.digitalasset.canton.version.{HasTestCloseContext, ProtocolVersion}
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.FixtureAsyncWordSpecLike
import org.scalatest.{Assertion, FutureOutcome}

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.Future
import scala.concurrent.duration.*

private[block] final case class WriteQueue[T](
    nextWrite: PromiseUnlessShutdown[Unit],
    writeReturn: Seq[FutureUnlessShutdown[Unit]],
    written: Seq[T],
)
object WriteQueue {
  def empty[T]: WriteQueue[T] =
    WriteQueue(PromiseUnlessShutdown.unsupervised(), Seq.empty, Seq.empty)
}

class BlockSequencerStateAsyncWriterTest
    extends FixtureAsyncWordSpecLike
    with BaseTest
    with HasTestCloseContext
    with HasExecutionContext {

  private[block] class Fixture {

    val inflightAggregations =
      new AtomicReference[WriteQueue[InFlightAggregationUpdates]](WriteQueue.empty)
    val blockInfos =
      new AtomicReference[WriteQueue[Seq[BlockInfo]]](WriteQueue.empty)

    def recordWrite[T](
        ref: AtomicReference[WriteQueue[T]],
        item: T,
    ): FutureUnlessShutdown[Unit] = {
      val queue = ref.getAndUpdate { queue =>
        WriteQueue(
          PromiseUnlessShutdown.unsupervised[Unit](),
          queue.writeReturn.drop(1),
          queue.written :+ item,
        )
      }
      queue.nextWrite.success(UnlessShutdown.unit)
      queue.writeReturn.headOption.getOrElse(FutureUnlessShutdown.unit)
    }

    val blockStore = {
      val m = mock[SequencerBlockStore]
      when(m.storeInflightAggregations(any[InFlightAggregationUpdates])(anyTraceContext))
        .thenAnswer[InFlightAggregationUpdates] { x =>
          logger.debug(s"Record in flight aggregation $x")
          recordWrite(inflightAggregations, x)
        }
      when(m.finalizeBlockUpdates(any[Seq[BlockInfo]])(anyTraceContext))
        .thenAnswer[Seq[BlockInfo]] { x =>
          logger.debug(s"Record block infos $x")
          recordWrite(blockInfos, x)
        }
      m
    }

    val trafficConsumed =
      new AtomicReference[WriteQueue[Seq[TrafficConsumed]]](
        WriteQueue.empty
      )
    val trafficStore = {
      val m = mock[TrafficConsumedStore]
      when(m.store(any[Seq[TrafficConsumed]])(anyTraceContext))
        .thenAnswer[Seq[TrafficConsumed]] { x =>
          logger.debug(s"Record traffic consumed $x")
          recordWrite(trafficConsumed, x)
        }
      m
    }

    val writer = new BlockSequencerStateAsyncWriter(
      blockStore,
      trafficStore,
      futureSupervisor,
      AsyncWriterParameters(
        trafficBatchSize = PositiveInt.two,
        aggregationBatchSize = PositiveInt.two,
        blockInfoBatchSize = PositiveInt.two,
      ),
      loggerFactory,
    )

  }

  override protected type FixtureParam = Fixture

  override def withFixture(test: OneArgAsyncTest): FutureOutcome = {
    val env = new Fixture()
    super.withFixture(test.toNoArgAsyncTest(env))
  }

  private lazy val member = DefaultTestIdentities.participant1
  private lazy val tc1 = TrafficConsumed(
    member,
    CantonTimestamp.Epoch,
    NonNegativeLong.one,
    NonNegativeLong.one,
    NonNegativeLong.one,
  )
  private lazy val tc2 = tc1.copy(sequencingTimestamp = CantonTimestamp.Epoch.plusSeconds(1))
  private lazy val tc3 = tc1.copy(sequencingTimestamp = CantonTimestamp.Epoch.plusSeconds(2))

  private lazy val aggId1 = AggregationId(TestHash.digest(1))
  private lazy val aggId2 = AggregationId(TestHash.digest(2))
  private lazy val sender1 =
    AggregatedSender(
      DefaultTestIdentities.participant2,
      AggregationBySender(CantonTimestamp.Epoch, Seq.empty),
    )
  private lazy val sender2 = sender1.copy(sender = DefaultTestIdentities.participant3)
  private lazy val fresh =
    FreshInFlightAggregation(
      CantonTimestamp.Epoch,
      AggregationRule(
        eligibleMembers = NonEmpty.mk(Seq, member): NonEmpty[Seq[Member]],
        threshold = PositiveInt.one,
        protocolVersion = ProtocolVersion.latest,
      ),
    )
  private lazy val agg1 = InFlightAggregationUpdate(
    Some(fresh),
    Chain.one(sender1),
  )

  private lazy val agg2 = InFlightAggregationUpdate(
    Some(fresh),
    Chain.one(sender2),
  )

  private lazy val block1 =
    BlockInfo(10L, CantonTimestamp.Epoch, Some(CantonTimestamp.Epoch), Some(CantonTimestamp.Epoch))
  private def unwrap(t: EitherT[FutureUnlessShutdown, String, Assertion]): Future[Assertion] =
    t.failOnShutdown.value.map(_.valueOrFail("EitherT returned left"))

  private def syncWrite[T, Q](ref: AtomicReference[WriteQueue[Q]])(
      fus: => EitherT[FutureUnlessShutdown, String, T]
  ): EitherT[FutureUnlessShutdown, String, T] = {
    val promise = ref.get().nextWrite
    fus.flatMap { res =>
      EitherT.right(promise.futureUS).map(_ => res)
    }
  }

  "BlockSequencerStateAsyncWriter" should {
    "queue on concurrent" in { fixture =>
      import fixture.*
      val trafficWriteP = PromiseUnlessShutdown.unsupervised[Unit]()
      trafficConsumed.updateAndGet(_.copy(writeReturn = Seq(trafficWriteP.futureUS)))

      unwrap(for {
        // first, we fill both queues => writes should start a write
        _ <- syncWrite(trafficConsumed)(
          writer.append(Seq(tc1), Map(), EitherT.pure(()))
        )
        _ = {
          // check that writes have started
          trafficConsumed.get().written shouldBe Seq(Seq(tc1))
        }
        _ <- EitherT.right(writer.finalizeBlockUpate(block1))
        _ = {
          // check that block update is not written yet (because aggregation write is in progress)
          blockInfos.get().written shouldBe empty
        }
        // next item should be queued
        _ <- writer.append(Seq(tc2), Map(), EitherT.pure(()))
        _ = always(200.millis) {
          // item has not been written yet as it is queued, so result should remain unchanged
          trafficConsumed.get().written shouldBe Seq(Seq(tc1))
          // and the block update is still not written
          blockInfos.get().written shouldBe empty
        }
        // now, we complete the first write, this should now start the second write and trigger
        // the flush of the block update
        blockInfoFU = blockInfos.get().nextWrite
        _ = {
          logger.debug("Completing first traffic write")
          trafficWriteP.success(UnlessShutdown.unit)
          logger.debug("Waiting for pickup")
        }
        _ <- EitherT.right(blockInfoFU.futureUS)
        _ = {
          trafficConsumed.get().written shouldBe Seq(Seq(tc1), Seq(tc2))
          blockInfos.get().written shouldBe Seq(Seq(block1))
        }
      } yield {
        succeed
      })
    }
    "backpressure on full queue" in { fixture =>
      import fixture.*
      val trafficWriteP = PromiseUnlessShutdown.unsupervised[Unit]()
      trafficConsumed.updateAndGet(_.copy(writeReturn = Seq(trafficWriteP.futureUS))).discard
      unwrap(for {
        // first, we fill both queues => writes should start a write
        _ <- syncWrite(trafficConsumed)(writer.append(Seq(tc1), Map(), EitherT.pure(())))
        _ <- writer.append(Seq(tc2), Map(), EitherT.pure(()))
        bP = writer.append(Seq(tc3), Map(), EitherT.pure(()))
        _ = {
          Threading.sleep(100)
          // this append should be backpressured
          bP.value.isCompleted shouldBe false
          // check that writes have started
          trafficConsumed.get().written shouldBe Seq(Seq(tc1))
        }
        _ = {
          // after completing writing, the next queue should be picked up and the backpressure
          // should be released
          trafficWriteP.success(UnlessShutdown.unit)
        }
        // now wait until the queue was written and the backpressure released
        _ = eventually() {
          bP.value.isCompleted shouldBe true
          trafficConsumed.get().written shouldBe Seq(Seq(tc1), Seq(tc2, tc3))
        }

      } yield succeed)
    }

    "aggregation caches are managed correctly" in { fixture =>
      import fixture.*
      // note, we test here the aggregation caching and merging logic which is a bit
      // more complicated
      val aggregationP = PromiseUnlessShutdown.unsupervised[Unit]()
      inflightAggregations.updateAndGet(_.copy(writeReturn = Seq(aggregationP.futureUS)))
      unwrap(for {
        // first write will start immediately, rest will be queued
        _ <- writer.append(Seq.empty, Map(aggId1 -> agg1), EitherT.pure(()))
        // adding first aggregation to the queue
        _ = writer.append(Seq.empty, Map(aggId2 -> agg1, aggId1 -> agg1), EitherT.pure(()))
        // adding second aggregation to the queue
        _ = writer.append(Seq.empty, Map(aggId2 -> agg2), EitherT.pure(()))
      } yield {
        // flushing first write
        aggregationP.success(UnlessShutdown.unit)
        eventually() {
          val ret = inflightAggregations.get().written
          ret should have length (2)
          ret(0) shouldBe Map(aggId1 -> agg1)
          ret(1) shouldBe Map(aggId2 -> agg1.tryMerge(agg2), aggId1 -> agg1)
        }
        succeed
      })
    }

    "escalate errors" in { fixture =>
      import fixture.*
      val trafficWriteP = PromiseUnlessShutdown.unsupervised[Unit]()
      val boooh = new Exception("booh")
      trafficConsumed.updateAndGet(_.copy(writeReturn = Seq(trafficWriteP.futureUS))).discard
      unwrap(for {
        _ <- syncWrite(trafficConsumed)(writer.append(Seq(tc1), Map(), EitherT.pure(())))
        _ = loggerFactory.assertLogs(
          {
            // now, we fail the write in the background
            trafficWriteP.failure(boooh)
            eventually() {
              // once the error is registered, every future will fail
              val ret = writer.append(Seq(tc2), Map(), EitherT.pure(()))
              // the test is racy as the append might pick up the exception or end up with the backpressureF,
              // waiting for the next write to complete. If the error is already registered,
              // then the future will complete immediately.
              ret.value.isCompleted shouldBe true
              ret.failOnShutdown.value.failed.futureValue.getCause shouldBe boooh
            }
          },
          _.errorMessage should include("Background write failed"),
        )
        _ = {}
      } yield {
        succeed
      })
    }

  }

}
