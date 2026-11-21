// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.data

import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.TestHash
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.synchronizer.block.data.{
  BlockEphemeralState,
  BlockInfo,
  SequencerBlockStore,
}
import com.digitalasset.canton.synchronizer.sequencer.*
import com.digitalasset.canton.synchronizer.sequencer.InFlightAggregation.AggregationBySender
import com.digitalasset.canton.synchronizer.sequencer.errors.SequencerError.BlockNotFound
import com.digitalasset.canton.synchronizer.sequencer.store.SequencerStore
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, FailOnShutdown, ProtocolVersionChecksAsyncWordSpec}
import monocle.macros.syntax.lens.*
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.{BeforeAndAfterAll, EitherValues, OptionValues}

trait SequencerBlockStoreTest
    extends BeforeAndAfterAll
    with EitherValues
    with BaseTest
    with ProtocolVersionChecksAsyncWordSpec
    with FailOnShutdown {
  this: AsyncWordSpec & Matchers & OptionValues =>

  private val synchronizerId = DefaultTestIdentities.physicalSynchronizerId

  def sequencerBlockStore(mkBlockStore: => (SequencerStore, SequencerBlockStore)): Unit = {
    val alice = ParticipantId("alice")
    val bob = ParticipantId("bob")
    val carlos = ParticipantId("carlos")
    val allMembers = Seq(alice, bob, carlos)
    val t1 = CantonTimestamp.Epoch.plusSeconds(1)
    val t2 = t1.plusSeconds(1)
    val t3 = t2.plusSeconds(1)
    val t4 = t3.plusSeconds(1)
    val t5 = t4.plusSeconds(1)
    val t6 = t5.plusSeconds(1)
    val aggregationId1 = AggregationId(TestHash.digest(1))
    val aggregationId2 = AggregationId(TestHash.digest(2))
    val aggregationRule1 =
      AggregationRule(NonEmpty(Seq, alice, bob), PositiveInt.tryCreate(2), testedProtocolVersion)
    val aggregationRule2 = AggregationRule(
      NonEmpty(Seq, alice, bob, carlos),
      PositiveInt.tryCreate(3),
      testedProtocolVersion,
    )

    def mkBothStores(): (SequencerStore, SequencerBlockStore) = mkBlockStore

    "read head" should {
      "hydrate a correct empty state when there have been no updates" in {
        val (sequencerStore, store) = mkBothStores()
        for {
          _ <- allMembers.parTraverse(member =>
            sequencerStore.registerMember(member, CantonTimestamp.now())
          )
          headO <- store.readHead
        } yield headO shouldBe None
      }

      "hydrate correct state from previous updates" in {
        val (sequencerStore, store) = mkBothStores()
        val agg1 = InFlightAggregation(
          rule = aggregationRule1,
          maxSequencingTimestamp = t3,
          aggregatedSenders = alice -> AggregationBySender(t2, Seq.empty),
        )
        val agg2 = InFlightAggregation(
          rule = aggregationRule2,
          maxSequencingTimestamp = t4,
          aggregatedSenders = bob -> AggregationBySender(t1, Seq.empty),
        )
        val agg2Update = AggregatedSender(
          sender = alice,
          signatures = Seq.empty,
          sequencingTimestamp = t3,
        )
        val agg2a = agg2.tryAggregate(agg2Update).value
        for {
          _ <- allMembers.parTraverse(member =>
            sequencerStore.registerMember(member, CantonTimestamp.now())
          )
          _ <- addBlockUpdates(store)(
            block = BlockInfo(2, t2, Some(t2)),
            inFlightAggregationUpdates =
              Map(aggregationId1 -> agg1.asUpdate, aggregationId2 -> agg2.asUpdate),
          )
          _ <- addBlockUpdates(store)(
            block = BlockInfo(3, t2, Some(t2))
          )
          _ <- addBlockUpdates(store)(
            block = BlockInfo(4, t2, Some(t2))
          )
          _ <- addBlockUpdates(store)(
            block = BlockInfo(5, t3, Some(t2)),
            inFlightAggregationUpdates = Map(
              aggregationId1 -> InFlightAggregationUpdate.sender(
                AggregatedSender(
                  sender = bob,
                  signatures = Seq.empty,
                  sequencingTimestamp = t3,
                )
              ),
              aggregationId2 -> InFlightAggregationUpdate.sender(agg2Update),
            ),
          )
          // In the unified sequencer, the watermark affects the head state (which block is considered cleanly processed)
          _ <- sequencerStore.saveWatermark(0, t3).valueOrFail("save watermark")
          headO <- store.readHead
        } yield {
          val head = headO.value
          head.latestBlock.height shouldBe 5
          head.latestBlock.lastTs shouldBe t3
          head.latestBlock.latestSequencerEventTimestamp shouldBe Some(t2)
          head.inFlightAggregations shouldBe Map(aggregationId2 -> agg2a)
        }
      }

      "ignore partial block updates of unfinalized blocks" in {
        val (sequencerStore, store) = mkBothStores()
        val agg1 = InFlightAggregation(
          rule = aggregationRule1,
          maxSequencingTimestamp = t3,
          aggregatedSenders = alice -> AggregationBySender(t2, Seq(Seq.empty)),
        )
        val agg2 = InFlightAggregation(
          rule = aggregationRule2,
          maxSequencingTimestamp = t4,
          aggregatedSenders = carlos -> AggregationBySender(t3, Seq(Seq.empty)),
        )
        for {
          _ <- allMembers.parTraverse(member =>
            sequencerStore.registerMember(member, CantonTimestamp.now())
          )
          _ <- addBlockUpdates(store)(
            block = BlockInfo(1L, t2, Some(t1)),
            inFlightAggregationUpdates = Map(aggregationId1 -> agg1.asUpdate),
          )
          _ <- partialBlockUpdate(store)(
            inFlightAggregationUpdates = Map(aggregationId2 -> agg2.asUpdate)
          )
          // In the unified sequencer, the watermark affects the head state (which block is considered cleanly processed)
          _ <- sequencerStore.saveWatermark(0, t3).valueOrFail("save watermark")
          headO <- store.readHead
        } yield {
          val head = headO.value
          head.latestBlock shouldBe BlockInfo(1L, t2, Some(t1))
          head.inFlightAggregations shouldBe Map(aggregationId1 -> agg1)
        }
      }

      "hydrate correct state from previous updates at specific timestamps" in {
        val (sequencerStore, store) = mkBothStores()
        val agg1 = InFlightAggregation(
          rule = aggregationRule1,
          maxSequencingTimestamp = t3,
          alice -> AggregationBySender(t1, Seq.empty),
          bob -> AggregationBySender(t2, Seq.empty),
        )
        val agg2 = InFlightAggregation(
          rule = aggregationRule2,
          maxSequencingTimestamp = t5,
          bob -> AggregationBySender(t2, Seq.empty),
        )
        val agg2Update = AggregatedSender(
          sender = alice,
          signatures = Seq.empty,
          sequencingTimestamp = t3,
        )
        val agg2a = agg2.tryAggregate(agg2Update).value
        for {
          _ <- allMembers.parTraverse(member =>
            sequencerStore.registerMember(member, CantonTimestamp.now())
          )
          _ <- addBlockUpdates(store)(
            block = BlockInfo(1, t2, Some(t2)),
            inFlightAggregationUpdates =
              Map(aggregationId1 -> agg1.asUpdate, aggregationId2 -> agg2.asUpdate),
          )
          _ <- addBlockUpdates(store)(
            block = BlockInfo(2, t4, Some(t3)),
            inFlightAggregationUpdates =
              Map(aggregationId2 -> InFlightAggregationUpdate.sender(agg2Update)),
          )
          _ <- addBlockUpdates(store)(
            block = BlockInfo(3, t4, Some(t3))
          )
          _ <- addBlockUpdates(store)(
            block = BlockInfo(4, t6, Some(t3))
          )
          stateAtT1 <- store
            .readStateForBlockContainingTimestamp(
              t1,
              maxSequencingTimeBound = CantonTimestamp.MaxValue,
            )
            .valueOrFail("read at t1")
          stateAtT2 <- store
            .readStateForBlockContainingTimestamp(
              t2,
              maxSequencingTimeBound = CantonTimestamp.MaxValue,
            )
            .valueOrFail("read at t2")
          stateAtT3 <- store
            .readStateForBlockContainingTimestamp(
              t3,
              maxSequencingTimeBound = CantonTimestamp.MaxValue,
            )
            .valueOrFail("read at t3")
          stateAtT4 <- store
            .readStateForBlockContainingTimestamp(
              t4,
              maxSequencingTimeBound = CantonTimestamp.MaxValue,
            )
            .valueOrFail("read at t4")
          _ <- sequencerStore.saveWatermark(0, t1).valueOrFail("save watermark t1")
          headAtWatermarkT1 <- store.readHead
          _ <- sequencerStore.saveWatermark(0, t2).valueOrFail("save watermark t2")
          headAtWatermarkT2 <- store.readHead
          _ <- sequencerStore.saveWatermark(0, t3).valueOrFail("save watermark t3")
          headAtWatermarkT3 <- store.readHead
          _ <- sequencerStore.saveWatermark(0, t4).valueOrFail("save watermark t4")
          headAtWatermarkT4 <- store.readHead
          _ <- sequencerStore.saveWatermark(0, t5).valueOrFail("save watermark t5")
          headAtWatermarkT5 <- store.readHead
          futureTimestamp = t4.plusSeconds(100)
          stateAtFuture <- store
            .readStateForBlockContainingTimestamp(
              futureTimestamp,
              maxSequencingTimeBound = CantonTimestamp.MaxValue,
            )
            .value
        } yield {
          val block1 = BlockEphemeralState(
            BlockInfo(1, t2, Some(t2)),
            // aggregationId1 had already been expired by the latest block, but here we're getting an old snapshot
            Map(aggregationId1 -> agg1, aggregationId2 -> agg2),
          )
          val block2 = BlockEphemeralState(
            BlockInfo(2, t4, Some(t3)),
            Map(aggregationId2 -> agg2a),
          )
          val block3 = BlockEphemeralState(
            BlockInfo(3, t4, Some(t3)),
            Map(aggregationId2 -> agg2a),
          )

          stateAtT1 shouldBe block1
          stateAtT2 shouldBe block1
          stateAtT3 shouldBe block2
          stateAtT4 shouldBe block2
          headAtWatermarkT1 shouldBe None
          headAtWatermarkT2.value shouldBe block1
          headAtWatermarkT3.value shouldBe block1
          headAtWatermarkT4.value shouldBe block3
          headAtWatermarkT5.value shouldBe block3
          stateAtFuture shouldBe Left(BlockNotFound.InvalidTimestamp(futureTimestamp))
        }
      }

      "correctly deal with conflicts on bulk block info inserts" in {
        val (seqStore, store) = mkBothStores()
        val block1 = BlockInfo(1L, t2, Some(t2))
        val block2 = BlockInfo(2L, t3, Some(t2))
        val block3 = BlockInfo(3L, t4, Some(t3))
        for {
          _ <- store.finalizeBlockUpdates(Seq(block1))
          _ <- seqStore.saveWatermark(0, t2).value
          fetch1 <- store.readHead
          _ <- store.finalizeBlockUpdates(Seq(block1, block2))
          _ <- seqStore.saveWatermark(0, t3).value
          fetch2 <- store.readHead
          _ <- store.finalizeBlockUpdates(Seq(block2, block3))
          _ <- seqStore.saveWatermark(0, t4).value
          fetch3 <- store.readHead
        } yield {
          fetch1.valueOrFail("must exist").latestBlock shouldBe block1
          fetch2.valueOrFail("must exist").latestBlock shouldBe block2
          fetch3.valueOrFail("must exist").latestBlock shouldBe block3
        }
      }

    }

    "partialBlockUpdates" should {
      "be idempotent" in {
        val (sequencerStore, store) = mkBothStores()
        val agg1 = InFlightAggregation(
          rule = aggregationRule1,
          maxSequencingTimestamp = t3,
          alice -> AggregationBySender(t1, Seq.empty),
          bob -> AggregationBySender(t2, Seq.empty),
        )
        val agg2 = InFlightAggregation(
          rule = aggregationRule2,
          maxSequencingTimestamp = t4,
          bob -> AggregationBySender(t2, Seq.empty),
        )
        for {
          _ <- allMembers.parTraverse(member =>
            sequencerStore.registerMember(member, CantonTimestamp.now())
          )
          _ <- addBlockUpdates(store)(
            block = BlockInfo(0L, CantonTimestamp.Epoch, None)
          )
          _ <- partialBlockUpdate(store)(
            inFlightAggregationUpdates =
              Map(aggregationId1 -> agg1.asUpdate, aggregationId2 -> agg2.asUpdate)
          )
          _ <- partialBlockUpdate(store)(
            inFlightAggregationUpdates = Map(
              aggregationId1 -> agg1.focus(_.aggregatedSenders).modify(_.removed(bob)).asUpdate
            )
          )
          _ <- partialBlockUpdate(store)(
            inFlightAggregationUpdates = Map(
              aggregationId1 -> InFlightAggregationUpdate.sender(
                AggregatedSender(
                  sender = bob,
                  signatures = Seq.empty,
                  sequencingTimestamp = t2,
                )
              ),
              aggregationId2 -> agg2.asUpdate,
            )
          )
          _ <- store.finalizeBlockUpdates(Seq(BlockInfo(1L, t2, Some(t2))))
          head <- store
            .readStateForBlockContainingTimestamp(
              t2,
              maxSequencingTimeBound = CantonTimestamp.MaxValue,
            )
            .value
        } yield {
          head.value.latestBlock shouldBe BlockInfo(1L, t2, Some(t2))
        }
      }
    }

    "set initial state" should {

      "set and get head state" in {
        val (sequencerStore, store) = mkBothStores()
        val agg1 = InFlightAggregation(
          rule = aggregationRule1,
          maxSequencingTimestamp = t4,
          alice -> AggregationBySender(t1, Seq.empty),
          bob -> AggregationBySender(t2, Seq.empty),
        )
        val initial = SequencerInitialState(
          synchronizerId,
          SequencerSnapshot(
            t3,
            5L,
            Map.empty,
            InternalSequencerPruningStatus(CantonTimestamp.Epoch, Set.empty)
              .toSequencerPruningStatus(CantonTimestamp.now()),
            Map(aggregationId1 -> agg1): InFlightAggregations,
            None,
            protocolVersion = testedProtocolVersion,
            trafficPurchased = Seq.empty,
            trafficConsumed = Seq.empty,
          ),
          None,
          None,
        )
        for {
          _ <- allMembers.parTraverse(member =>
            sequencerStore.registerMember(member, CantonTimestamp.now())
          )
          _ <- store.setInitialState(initial)
          // In the unified sequencer, the watermark affects the head state (which block is considered cleanly processed)
          _ <- sequencerStore.saveWatermark(0, t3).valueOrFail("save watermark")
          result <- store.readHead
        } yield result.value shouldBe BlockEphemeralState(
          BlockInfo(5, t3, None),
          Map(aggregationId1 -> agg1),
        )
      }

    }
  }

  def partialBlockUpdate(store: SequencerBlockStore)(
      inFlightAggregationUpdates: InFlightAggregationUpdates
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    store.storeInflightAggregations(
      inFlightAggregationUpdates
    )

  /** Store all events that happened within the block with the given height. Shorthand for
    * [[partialBlockUpdate]] immediately followed by [[finalizeBlockUpdate]].
    */
  def addBlockUpdates(store: SequencerBlockStore)(
      block: BlockInfo,
      inFlightAggregationUpdates: InFlightAggregationUpdates = Map.empty,
  ): FutureUnlessShutdown[Unit] =
    for {
      _ <- partialBlockUpdate(store)(
        inFlightAggregationUpdates
      )
      _ <- store.finalizeBlockUpdates(Seq(block))
    } yield ()

}
