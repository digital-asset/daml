// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.block.data.memory

import cats.data.EitherT
import cats.syntax.foldable.*
import cats.syntax.functor.*
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.domain.block.data.EphemeralState.counterToCheckpoint
import com.digitalasset.canton.domain.block.data.{
  BlockEphemeralState,
  BlockInfo,
  SequencerBlockStore,
}
import com.digitalasset.canton.domain.sequencing.integrations.state.InMemorySequencerStateManagerStore
import com.digitalasset.canton.domain.sequencing.integrations.state.statemanager.{
  MemberCounters,
  MemberSignedEvents,
  MemberTimestamps,
}
import com.digitalasset.canton.domain.sequencing.sequencer.errors.SequencerError
import com.digitalasset.canton.domain.sequencing.sequencer.errors.SequencerError.BlockNotFound
import com.digitalasset.canton.domain.sequencing.sequencer.{
  InFlightAggregationUpdates,
  InternalSequencerPruningStatus,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.OrdinarySerializedEvent
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import monocle.macros.syntax.lens.*
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import java.util.concurrent.atomic.AtomicReference
import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

/** @param checkedInvariant Defines whether all methods should check the block invariant when they modify the state.
  *                          Invariant checking is slow.
  *                          It should only be enabled for testing and debugging.
  *                          [[scala.Some$]] defines the member under whom the sequencer's topology client subscribes.
  */
class InMemorySequencerBlockStore(
    private val checkedInvariant: Option[Member],
    protected val loggerFactory: NamedLoggerFactory,
) extends SequencerBlockStore
    with NamedLogging {

  private val sequencerStore = new InMemorySequencerStateManagerStore(loggerFactory)
  implicit override protected val executionContext: ExecutionContext =
    DirectExecutionContext(noTracingLogger)

  /** Stores for each block height the timestamp of the last event and the last topology client timestamp
    * up to and including this block
    */
  private val blockToTimestampMap =
    new TrieMap[Long, (CantonTimestamp, Option[CantonTimestamp])]
  private val initialState = new AtomicReference[BlockEphemeralState](BlockEphemeralState.empty)

  override def setInitialState(
      initial: BlockEphemeralState,
      maybeOnboardingTopologyEffectiveTimestamp: Option[CantonTimestamp],
  )(implicit traceContext: TraceContext): Future[Unit] = {
    for {
      _ <-
        sequencerStore
          .saveLowerBound(
            initial.state.status.lowerBound,
            maybeOnboardingTopologyEffectiveTimestamp,
          )
          .value
          .void
      _ <- Future.traverse(initial.state.status.members.toSeq.sortBy(_.registeredAt))(m =>
        for {
          _ <- sequencerStore.addMember(m.member, m.registeredAt)
          _ <- m.lastAcknowledged.traverse_ { ts =>
            sequencerStore.acknowledge(m.member, ts)
          }
          _ <- if (m.enabled) Future.unit else sequencerStore.disableMember(m.member)
        } yield ()
      )
      _ <- sequencerStore.addInFlightAggregationUpdates(
        initial.state.inFlightAggregations.fmap(_.asUpdate)
      )
    } yield {
      initialState.set(initial)
      checkBlockInvariantIfEnabled(initial.latestBlock.height)
    }
  }

  override def getInitialState(implicit traceContext: TraceContext): Future[BlockEphemeralState] =
    Future.successful(initialState.get())

  override def initialMemberCounters(implicit traceContext: TraceContext): Future[MemberCounters] =
    Future.successful(initialState.get().state.heads)

  override def partialBlockUpdate(
      newMembers: MemberTimestamps,
      events: Seq[MemberSignedEvents],
      acknowledgments: MemberTimestamps,
      membersDisabled: Seq[Member],
      inFlightAggregationUpdates: InFlightAggregationUpdates,
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val addMember = sequencerStore.addMember(_, _)
    val addEvents = sequencerStore.addEvents(_)
    val addAcks = sequencerStore.acknowledge(_, _)
    val disableMember = sequencerStore.disableMember(_)
    // Since these updates are being run sequentially from the state manager, there is no problem with this
    // implementation not being atomic.
    // Also because this is an in-mem implementation, there is no concern about crashing mid update since all state
    // is lost anyway in that case.
    for {
      _ <- Future.traverse(newMembers.toSeq)(addMember.tupled)
      _ <- Future.traverse(events)(addEvents)
      _ <- Future.traverse(acknowledgments.toSeq)(addAcks.tupled)
      _ <- Future.traverse(membersDisabled)(disableMember)
      _ <- sequencerStore.addInFlightAggregationUpdates(inFlightAggregationUpdates)
    } yield ()
  }

  override def finalizeBlockUpdate(block: BlockInfo)(implicit
      traceContext: TraceContext
  ): Future[Unit] = {
    blockToTimestampMap
      .put(block.height, block.lastTs -> block.latestSequencerEventTimestamp)
      .discard
    checkBlockInvariantIfEnabled(block.height)
    Future.unit
  }

  override def readRange(
      member: Member,
      startInclusive: SequencerCounter,
      endExclusive: SequencerCounter,
  )(implicit traceContext: TraceContext): Source[OrdinarySerializedEvent, NotUsed] =
    sequencerStore.readRange(member, startInclusive, endExclusive)

  override def readHead(implicit traceContext: TraceContext): Future[BlockEphemeralState] =
    blockToTimestampMap.snapshot().maxByOption { case (height, _) => height } match {
      case Some((height, (latestTs, latestSequencerEventTimestamp))) =>
        for {
          state <- sequencerStore.readAtBlockTimestamp(latestTs)
        } yield mergeWithInitialState(
          BlockEphemeralState(
            BlockInfo(height, latestTs, latestSequencerEventTimestamp),
            state,
          )
        )
      case None =>
        Future.successful(initialState.get())
    }

  override def readStateForBlockContainingTimestamp(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): EitherT[Future, SequencerError, BlockEphemeralState] =
    blockToTimestampMap.toList
      .sortBy(_._2._1)
      .find(_._2._1 >= timestamp)
      .fold[EitherT[Future, SequencerError, BlockEphemeralState]](
        EitherT.leftT(BlockNotFound.InvalidTimestamp(timestamp))
      ) { case (blockHeight, (blockTimestamp, latestSequencerEventTs)) =>
        val block = BlockInfo(blockHeight, blockTimestamp, latestSequencerEventTs)
        EitherT.right(
          sequencerStore
            .readAtBlockTimestamp(blockTimestamp)
            .map(state => mergeWithInitialState(BlockEphemeralState(block, state)))
        )
      }

  private def mergeWithInitialState(current: BlockEphemeralState) = {
    val initial = initialState.get()
    current.copy(state =
      current.state.copy(
        checkpoints = initial.state.checkpoints ++ current.state.checkpoints
      )
    )
  }

  override def pruningStatus()(implicit
      traceContext: TraceContext
  ): Future[InternalSequencerPruningStatus] =
    sequencerStore.status()

  def locatePruningTimestamp(skip: NonNegativeInt)(implicit
      traceContext: TraceContext
  ): Future[Option[CantonTimestamp]] =
    Future.successful(sequencerStore.locatePruningTimestamp(skip))

  override def prune(requestedTimestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[String] = {
    val result = sequencerStore.pruneSync(requestedTimestamp)
    val newInFlightAggregations =
      sequencerStore.pruneExpiredInFlightAggregationsInternal(requestedTimestamp)
    val blocksToBeRemoved = blockToTimestampMap.filter(_._2._1 < requestedTimestamp)
    blocksToBeRemoved.keys.foreach(
      blockToTimestampMap.remove(_).discard[Option[(CantonTimestamp, Option[CantonTimestamp])]]
    )
    val lastBlockRemovedO = blocksToBeRemoved.maxByOption(_._1)
    // Update the initial state only if we have actually removed blocks
    lastBlockRemovedO.foreach { case (height, (lastTs, latestSequencerEventTimestamp)) =>
      initialState.getAndUpdate { state =>
        // the initial state holds the counters immediately before the ones sequencer actually supports from
        val newHeads = state.state.heads ++ result.newMinimumCountersSupported.fmap(_ - 1)
        BlockEphemeralState(
          latestBlock = BlockInfo(height, lastTs, latestSequencerEventTimestamp),
          state = state.state.copy(
            status = sequencerStore.statusSync(),
            inFlightAggregations = newInFlightAggregations,
            checkpoints = newHeads.fmap(counterToCheckpoint),
          ),
        )
      }
      checkBlockInvariantIfEnabled(height)
    }
    Future.successful(
      s"Removed ${result.eventsPruned} events and ${blocksToBeRemoved.size} blocks"
    )
  }

  override def updateMemberCounterSupportedAfter(
      member: Member,
      counterLastUnsupported: SequencerCounter,
  )(implicit
      traceContext: TraceContext
  ): Future[Unit] =
    Future.successful(initialState.getAndUpdate { previousState =>
      // Don't update member counter if specified counter is less than or equal that previous counter.
      val prevCounter = previousState.state.headCounter(member)
      if (prevCounter.exists(_ >= counterLastUnsupported)) previousState
      else
        previousState
          .focus(_.state.checkpoints)
          .modify(_ + (member -> counterToCheckpoint(counterLastUnsupported)))
    }.discard)

  private[this] def checkBlockInvariantIfEnabled(
      blockHeight: Long
  )(implicit traceContext: TraceContext): Unit =
    checkedInvariant.foreach { topologyClientMember =>
      checkBlockInvariant(topologyClientMember, blockHeight)
    }

  private[this] def checkBlockInvariant(
      topologyClientMember: Member,
      blockHeight: Long,
  )(implicit traceContext: TraceContext): Unit = {
    val snapshot = blockToTimestampMap.snapshot()
    snapshot.get(blockHeight).foreach { case (lastTs, latestSequencerEventTimestamp) =>
      val currentBlock = BlockInfo(blockHeight, lastTs, latestSequencerEventTimestamp)
      val prevBlockO = snapshot.view
        .filter { case (height, _) => height < blockHeight }
        .maxByOption { case (height, _) => height }
        .map { case (height, (prevLastTs, prevLatestSequencerEventTimestamp)) =>
          BlockInfo(height, prevLastTs, prevLatestSequencerEventTimestamp)
        }
      val prevLastTs = prevBlockO.fold(CantonTimestamp.MinValue)(_.lastTs)
      val allEventsInBlock = sequencerStore.allEventsInTimeRange(prevLastTs, lastTs)
      val newMembers = sequencerStore.allRegistrations().filter { case (member, ts) =>
        ts > prevLastTs && ts <= lastTs
      }
      val inFlightAggregations = sequencerStore.inFlightAggregationsAt(lastTs)

      blockInvariant(
        topologyClientMember,
        currentBlock,
        prevBlockO,
        allEventsInBlock,
        newMembers,
        inFlightAggregations,
      )
    }
  }

  override def getInitialTopologySnapshotTimestamp(implicit
      traceContext: TraceContext
  ): Future[Option[CantonTimestamp]] = sequencerStore.getInitialTopologySnapshotTimestamp

  override def close(): Unit = ()

}
