// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.block.data

import cats.data.EitherT
import cats.syntax.reducible.*
import com.daml.nonempty.NonEmpty
import com.daml.nonempty.catsinstances.*
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.block.data.SequencerBlockStore.InvalidTimestamp
import com.digitalasset.canton.domain.block.data.db.DbSequencerBlockStore
import com.digitalasset.canton.domain.block.data.memory.InMemorySequencerBlockStore
import com.digitalasset.canton.domain.sequencing.integrations.state.statemanager.{
  MemberCounters,
  MemberSignedEvents,
  MemberTimestamps,
}
import com.digitalasset.canton.domain.sequencing.sequencer.{
  InFlightAggregationUpdates,
  InFlightAggregations,
  InternalSequencerPruningStatus,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.sequencing.OrdinarySerializedEvent
import com.digitalasset.canton.sequencing.protocol.{AllMembersOfDomain, Deliver, SequencersOfDomain}
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.version.ProtocolVersion
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.{ExecutionContext, Future}

trait SequencerBlockStore extends AutoCloseable {
  this: NamedLogging =>

  protected def executionContext: ExecutionContext

  /** Set initial state of the sequencer node from which it supports serving requests.
    * This should be called at most once. If not called, it means this sequencer node can
    * server requests from genesis.
    */
  def setInitialState(
      initial: BlockEphemeralState = BlockEphemeralState.empty,
      maybeOnboardingTopologyEffectiveTimestamp: Option[CantonTimestamp] = None,
  )(implicit
      traceContext: TraceContext
  ): Future[Unit]

  /** Get initial state from which this sequencer node supports serving requests.
    * The member counters returned as part of this initial state indicate the minimum counters
    * that this sequencer supports serving requests from.
    * If a member is not included in these counters, it means that this sequencer node supports serving requests
    * from the [[com.digitalasset.canton.data.CounterCompanion.Genesis]] for that member.
    */
  def getInitialState(implicit traceContext: TraceContext): Future[BlockEphemeralState]

  /** Get initial member counters from which this sequencer node supports serving requests.
    * If a member is not included in these counters, it means that this sequencer node supports serving requests
    * from the [[com.digitalasset.canton.data.CounterCompanion.Genesis]] for that member.
    */
  def initialMemberCounters(implicit traceContext: TraceContext): Future[MemberCounters]

  /** Retrieve the timestamp of the initial topology snapshot if available.
    */
  def getInitialTopologySnapshotTimestamp(implicit
      traceContext: TraceContext
  ): Future[Option[CantonTimestamp]]

  /** The current state of the sequencer, which can be used when the node is restarted to deterministically
    * derive the following counters and timestamps.
    *
    * The state excludes updates of unfinalized blocks added with [[partialBlockUpdate]].
    */
  def readHead(implicit traceContext: TraceContext): Future[BlockEphemeralState]

  /** The state at the end of the block that contains the given timestamp. This will typically be used to inform
    * other sequencer nodes being initialized of the initial state they should use based on the timestamp they provide
    * which is typically the timestamp of their signing key.
    */
  def readStateForBlockContainingTimestamp(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[Future, InvalidTimestamp, BlockEphemeralState]

  def pruningStatus()(implicit traceContext: TraceContext): Future[InternalSequencerPruningStatus]

  /** Locate a timestamp relative to the earliest available event based on a skip index starting at 0.
    * Useful to monitor the progress of pruning and for pruning in batches.
    * @return The timestamp of the (skip+1)'th event if it exists, None otherwise.
    */
  def locatePruningTimestamp(skip: NonNegativeInt)(implicit
      traceContext: TraceContext
  ): Future[Option[CantonTimestamp]]
  def prune(requestedTimestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[String]

  /** Updates the last unsupported member sequencer counter, i.e. the one just before the
    * first supported sequencer counter. Only ever increases counter.
    */
  def updateMemberCounterSupportedAfter(
      member: Member,
      counterLastUnsupported: SequencerCounter,
  )(implicit
      traceContext: TraceContext
  ): Future[Unit]

  /** Stores some updates that happen in a single block.
    * May be called several times for the same block
    * and the same update may be contained in several of the calls.
    * Before adding updates of a subsequent block, [[finalizeBlockUpdate]] must be called to wrap up
    * the current block.
    *
    * This method must not be called concurrently with itself or [[finalizeBlockUpdate]].
    */
  def partialBlockUpdate(
      newMembers: MemberTimestamps,
      events: Seq[MemberSignedEvents],
      acknowledgments: MemberTimestamps,
      membersDisabled: Seq[Member],
      inFlightAggregationUpdates: InFlightAggregationUpdates,
  )(implicit traceContext: TraceContext): Future[Unit]

  /** Finalizes the current block whose updates have been added in the calls to [[partialBlockUpdate]]
    * since the last call to [[finalizeBlockUpdate]].
    *
    * This method must not be called concurrently with itself or [[partialBlockUpdate]],
    * and must be called for the blocks in monotonically increasing order of height.
    *
    * @param block The block information about the current block.
    *              It is the responsibility of the caller to ensure that the height increases monotonically by one
    */
  def finalizeBlockUpdate(block: BlockInfo)(implicit traceContext: TraceContext): Future[Unit]

  /** Serve events (or tombstones) for member with the given counter range.
    *
    * The returned range may include events added with [[partialBlockUpdate]]
    * even before the corresponding block has been finalized with [[finalizeBlockUpdate]].
    * This ensures that [[com.digitalasset.canton.sequencing.client.SequencerClient]]s
    * can immediately read events when they are added. In particular,
    * the sequencer's [[com.digitalasset.canton.topology.client.DomainTopologyClient]] will see such updates
    * so that block processing can request a [[com.digitalasset.canton.topology.client.TopologySnapshot]]
    * that includes the updates in a block while processing continues for the same block.
    */
  def readRange(member: Member, startInclusive: SequencerCounter, endExclusive: SequencerCounter)(
      implicit traceContext: TraceContext
  ): Source[OrdinarySerializedEvent, NotUsed]

  /** Check the invariant for a block in the store:
    * <ul>
    * <li>If the previous block is defined, then its block height is `currentBlock.height - 1`.</li>
    * <li>If the previous block is defined,
    * then its [[com.digitalasset.canton.domain.block.data.BlockInfo.lastTs]] of the
    * the [[com.digitalasset.canton.domain.block.data.BlockEphemeralState]]
    * is no later than the one of the current block.
    * If there are neither events nor registrations in the current block, the two are equal.
    * </li>
    * <li>If the previous block is defined,
    * the [[com.digitalasset.canton.domain.block.data.BlockInfo.latestSequencerEventTimestamp]] of the
    * its [[com.digitalasset.canton.domain.block.data.BlockEphemeralState]]
    * is no later than the one of the current block. In particular,
    * if [[com.digitalasset.canton.domain.block.data.BlockInfo.latestSequencerEventTimestamp]]
    * is defined in the previous block, then so is it in the successor block.
    * If it is defined in the successor block, then it is the same as in the predecessor block
    * unless the block contains events addressed to the sequencer's topology client.
    * </li>
    * <li>All events and member registrations in the current block are ealier or equal to the blocks'
    * [[com.digitalasset.canton.domain.block.data.BlockInfo.lastTs]].
    * [[com.digitalasset.canton.domain.block.data.BlockInfo.lastTs]] is equal to the latest timestamp
    * of an event or member registration in the block, unless the previous block is undefined
    * and the block contains member registrations.
    * </li>
    * <li>All events in the block addressed to the `topologyClientMember` have timestamps of at most
    * [[com.digitalasset.canton.domain.block.data.BlockInfo.latestSequencerEventTimestamp]] of the current block.
    * </li>
    * </ul>
    *
    * @param topologyClientMember             The member under whom the sequencer's topology client subscribes
    * @param currentBlock                     The block info of the current block
    * @param prevBlockO                       The block info of the block preceding the `currentBlock`, unless the current block is the initial state
    * @param allEventsInBlock                 All events in the current block
    * @param newMembersInBlock                All member registrations that happened in the block
    * @param inFlightAggregationsAtEndOfBlock The in-flight aggregation state valid at the end of the block,
    *                                         i.e., at `currentBlock`'s [[com.digitalasset.canton.domain.block.data.BlockInfo.lastTs]].
    * @throws java.lang.IllegalStateException if the invariant is violated
    */
  protected def blockInvariant(
      topologyClientMember: Member,
      currentBlock: BlockInfo,
      prevBlockO: Option[BlockInfo],
      allEventsInBlock: Map[Member, NonEmpty[Seq[OrdinarySerializedEvent]]],
      newMembersInBlock: Iterable[(Member, CantonTimestamp)],
      inFlightAggregationsAtEndOfBlock: InFlightAggregations,
  )(implicit traceContext: TraceContext): Unit = {
    prevBlockO.foreach { prevBlock =>
      ErrorUtil.requireState(
        prevBlock.height == currentBlock.height - 1,
        s"There is a gap between previous block height ${prevBlock.height} and the current block height ${currentBlock.height}",
      )

      ErrorUtil.requireState(
        prevBlock.lastTs <= currentBlock.lastTs,
        s"The last event timestamp ${currentBlock.lastTs} in the current block ${currentBlock.height} is before the last event timestamp ${prevBlock.lastTs} of the previous block",
      )

      (prevBlock.latestSequencerEventTimestamp, currentBlock.latestSequencerEventTimestamp) match {
        case (Some(prevTs), Some(currTs)) =>
          ErrorUtil.requireState(
            prevTs <= currTs,
            s"The latest topology client timestamp $prevTs of block ${prevBlock.height} is after the corresponding timestamp $currTs of the next block ${currentBlock.height}",
          )
        case (None, _) =>
        case (Some(prevTs), None) =>
          ErrorUtil.internalError(
            new IllegalStateException(
              s"The latest topology client timestamp for block ${currentBlock.height} is unknown, but known as $prevTs for the previous block"
            )
          )
      }
    }

    val maxEventO = allEventsInBlock.values.flatMap(_.forgetNE).maxByOption(_.timestamp)
    val maxRegistrationTsO = newMembersInBlock.maxByOption(_._2)

    (maxEventO, maxRegistrationTsO) match {
      case (None, None) =>
        // If all the events for the block have been filtered out
        // (because they were not received within the request's `maxSequencingTime`),
        // `prevBlock.lastTs` can be < `currentBlock.lastTs`.
        prevBlockO.foreach { prevBlock =>
          ErrorUtil.requireState(
            prevBlock.lastTs <= currentBlock.lastTs,
            s"The last timestamp for block ${currentBlock.height} went backwards from ${prevBlock.lastTs} to ${currentBlock.lastTs}, and there are neither events nor member registrations",
          )
        }
      case (Some(maxEvent), None) =>
        // If some events in the block have been filtered out
        // (because they were not received within the request's `maxSequencingTime`),
        // `maxEvent.timestamp` can be < `currentBlock.lastTs`.
        ErrorUtil.requireState(
          maxEvent.timestamp <= currentBlock.lastTs,
          s"The last timestamp ${currentBlock.lastTs} for block ${currentBlock.height} is past the block's last event at ${maxEvent.timestamp} and there are no member registrations",
        )
      case (None, Some((member, memberTs))) =>
        // pruning deletes the events of the then initial block, but not the member registrations.
        // So we allow that the last ts is after the last member registration.
        ErrorUtil.requireState(
          memberTs == currentBlock.lastTs || prevBlockO.isEmpty && memberTs <= currentBlock.lastTs,
          s"The last timestamp ${currentBlock.lastTs} for block ${currentBlock.height} differs from the block's last registration (for member $member) at ${memberTs} and there are no events",
        )
      case (Some(maxEvent), Some((member, memberTs))) =>
        ErrorUtil.requireState(
          Ordering[CantonTimestamp].max(maxEvent.timestamp, memberTs) == currentBlock.lastTs,
          s"The last timestamp ${currentBlock.lastTs} for block ${currentBlock.height} differs from both the block's last registration (for member $member) at ${memberTs} and the block's last event at ${maxEvent.timestamp}",
        )
    }

    // Keep only the events addressed to the sequencer that advance `latestSequencerEventTimestamp`.
    // TODO(i17741): write a security test that checks proper behavior when a sequencer is targeted directly
    val topologyEventsInBlock = allEventsInBlock
      .get(topologyClientMember)
      .map(_.filter(_.signedEvent.content match {
        case Deliver(_, _, _, _, batch, _, _) =>
          val recipients = batch.allRecipients
          recipients.contains(SequencersOfDomain) || recipients.contains(AllMembersOfDomain)
        case _ => false
      }))
      .flatMap(NonEmpty.from)

    topologyEventsInBlock match {
      case None =>
        // For the initial state, the latest topology client timestamp
        // may be set even though the block does not contain events and we don't know the previous block any more
        prevBlockO.foreach { prevBlock =>
          ErrorUtil.requireState(
            prevBlock.latestSequencerEventTimestamp == currentBlock.latestSequencerEventTimestamp,
            s"The latest topology client timestamp for block ${currentBlock.height} changed from ${prevBlock.latestSequencerEventTimestamp} to ${currentBlock.latestSequencerEventTimestamp}, but the block contains no events for $topologyClientMember",
          )
        }
      case Some(topologyEvents) =>
        val lastEvent = topologyEvents.toNEF.maximumBy(_.timestamp)

        ErrorUtil.requireState(
          currentBlock.latestSequencerEventTimestamp.contains(lastEvent.timestamp),
          s"The latest topology client timestamp for block ${currentBlock.height} is ${currentBlock.latestSequencerEventTimestamp}, but the last event in the block to $topologyClientMember is at ${lastEvent.timestamp}",
        )
    }

    inFlightAggregationsAtEndOfBlock.foreach { case (aggregationId, inFlightAggregation) =>
      inFlightAggregation.checkInvariant()
    }
  }
}

object SequencerBlockStore {
  def apply(
      storage: Storage,
      protocolVersion: ProtocolVersion,
      timeouts: ProcessingTimeout,
      enableAdditionalConsistencyChecks: Boolean,
      checkedInvariant: Option[Member],
      loggerFactory: NamedLoggerFactory,
      unifiedSequencer: Boolean,
  )(implicit
      executionContext: ExecutionContext
  ): SequencerBlockStore =
    storage match {
      case _: MemoryStorage =>
        new InMemorySequencerBlockStore(checkedInvariant, loggerFactory)
      case dbStorage: DbStorage =>
        new DbSequencerBlockStore(
          dbStorage,
          protocolVersion,
          timeouts,
          enableAdditionalConsistencyChecks,
          checkedInvariant,
          loggerFactory,
          unifiedSequencer = unifiedSequencer,
        )
    }

  final case class InvalidTimestamp(timestamp: CantonTimestamp)
}
