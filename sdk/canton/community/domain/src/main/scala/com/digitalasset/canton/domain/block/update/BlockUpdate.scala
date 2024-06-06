// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.block.update

import cats.syntax.functor.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.crypto.SyncCryptoApi
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.block.data.{BlockInfo, BlockUpdateEphemeralState}
import com.digitalasset.canton.domain.block.update.BlockUpdateGenerator.EventsForSubmissionRequest
import com.digitalasset.canton.domain.sequencing.sequencer.block.BlockSequencer.LocalEvent
import com.digitalasset.canton.domain.sequencing.sequencer.{
  InFlightAggregationUpdates,
  SubmissionRequestOutcome,
}
import com.digitalasset.canton.error.BaseAlarm
import com.digitalasset.canton.sequencing.protocol.SequencedEventTrafficState
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MapsUtil

import scala.collection.MapView

/** Summarizes the updates that are to be persisted and signalled individually */
sealed trait BlockUpdate[+E <: ChunkEvents] extends Product with Serializable

/** Denotes an update that is generated from a block that went through ordering */
sealed trait OrderedBlockUpdate[+E <: ChunkEvents] extends BlockUpdate[E]

/** Signals that all updates in a block have been delivered as chunks.
  * The [[com.digitalasset.canton.domain.block.data.BlockInfo]] must be consistent with
  * the updates in all earlier [[ChunkUpdate]]s. In particular:
  * - [[com.digitalasset.canton.domain.block.data.BlockInfo.lastTs]] must be at least the
  *   one from the last chunk or previous block
  * - [[com.digitalasset.canton.domain.block.data.BlockInfo.latestSequencerEventTimestamp]]
  *   must be at least the one from the last chunk or previous block.
  * - [[com.digitalasset.canton.domain.block.data.BlockInfo.height]] must be exactly one higher
  *   than the previous block
  * The consistency conditions are checked in [[com.digitalasset.canton.domain.block.BlockSequencerStateManager]]'s `handleComplete`.
  */
final case class CompleteBlockUpdate(block: BlockInfo) extends OrderedBlockUpdate[Nothing]

/** Changes from processing a consecutive part of updates within a block from the blockchain.
  * We expect all values to be consistent with one another:
  *  - new members must exist in the registered members
  *  - the provided timestamps must be at or after the latest sequencer timestamp of the previous chunk or block
  *  - members receiving events must be registered
  *  - timestamps of events must not after the latest sequencer timestamp of the previous chunk or block
  *  - counter values for each member should be continuous
  *
  * @param newMembers Members that were added along with the timestamp that they are considered registered from.
  * @param acknowledgements The highest valid acknowledged timestamp for each member in the block.
  * @param invalidAcknowledgements All invalid acknowledgement timestamps in the block for each member.
  * @param events New sequenced events for members, and the snapshot to be used for signing them.
  * @param inFlightAggregationUpdates The updates to the in-flight aggregation states.
  *                             Includes the clean-up of expired aggregations.
  * @param lastSequencerEventTimestamp The highest timestamp of an event in `events` addressed to the sequencer, if any.
  * @param state Updated ephemeral state to be used for processing subsequent chunks.
  * @param submissionsOutcomes  A list of internal block sequencer states after processing submissions for the chunk.
  *                             This is used by the unified sequencer to generate and write events in the database sequencer.
  */
final case class ChunkUpdate[+E <: ChunkEvents](
    newMembers: Map[Member, CantonTimestamp] = Map.empty,
    acknowledgements: Map[Member, CantonTimestamp] = Map.empty,
    invalidAcknowledgements: Seq[(Member, CantonTimestamp, BaseAlarm)] = Seq.empty,
    events: Seq[E] = Seq.empty,
    inFlightAggregationUpdates: InFlightAggregationUpdates = Map.empty,
    lastSequencerEventTimestamp: Option[CantonTimestamp],
    state: BlockUpdateEphemeralState,
    submissionsOutcomes: Seq[SubmissionRequestOutcome] = Seq.empty,
) extends OrderedBlockUpdate[E] {
  // ensure that all new members appear in the ephemeral state
  require(
    newMembers.keys.forall(state.registeredMembers.contains),
    "newMembers should be placed within the ephemeral state",
  )
  // check all events are from registered members
  require(
    events.view.flatMap(_.members).forall(state.registeredMembers.contains),
    "events must be for registered members",
  )
  // check the counters assigned for each member are continuous

  locally {
    def isContinuous(counters: Seq[SequencerCounter]): Boolean =
      NonEmpty.from(counters) match {
        case None => true
        case Some(countersNE) =>
          val head = countersNE.head1
          val expectedCounters = head until (head + countersNE.size)
          counters == expectedCounters
      }

    val counters = events
      .map(_.counters.fmap(Seq(_)))
      .foldLeft(Map.empty[Member, Seq[SequencerCounter]])(
        MapsUtil.mergeWith(_, _)(_ ++ _)
      )
    require(
      counters.values.forall(isContinuous),
      s"Non-continuous counters: $counters",
    )
    // The other consistency conditions are checked in `BlockSequencerStateManager.handleChunkUpdate`
  }
}

/** Denotes an update to the persisted state that is caused by a local event that has not gone through ordering */
final case class LocalBlockUpdate(local: LocalEvent) extends BlockUpdate[Nothing]

sealed trait ChunkEvents extends Product with Serializable {
  def members: Set[Member]
  def counters: Map[Member, SequencerCounter]
  def timestamps: Iterable[CantonTimestamp]
}

final case class UnsignedChunkEvents(
    sender: Member,
    events: EventsForSubmissionRequest,
    topologyOrSequencingSnapshot: SyncCryptoApi,
    sequencingTimestamp: CantonTimestamp,
    latestSequencerEventTimestamp: Option[CantonTimestamp],
    trafficStates: MapView[Member, SequencedEventTrafficState],
    traceContext: TraceContext,
) extends ChunkEvents {
  override def members: Set[Member] = events.keySet
  override def counters: Map[Member, SequencerCounter] = events.fmap(_.counter)
  override def timestamps: Iterable[CantonTimestamp] = events.values.map(_.timestamp)
}

final case class SignedChunkEvents(
    events: BlockUpdateGenerator.SignedEvents
) extends ChunkEvents {
  override def members: Set[Member] = events.keySet
  override def counters: Map[Member, SequencerCounter] = events.fmap(_.counter)
  override def timestamps: Iterable[CantonTimestamp] = events.values.map(_.timestamp)
}
