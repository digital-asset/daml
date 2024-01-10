// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.block

import cats.syntax.functor.*
import com.daml.error.BaseError
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.block.BlockUpdateGenerator.SignedEvents
import com.digitalasset.canton.domain.block.data.BlockInfo
import com.digitalasset.canton.domain.sequencing.integrations.state.EphemeralState
import com.digitalasset.canton.domain.sequencing.sequencer.InFlightAggregationUpdates
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.Traced
import com.digitalasset.canton.util.MapsUtil

import scala.concurrent.Future

/** A series of changes from processing the chunks of updates within a block. */
sealed trait BlockUpdates extends Product with Serializable

/** A chunk of updates within a block. The updates can be delivered to
  * [[com.digitalasset.canton.sequencing.client.SequencerClient]]s immediately,
  * before fully processing the block.
  *
  * The next partial block update may depend on the events in the current chunk,
  * e.g., by the topology processor processing them via its sequencer client subscription.
  * For this reason, the next partial block update is wrapped in its own future,
  * which can sync the topology updates via the topology client.
  *
  * @param continuation Computes the remainder of updates in a given block
  */
final case class PartialBlockUpdate(
    chunk: ChunkUpdate,
    continuation: Future[BlockUpdates],
) extends BlockUpdates

/** Signals that all updates in a block have been delivered as chunks.
  * The [[com.digitalasset.canton.domain.block.data.BlockInfo]] must be consistent with
  * the updates in all earlier [[ChunkUpdate]]s. In particular:
  * - [[com.digitalasset.canton.domain.block.data.BlockInfo.lastTs]] must be at least the
  *   one from the last chunk or previous block
  * - [[com.digitalasset.canton.domain.block.data.BlockInfo.latestTopologyClientTimestamp]]
  *   must be at least the one from the last chunk or previous block.
  * - [[com.digitalasset.canton.domain.block.data.BlockInfo.height]] must be exactly one higher
  *   than the previous block
  * The consistency conditions are checked in `handleUpdate`
  */
final case class CompleteBlockUpdate(
    block: BlockInfo
) extends BlockUpdates

/** Changes from processing a consecutive part of updates within a block from the blockchain.
  * We expect all values to be consistent with one another:
  *  - new members must exist in the registered members
  *  - the provided timestamps must be at or after the latest sequencer timestamp of the previous chunk or block
  *  - members receiving events must be registered
  *  - timestamps of events must not after the latest sequencer timestamp of the previous chunk or block
  *  - counter values for each member should be continuous
  *
  * @param newMembers Members that were added along with the timestamp that they are considered registered from.
  * @param membersDisabled Members that were disabled.
  * @param acknowledgements The highest valid acknowledged timestamp for each member in the block.
  * @param invalidAcknowledgements All invalid acknowledgement timestamps in the block for each member.
  * @param signedEvents New sequenced events for members.
  * @param inFlightAggregationUpdates The updates to the in-flight aggregation states.
  *                             Does not include the clean-up of expired aggregations.
  * @param pruningRequests Upper bound timestamps to prune the sequencer's local state.
  * @param lastTopologyClientTimestamp The highest timestamp of an event in `events` addressed to the sequencer's topology client, if any.
  * @param state Updated ephemeral state to be used for processing subsequent chunks.
  */
final case class ChunkUpdate(
    newMembers: Map[Member, CantonTimestamp] = Map.empty,
    membersDisabled: Seq[Member] = Seq.empty,
    acknowledgements: Map[Member, CantonTimestamp] = Map.empty,
    invalidAcknowledgements: Seq[(Member, CantonTimestamp, BaseError)] = Seq.empty,
    signedEvents: Seq[SignedEvents] = Seq.empty,
    inFlightAggregationUpdates: InFlightAggregationUpdates = Map.empty,
    pruningRequests: Seq[Traced[CantonTimestamp]] = Seq.empty,
    lastTopologyClientTimestamp: Option[CantonTimestamp],
    state: EphemeralState,
) {
  // ensure that all new members appear in the ephemeral state
  require(
    newMembers.keys.forall(state.registeredMembers.contains),
    "newMembers should be placed within the ephemeral state",
  )
  // check all events are from registered members
  require(
    signedEvents.view.flatMap(_.keys).forall(state.registeredMembers.contains),
    "events must be for registered members",
  )
  // check the counters assigned for each member are continuous
  def isContinuous(counters: Seq[SequencerCounter]): Boolean =
    NonEmpty.from(counters) match {
      case None => true
      case Some(countersNE) =>
        val head = countersNE.head1
        val expectedCounters = head until (head + countersNE.size)
        counters == expectedCounters
    }

  locally {
    val counters = signedEvents
      .map(_.forgetNE.fmap(event => Seq(event.counter)))
      .foldLeft(Map.empty[Member, Seq[SequencerCounter]])(
        MapsUtil.mergeWith(_, _)(_ ++ _)
      )
    require(
      counters.values.forall(isContinuous),
      s"Non-continuous counters: $counters",
    )
  }
  // The other consistency conditions are checked in `BlockSequencerStateManager.handleChunkUpdate`
}
