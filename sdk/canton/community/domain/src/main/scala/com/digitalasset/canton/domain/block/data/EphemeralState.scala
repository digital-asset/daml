// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.block.data

import cats.syntax.functor.*
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.sequencer.store.CounterCheckpoint
import com.digitalasset.canton.domain.sequencing.sequencer.{
  InFlightAggregations,
  InternalSequencerPruningStatus,
  SequencerSnapshot,
}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.sequencing.traffic.{TrafficConsumed, TrafficPurchased}
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.version.ProtocolVersion

/** State held in memory by [[com.digitalasset.canton.domain.block.BlockSequencerStateManager]] to keep track of:
  *
  * @param checkpoints The latest counter value for members who have previously received an event
  *              (registered members who have not yet received an event will not have a value)
  * @param inFlightAggregations All aggregatable submission requests by their [[com.digitalasset.canton.sequencing.protocol.AggregationId]]
  *                             whose [[com.digitalasset.canton.domain.sequencing.sequencer.InFlightAggregation.maxSequencingTimestamp]] has not yet elapsed.
  * @param status Pruning status, which includes members info and relevant timestamps
  * @param trafficState The traffic state for each member
  */
final case class EphemeralState(
    inFlightAggregations: InFlightAggregations,
    status: InternalSequencerPruningStatus,
    checkpoints: Map[Member, CounterCheckpoint],
) extends PrettyPrinting {
  def registeredMembers: Set[Member] = status.membersMap.keySet
  def heads: Map[Member, SequencerCounter] = checkpoints.fmap(_.counter)

  locally {
    val registered = registeredMembers
    val unregisteredMembersWithCounters = checkpoints.keys.filterNot(registered.contains)
    require(
      unregisteredMembersWithCounters.isEmpty,
      s"All members with a head counter value must be registered. " +
        s"Members ${unregisteredMembersWithCounters.toList} have counters but are not registered.",
    )
  }

  def toSequencerSnapshot(
      lastTs: CantonTimestamp,
      latestBlockHeight: Long,
      additional: Option[SequencerSnapshot.ImplementationSpecificInfo],
      protocolVersion: ProtocolVersion,
      trafficPurchased: Seq[TrafficPurchased],
      trafficConsumed: Seq[TrafficConsumed],
  ): SequencerSnapshot =
    SequencerSnapshot(
      lastTs,
      latestBlockHeight,
      heads,
      status.toSequencerPruningStatus(lastTs),
      inFlightAggregations,
      additional,
      protocolVersion,
      trafficPurchased,
      trafficConsumed,
    )

  def evictExpiredInFlightAggregations(upToInclusive: CantonTimestamp): EphemeralState =
    this.copy(
      inFlightAggregations = inFlightAggregations.filterNot { case (_, inFlightAggregation) =>
        inFlightAggregation.expired(upToInclusive)
      }
    )

  def toBlockUpdateEphemeralState: BlockUpdateEphemeralState = BlockUpdateEphemeralState(
    checkpoints = checkpoints,
    inFlightAggregations = inFlightAggregations,
    membersMap = status.membersMap,
  )

  def mergeBlockUpdateEphemeralState(other: BlockUpdateEphemeralState): EphemeralState =
    EphemeralState(
      checkpoints = other.checkpoints,
      inFlightAggregations = other.inFlightAggregations,
      status = this.status.copy(membersMap = other.membersMap),
    )

  def headCounter(member: Member): Option[SequencerCounter] = checkpoints.get(member).map(_.counter)

  override def pretty: Pretty[EphemeralState] = prettyOfClass(
    param("checkpoints", _.checkpoints),
    param("in-flight aggregations", _.inFlightAggregations),
    param("status", _.status),
  )
}

object EphemeralState {
  val empty: EphemeralState =
    EphemeralState.fromHeads(
      Map.empty[Member, SequencerCounter],
      Map.empty: InFlightAggregations,
      InternalSequencerPruningStatus.Unimplemented,
    )

  def counterToCheckpoint(counter: SequencerCounter): CounterCheckpoint =
    CounterCheckpoint(counter, CantonTimestamp.MinValue, None)

  def fromHeads(
      heads: Map[Member, SequencerCounter],
      inFlightAggregations: InFlightAggregations,
      status: InternalSequencerPruningStatus,
  ): EphemeralState =
    EphemeralState(
      inFlightAggregations,
      status,
      heads.fmap(c => CounterCheckpoint(c, CantonTimestamp.MinValue, None)),
    )
}
