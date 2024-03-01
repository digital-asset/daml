// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.integrations.state

import cats.syntax.functor.*
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.sequencer.store.CounterCheckpoint
import com.digitalasset.canton.domain.sequencing.sequencer.{
  InFlightAggregations,
  InternalSequencerPruningStatus,
}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.sequencing.protocol.TrafficState
import com.digitalasset.canton.topology.Member

/** State held in memory by [[com.digitalasset.canton.domain.block.BlockSequencerStateManager]] to keep track of:
  *
  * @param heads The latest counter value for members who have previously received an event
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
    trafficState: Map[Member, TrafficState],
) extends PrettyPrinting {
  val registeredMembers: Set[Member] = status.members.map(_.member).toSet
  val heads: Map[Member, SequencerCounter] = checkpoints.fmap(_.counter)

  /** Return true if the head counter for the member is above the genesis counter.
    * False otherwise
    */
  def headCounterAboveGenesis(member: Member): Boolean =
    heads.get(member).exists(_ > SequencerCounter.Genesis)

  assert(
    heads.keys.forall(registeredMembers.contains),
    s"All members with a head counter value must be registered. " +
      s"Members ${heads.toList.filterNot(h => registeredMembers.contains(h._1))} have head counters but are not registered.",
  )

  /** Next counter value for a single member.
    * Callers must check that the member has been previously registered otherwise a [[java.lang.IllegalArgumentException]] will be thrown.
    */
  def tryNextCounter(member: Member): SequencerCounter = {
    require(registeredMembers contains member, s"Member ($member) must be registered")

    heads.get(member).fold(SequencerCounter.Genesis)(_ + 1)
  }

  /** Generate the next counter value for the provided set of members.
    * Callers must check that all members have been registered otherwise a [[java.lang.IllegalArgumentException]] will be thrown.
    */
  def tryNextCounters(members: Set[Member]): Map[Member, SequencerCounter] =
    members.map { member =>
      (member, tryNextCounter(member))
    }.toMap

  def evictExpiredInFlightAggregations(upToInclusive: CantonTimestamp): EphemeralState =
    this.copy(
      inFlightAggregations = inFlightAggregations.filterNot { case (_, inFlightAggregation) =>
        inFlightAggregation.expired(upToInclusive)
      }
    )

  override def pretty: Pretty[EphemeralState] = prettyOfClass(
    param("heads", _.heads),
    param("in-flight aggregations", _.inFlightAggregations),
    param("status", _.status),
  )
}

object EphemeralState {
  val empty: EphemeralState = EphemeralState(Map.empty, Map.empty)
  def counterToCheckpoint(counter: SequencerCounter) =
    CounterCheckpoint(counter, CantonTimestamp.MinValue, None)

  def apply(
      heads: Map[Member, SequencerCounter],
      inFlightAggregations: InFlightAggregations,
      status: InternalSequencerPruningStatus = InternalSequencerPruningStatus.Unimplemented,
      trafficState: Map[Member, TrafficState] = Map.empty,
  ): EphemeralState =
    EphemeralState(
      inFlightAggregations,
      status,
      heads.fmap(c => CounterCheckpoint(c, CantonTimestamp.MinValue, None)),
      trafficState,
    )
}
