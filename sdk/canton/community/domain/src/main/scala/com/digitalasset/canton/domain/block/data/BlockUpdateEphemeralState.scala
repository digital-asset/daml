// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.block.data

import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.sequencer.store.CounterCheckpoint
import com.digitalasset.canton.domain.sequencing.sequencer.{
  InFlightAggregations,
  InternalSequencerMemberStatus,
}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.sequencing.protocol.TrafficState
import com.digitalasset.canton.topology.Member

/** Subset of the [[EphemeralState]] that is used by the block processing stage
  * of the [[com.digitalasset.canton.domain.block.update.BlockUpdateGenerator]]
  */
final case class BlockUpdateEphemeralState(
    checkpoints: Map[Member, CounterCheckpoint],
    inFlightAggregations: InFlightAggregations,
    membersMap: Map[Member, InternalSequencerMemberStatus],
    trafficState: Map[Member, TrafficState],
) extends PrettyPrinting {

  /** Return true if the head counter for the member is above the genesis counter.
    * False otherwise
    */
  def headCounterAboveGenesis(member: Member): Boolean =
    checkpoints.get(member).exists(_.counter > SequencerCounter.Genesis)

  def registeredMembers: Set[Member] = membersMap.keySet

  def headCounter(member: Member): Option[SequencerCounter] = checkpoints.get(member).map(_.counter)

  /** Next counter value for a single member.
    * Callers must check that the member has been previously registered otherwise a [[java.lang.IllegalArgumentException]] will be thrown.
    */
  def tryNextCounter(member: Member): SequencerCounter = {
    require(registeredMembers contains member, s"Member ($member) must be registered")

    headCounter(member).fold(SequencerCounter.Genesis)(_ + 1)
  }

  def evictExpiredInFlightAggregations(upToInclusive: CantonTimestamp): BlockUpdateEphemeralState =
    this.copy(
      inFlightAggregations = inFlightAggregations.filterNot { case (_, inFlightAggregation) =>
        inFlightAggregation.expired(upToInclusive)
      }
    )

  override def pretty: Pretty[BlockUpdateEphemeralState] = prettyOfClass(
    param("checkpoints", _.checkpoints),
    param("in-flight aggregations", _.inFlightAggregations),
    param("members map", _.membersMap),
    param("traffic state", _.trafficState),
  )
}
