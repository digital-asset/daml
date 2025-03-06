// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology

import com.digitalasset.canton.topology.SequencerId
import com.google.common.annotations.VisibleForTesting

final case class Membership(
    myId: SequencerId,
    orderingTopology: OrderingTopology,
    leaders: Seq[SequencerId],
) {
  val otherPeers: Set[SequencerId] = orderingTopology.peers - myId
  lazy val sortedPeers: Seq[SequencerId] = orderingTopology.sortedPeers
}

object Membership {

  /** A simple constructor for tests so that we don't have to provide a full ordering topology. */
  @VisibleForTesting
  def forTesting(
      myId: SequencerId,
      otherPeers: Set[SequencerId] = Set.empty,
      sequencingParameters: SequencingParameters = SequencingParameters.Default,
      leaders: Option[Seq[SequencerId]] = None,
  ): Membership = {
    val orderingTopology = OrderingTopology(otherPeers + myId, sequencingParameters)
    val peers = orderingTopology.sortedPeers
    Membership(myId, orderingTopology, leaders.getOrElse(peers))
  }
}
