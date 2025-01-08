// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.topology

import com.digitalasset.canton.topology.SequencerId
import com.google.common.annotations.VisibleForTesting

final case class Membership(myId: SequencerId, orderingTopology: OrderingTopology) {
  val otherPeers: Set[SequencerId] = orderingTopology.peers - myId
  val sortedPeers = orderingTopology.peers.toList.sorted
}

object Membership {

  /** A simple constructor for tests so that we don't have to provide a full ordering topology. */
  @VisibleForTesting
  def apply(
      myId: SequencerId,
      otherPeers: Set[SequencerId] = Set.empty,
      sequencingParameters: SequencingParameters = SequencingParameters.Default,
  ): Membership =
    Membership(myId, OrderingTopology(otherPeers + myId, sequencingParameters))
}
