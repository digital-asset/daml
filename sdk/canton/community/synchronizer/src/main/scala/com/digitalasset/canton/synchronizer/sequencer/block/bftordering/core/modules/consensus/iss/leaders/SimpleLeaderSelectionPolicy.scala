// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.leaders

import com.digitalasset.canton.topology.SequencerId

import scala.collection.immutable.SortedSet

/** A simple leader selection policy based on the one from the ISS paper. It returns a sorted set of
  * all peers.
  */
object SimpleLeaderSelectionPolicy extends LeaderSelectionPolicy {

  override def selectLeaders(peers: Set[SequencerId]): SortedSet[SequencerId] =
    SortedSet.from(peers)
}
