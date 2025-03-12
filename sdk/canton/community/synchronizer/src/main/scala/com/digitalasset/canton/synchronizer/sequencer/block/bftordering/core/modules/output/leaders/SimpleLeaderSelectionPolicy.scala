// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.leaders

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId

import scala.collection.immutable.SortedSet

/** A simple leader selection policy based on the one from the ISS paper. It returns a sorted set of
  * all nodes.
  */
object SimpleLeaderSelectionPolicy extends LeaderSelectionPolicy {

  override def selectLeaders(nodes: Set[BftNodeId]): SortedSet[BftNodeId] =
    SortedSet.from(nodes)
}
