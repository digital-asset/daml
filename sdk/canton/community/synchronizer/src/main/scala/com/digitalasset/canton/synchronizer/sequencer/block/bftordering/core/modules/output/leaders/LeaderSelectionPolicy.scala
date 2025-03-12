// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.leaders

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.OrderingTopology

import scala.collection.immutable.SortedSet

import LeaderSelectionPolicy.rotateLeaders

trait LeaderSelectionPolicy {

  def selectLeaders(nodes: Set[BftNodeId]): SortedSet[BftNodeId]

  def getLeaders(orderingTopology: OrderingTopology, epochNumber: EpochNumber): Seq[BftNodeId] = {
    val selectedLeaders =
      selectLeaders(orderingTopology.nodes)
    rotateLeaders(selectedLeaders, epochNumber)
  }
}

object LeaderSelectionPolicy {

  def rotateLeaders(
      originalLeaders: SortedSet[BftNodeId],
      epochNumber: EpochNumber,
  ): Seq[BftNodeId] = {
    val splitIndex = (epochNumber % originalLeaders.size).toInt
    originalLeaders.drop(splitIndex).toSeq ++ originalLeaders.take(splitIndex).toSeq
  }
}
