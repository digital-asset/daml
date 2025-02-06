// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.leaders

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.NumberIdentifiers.EpochNumber
import com.digitalasset.canton.topology.SequencerId

import scala.collection.immutable.SortedSet

trait LeaderSelectionPolicy {

  def selectLeaders(peers: Set[SequencerId]): SortedSet[SequencerId]

  def rotateLeaders(
      originalLeaders: SortedSet[SequencerId],
      epochNumber: EpochNumber,
  ): Seq[SequencerId] = {
    val splitIndex = (epochNumber % originalLeaders.size).toInt
    originalLeaders.drop(splitIndex).toSeq ++ originalLeaders.take(splitIndex).toSeq
  }
}
