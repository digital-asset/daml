// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.utils

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId

import scala.util.Random

// "Everyday I'm shufflin'" ♪,♪,♪
final class BftNodeShuffler(random: Random) {
  import BftNodeShuffler.*

  def shuffle(bftNodes: Seq[BftNodeId]): ShuffledBftNodes = {
    val shuffled = random.shuffle(bftNodes)
    ShuffledBftNodes(shuffled.headOption, shuffled.drop(1))
  }
}

object BftNodeShuffler {
  final case class ShuffledBftNodes(headOption: Option[BftNodeId], tail: Seq[BftNodeId])
}
