// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  BlockNumber,
}

import EpochState.Segment

/** Tells whether the ordering node's local segment should sequence _some_ block, even if that block
  * is empty, because it is potentially blocking progress within the epoch (including across other
  * nodes' segments) or when it comes to BFT time.
  *
  * Created once per epoch; the state is transient to detect lack of progress and doesn't need to be
  * preserved across crashes.
  */
class BlockedProgressDetector(
    leaderToSegmentState: Map[BftNodeId, Segment],
    isBlockComplete: BlockNumber => Boolean,
    isBlockEmpty: BlockNumber => Boolean,
) {

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var previousBlockCompletionState: Option[Seq[Boolean]] = None

  def isProgressBlocked(nextRelativeBlockIndexToFill: Int): Boolean =
    isEpochProgressBlocked(nextRelativeBlockIndexToFill) || isNetworkSilent

  // The heuristic we use asks the following question:
  // As a leader, for the next relative block index I must fill, are there leaders in other
  // segments that have a non-empty block already ordered at the same relative index or higher?
  // If another leader has completed the block on the last relative index in their segment,
  // we also consider that we're blocking progress regardless of that block being empty or not,
  // because in that case we are blocking the completion of the epoch for that leader so they can
  // start working on the next one.
  private def isEpochProgressBlocked(nextRelativeBlockIndexToFill: Int): Boolean =
    leaderToSegmentState.values
      .map(_.slotNumbers)
      .exists { slotNumbers =>
        isBlockComplete(slotNumbers.last1) ||
        slotNumbers
          .drop(nextRelativeBlockIndexToFill)
          .exists(slot => isBlockComplete(slot) && !isBlockEmpty(slot))
      }

  // Since BFT time is decided using a previous block, if there is no traffic for a while, we need to signalize that
  // and order an empty block. Otherwise, the first block (and its transactions) afterwards could have a time that
  // appears to be quite in the past.
  // Note that the below logic may result in ordering empty blocks from multiple nodes "at the same time".
  // It should not be a big deal if we order multiple empty blocks when there is no traffic as long as the empty
  // proposal creation interval is not too short. Also, empty blocks (at least so far) do not leak outside the ordering
  // layer.
  private def isNetworkSilent = {
    val currentBlockCompletions = Some(
      leaderToSegmentState.view.values.flatMap(_.slotNumbers).map(isBlockComplete).toSeq
    )
    val haveCompletedBlocksNotChanged = currentBlockCompletions == previousBlockCompletionState
    previousBlockCompletionState = currentBlockCompletions
    haveCompletedBlocksNotChanged
  }
}
