// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.consensus.iss

import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.BlockNumber
import com.digitalasset.canton.topology.SequencerId

import EpochState.Segment

/** Tells whether the ordering node's local segment should sequence _some_ block, even if that block is empty,
  * because it is potentially blocking progress within the epoch (including across other nodes' segments)
  * or when it comes to BFT time.
  *
  * Created once per epoch; the state is transient to detect lack of progress and doesn't need to be preserved across crashes.
  */
class BlockedProgressDetector(
    epochStartBlockNumber: BlockNumber,
    mySegmentState: Option[LeaderSegmentState],
    leaderToSegmentState: Map[SequencerId, Segment],
    isBlockComplete: BlockNumber => Boolean,
) {

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var previousBlockCompletionState: Option[Seq[Boolean]] = None

  def isProgressBlocked: Boolean =
    mySegmentState.exists { mySegment =>
      mySegment.moreSlotsToAssign && (isEpochProgressBlocked(mySegment) || isNetworkSilent)
    }

  private def isEpochProgressBlocked(mySegment: LeaderSegmentState) = {
    val nextSlotToFill = mySegment.nextSlotToFill
    val allPreviousSlotsFilled =
      (epochStartBlockNumber until nextSlotToFill).forall(n => isBlockComplete(BlockNumber(n)))
    epochStartBlockNumber != nextSlotToFill && allPreviousSlotsFilled
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
