// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore.Block
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.NumberIdentifiers.BlockNumber
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability.OrderingBlock
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.bfttime.CanonicalCommitSet
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.OrderedBlock
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.iss.BlockMetadata
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.Commit

import scala.collection.mutable

import EpochState.Epoch

/** Keeps track of progress of the segment this node is responsible for filling (leader)
  */
class LeaderSegmentState(
    state: SegmentState,
    epoch: Epoch,
    initialCompletedBlocks: Seq[Block],
) {
  private val segment = state.segment

  private val completedBlocks: mutable.Set[BlockNumber] =
    mutable.Set[BlockNumber](initialCompletedBlocks.map(_.blockNumber)*)

  private val blockedProgressDetector = {

    def isBlockComplete(blockNumber: BlockNumber) = completedBlocks.contains(
      blockNumber
    ) || (segment.slotNumbers.contains(blockNumber) && state.isBlockComplete(blockNumber))

    new BlockedProgressDetector(
      epoch.info.startBlockNumber,
      Some(this),
      epoch.segments.map(s => s.originalLeader -> s).toMap,
      isBlockComplete,
    )
  }

  def confirmCompleteBlockStored(blockNumber: BlockNumber): Boolean =
    completedBlocks.add(blockNumber)

  def isProgressBlocked: Boolean = blockedProgressDetector.isProgressBlocked

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var nextRelativeBlockToOrder =
    // TODO(#16761): This assumes that a node's locally-assigned slots complete in order
    // Right now, this is guaranteed since each leader only works on one block at a time.
    // However, this may change in the future as we look to improve performance.
    segment.slotNumbers.count(initialCompletedBlocks.map(_.blockNumber).contains)

  // `moreSlotsToAssign` determines whether this ordering node should request a proposal
  // (of transactions) from the Availability module to sequence within the locally-owned segment.
  // Reasons that `moreSlotsToAssign` returns false include:
  //   - If there are no more slots left to assign in the local segment
  //   - If at least one view change occurred, the original segment leader is no longer in control,
  //       and only preexisting (partially progressed) or bottom blocks are allowed for the rest of the epoch
  //   - The previous local segment slot is still in progress (not yet completed)
  // It is important to return false in these cases to prevent multiple outstanding proposal requests
  // from being provided simultaneously, which would potentially overwhelm the Consensus module, resulting
  // in a potential `IndexOutOfBounds` exception.
  def moreSlotsToAssign: Boolean =
    segment.slotNumbers.sizeIs > nextRelativeBlockToOrder && // we haven't filled all slots
      !viewChangeOccurred && // we haven't entered a view change ever in this epoch for our segment (view = 0)
      (nextRelativeBlockToOrder == 0 || state.isBlockComplete(
        segment.slotNumbers(nextRelativeBlockToOrder - 1)
      )) // we finished processing the current slot

  private def viewChangeOccurred: Boolean = state.currentView > 0

  def assignToSlot(
      blockToOrder: OrderingBlock,
      latestCompletedEpochLastCommits: Seq[SignedMessage[Commit]],
  ): OrderedBlock = {
    val lastStableCommits = if (nextRelativeBlockToOrder > 0) {
      val previousBlockNumberInSegment = segment.slotNumbers(nextRelativeBlockToOrder - 1)
      state.blockCommitMessages(previousBlockNumberInSegment)
    } else latestCompletedEpochLastCommits

    val blockNumber = segment.slotNumbers(nextRelativeBlockToOrder)
    val blockMetadata = BlockMetadata(state.epoch.info.number, blockNumber)
    val orderedBlock =
      OrderedBlock(
        blockMetadata,
        blockToOrder.proofs,
        CanonicalCommitSet(lastStableCommits.toSet),
      )

    nextRelativeBlockToOrder += 1

    orderedBlock
  }

  def nextBlockNumberToFill: BlockNumber = segment.slotNumbers(nextRelativeBlockToOrder)

  def isNextSlotFirst: Boolean = nextRelativeBlockToOrder == 0
}
