// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss

import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore.Block
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BlockNumber
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability.OrderingBlock
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.bfttime.CanonicalCommitSet
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.OrderedBlock
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.iss.BlockMetadata
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.Commit
import com.digitalasset.canton.tracing.TraceContext

import scala.collection.mutable

import EpochState.Epoch

/** Keeps track of progress of the segment this node is originally responsible for filling (leader)
  */
class OriginalLeaderSegmentState(
    state: SegmentState,
    epoch: Epoch,
    initialCompletedBlocks: Seq[Block],
    initialCurrentViewPrePrepareBlockNumbers: Seq[BlockNumber],
    override val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {
  private val segment = state.segment

  private val completedBlockIsEmpty: mutable.Map[BlockNumber, Boolean] =
    mutable.Map(
      initialCompletedBlocks.map(b =>
        b.blockNumber -> b.commitCertificate.prePrepare.message.block.proofs.isEmpty
      )*
    )

  private val blockedProgressDetector = {
    def isBlockComplete(blockNumber: BlockNumber) = completedBlockIsEmpty.contains(
      blockNumber
    ) || (segment.slotNumbers.contains(blockNumber) && state.isBlockComplete(blockNumber))

    def isBlockEmpty(blockNumber: BlockNumber) =
      completedBlockIsEmpty.getOrElse(blockNumber, false)

    new BlockedProgressDetector(
      state.segment,
      epoch.segments
        .filterNot(_.originalLeader == state.leader)
        .map(s => s.originalLeader -> s)
        .toMap,
      isBlockComplete,
      isBlockEmpty,
    )
  }

  def confirmCompleteBlockStored(blockNumber: BlockNumber, isEmpty: Boolean): Unit =
    completedBlockIsEmpty.put(blockNumber, isEmpty).discard

  def isProgressBlocked: Boolean =
    canReceiveProposals && blockedProgressDetector.isProgressBlocked(nextRelativeBlockToPropose)

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var nextRelativeBlockToPropose =
    // TODO(#16761): This assumes that a node's locally-assigned slots complete in order
    // Right now, this is guaranteed since each leader only works on one block at a time.
    // However, this may change in the future as we look to improve performance.
    segment.slotNumbers.count(blockNumber =>
      initialCompletedBlocks
        .map(_.blockNumber)
        .contains(blockNumber) || initialCurrentViewPrePrepareBlockNumbers
        .contains(blockNumber)
    )

  logger.debug(
    s"At segment creation with initialCompletedBlocks = ${initialCompletedBlocks.map(_.blockNumber)}, " +
      s"next relative block to propose = $nextRelativeBlockToPropose$absoluteNextBlockToProposeLogSuffix"
  )(TraceContext.empty)

  // `canReceiveProposals` determines whether this ordering node should request a proposal
  //  (of batches of submission requests) from the Availability module to sequence within the locally-owned segment.
  //
  // `canReceiveProposals` returns `false` if and only if any of the following conditions is met:
  //   - There are no more slots left to assign in the local segment
  //   - At least one view change occurred, the original segment leader is no longer in control,
  //       and only preexisting (partially progressed) or bottom blocks are allowed for the rest of the epoch
  //   - The previous local segment slot is still in progress (not yet completed)
  //
  // It is important to return `false` in these cases to prevent multiple outstanding proposal requests
  //  from being provided simultaneously, which would potentially exceed the Consensus segment module
  //  original leader's segment size, resulting in a potential `IndexOutOfBounds` exception in `assignToSlot`.
  def canReceiveProposals: Boolean =
    segment.slotNumbers.sizeIs > nextRelativeBlockToPropose && // we haven't filled all slots
      !viewChangeOccurred && // we haven't entered a view change ever in this epoch for our segment (view = 0)
      (nextRelativeBlockToPropose == 0 || state.isBlockComplete(
        segment.slotNumbers(nextRelativeBlockToPropose - 1)
      )) // we finished processing the current slot

  private def viewChangeOccurred: Boolean = state.currentView > 0

  def assignToSlot(
      blockToOrder: OrderingBlock,
      latestCompletedEpochLastCommits: Seq[SignedMessage[Commit]],
  )(implicit traceContext: TraceContext): OrderedBlock = {
    val lastStableCommits = if (nextRelativeBlockToPropose > 0) {
      val previousBlockNumberInSegment = segment.slotNumbers(nextRelativeBlockToPropose - 1)
      state.blockCommitMessages(previousBlockNumberInSegment)
    } else latestCompletedEpochLastCommits

    val blockMetadata = BlockMetadata(state.epoch.info.number, nextBlockToPropose)
    val orderedBlock =
      OrderedBlock(
        blockMetadata,
        blockToOrder.proofs,
        CanonicalCommitSet(lastStableCommits.toSet),
      )

    nextRelativeBlockToPropose += 1

    logger.debug(
      s"Next relative block to propose after assigning slot = $nextRelativeBlockToPropose$absoluteNextBlockToProposeLogSuffix"
    )

    orderedBlock
  }

  private def absoluteNextBlockToProposeLogSuffix =
    s" (segment = ${segment.slotNumbers}, " +
      (if (segment.slotNumbers.sizeIs > nextRelativeBlockToPropose)
         s"absolute = $nextBlockToPropose)"
       else s"beyond segment end at block ${segment.slotNumbers.last1})")

  def isNextSlotFirst: Boolean = nextRelativeBlockToPropose == 0

  def nextBlockToPropose: BlockNumber = segment.slotNumbers(nextRelativeBlockToPropose)
}
