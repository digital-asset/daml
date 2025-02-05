// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering

import com.digitalasset.canton.topology.SequencerId

/** The class allows preserving some contextual information during the roundtrip from output to local availability
  * to retrieve the batches.
  *
  * @param isLastInEpoch From consensus: whether a block is the last in an epoch. Since the output module processes
  *                      blocks in order, this boolean information is enough to determine when an epoch ends and
  *                      the ordering topology for the next epoch may thus need to be sent to the consensus module.
  */
final case class OrderedBlockForOutput(
    orderedBlock: OrderedBlock,
    from: SequencerId, // Only used for metrics
    isLastInEpoch: Boolean,
    mode: OrderedBlockForOutput.Mode,
)

object OrderedBlockForOutput {

  sealed trait Mode extends Product with Serializable {

    /** If `true`, dissemination will use the current topology for the output pull protocol. */
    def isStateTransfer: Boolean = this match {
      case _: Mode.StateTransfer => true
      case Mode.FromConsensus => false
    }

    def mustSendTopologyToConsensus: Boolean = this match {
      case Mode.FromConsensus | Mode.StateTransfer.LastBlock => true
      case Mode.StateTransfer.MiddleBlock => false
    }
  }

  object Mode {

    case object FromConsensus extends Mode

    sealed trait StateTransfer extends Mode
    object StateTransfer {
      case object MiddleBlock extends StateTransfer
      case object LastBlock extends StateTransfer
    }
  }
}
