// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.ordering

import com.digitalasset.canton.topology.SequencerId

/** The class allows preserving some contextual information (the info from consensus about whether a block
  * is the last in an epoch) during the roundtrip from output to local availability to retrieve the batches.
  * Since the output module processes blocks in order, this boolean information is enough to determine
  * when an epoch ends and topology should be queried.
  */
final case class OrderedBlockForOutput(
    orderedBlock: OrderedBlock,
    from: SequencerId, // Only used for metrics
    isLastInEpoch: Boolean,
    mode: OrderedBlockForOutput.Mode,
)

object OrderedBlockForOutput {

  sealed trait Mode extends Product with Serializable {

    def isStateTransfer: Boolean = this match {
      case Mode.StateTransfer | Mode.StateTransferLastBlock => true
      case Mode.FromConsensus => false
    }

    def isConsensusActive: Boolean = this match {
      case Mode.FromConsensus | Mode.StateTransferLastBlock => true
      case Mode.StateTransfer => false
    }
  }

  object Mode {

    case object FromConsensus extends Mode
    case object StateTransfer extends Mode
    case object StateTransferLastBlock extends Mode
  }
}
