// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.ordering

import com.digitalasset.canton.topology.SequencerId

/** The class allows preserving some contextual information during the roundtrip from output to local availability
  * to retrieve the batches.
  *
  * @param isLastInEpoch From consensus: whether a block is the last in an epoch. Since the output module processes
  *                      blocks in order, this boolean information is enough to determine when an epoch ends and
  *                      the ordering topology for the next epoch may thus need to be retrieved from the topology
  *                      client.
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

    def shouldQueryTopology: Boolean = this match {
      case Mode.FromConsensus => true
      // TODO(#19661): we should rather query the topology for the last state-transferred block but
      //  we assume that the initial sequencer topology for the onboarded node won't change
      //  up to and including the first epoch in which it switches from state transfer to consensus.
      case _: Mode.StateTransfer => false
    }
  }

  object Mode {

    case object FromConsensus extends Mode

    sealed trait StateTransfer extends Mode {
      def pendingTopologyChangesInNextEpoch: Boolean
    }
    object StateTransfer {
      final case class MiddleBlock(
          override val pendingTopologyChangesInNextEpoch: Boolean
      ) extends StateTransfer
      // TODO(#19661): we should rather get this info from the topology
      //  queried for the last state-transferred block (see above).
      final case class LastBlock(
          override val pendingTopologyChangesInNextEpoch: Boolean
      ) extends StateTransfer
    }
  }
}
