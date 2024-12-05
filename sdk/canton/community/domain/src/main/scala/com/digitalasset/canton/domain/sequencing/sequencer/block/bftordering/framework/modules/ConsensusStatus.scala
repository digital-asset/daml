// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules

import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  EpochNumber,
  ViewNumber,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.ordering.CommitCertificate
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.PbftNetworkMessage

object ConsensusStatus {

  final case class EpochStatus(epochNumber: EpochNumber, segments: Seq[SegmentStatus])

  sealed trait SegmentStatus

  object SegmentStatus {
    final object Complete extends SegmentStatus
    sealed trait Incomplete extends SegmentStatus {
      def viewNumber: ViewNumber
      def areBlocksComplete: Seq[Boolean]
    }

    final case class InProgress(viewNumber: ViewNumber, blockStatus: Seq[BlockStatus])
        extends Incomplete {
      override def areBlocksComplete: Seq[Boolean] = blockStatus.map(_.isComplete)
    }
    final case class InViewChange(
        viewNumber: ViewNumber,
        viewChangeMessages: Seq[Boolean],
        areBlocksComplete: Seq[Boolean],
    ) extends Incomplete
  }

  sealed trait BlockStatus {
    def isComplete: Boolean
  }

  object BlockStatus {
    final object Complete extends BlockStatus {
      override val isComplete: Boolean = true
    }
    final case class InProgress(
        prePrepared: Boolean,
        preparesPresent: Seq[Boolean],
        commitsPresent: Seq[Boolean],
    ) extends BlockStatus {
      override def isComplete: Boolean = false
    }
  }

  final case class RetransmissionResult(
      messagesToRetransmit: Seq[SignedMessage[PbftNetworkMessage]],
      commitCertsToRetransmit: Seq[CommitCertificate] = Seq.empty,
  )

  object RetransmissionResult {
    val empty = RetransmissionResult(Seq.empty, Seq.empty)
  }
}
