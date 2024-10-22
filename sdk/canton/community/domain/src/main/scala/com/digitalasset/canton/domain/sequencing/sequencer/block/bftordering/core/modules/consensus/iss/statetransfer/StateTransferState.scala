// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.statetransfer

import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.EpochNumber
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.PrePrepare

sealed trait StateTransferState

object StateTransferState {
  final case class InProgress(private val prePrepares: Seq[PrePrepare]) extends StateTransferState {

    // Wrap for additional type-safety and clarity.
    def prePreparesFromState: PrePreparesFromState = PrePreparesFromState(prePrepares)
  }
  final case class Done(epochNumber: EpochNumber) extends StateTransferState

  final case class PrePreparesFromState(prePrepares: Seq[PrePrepare])
}
