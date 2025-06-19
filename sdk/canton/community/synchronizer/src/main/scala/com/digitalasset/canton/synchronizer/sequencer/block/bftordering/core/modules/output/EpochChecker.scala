// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.Membership

trait EpochChecker {
  def check(thisNode: BftNodeId, epochNumber: EpochNumber, membership: Membership): Unit
}

object EpochChecker {
  val DefaultEpochChecker: EpochChecker = new EpochChecker {
    override def check(
        thisNode: BftNodeId,
        epochNumber: EpochNumber,
        membership: Membership,
    ): Unit = ()
  }
}
