// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId

trait SimulationVerifier {
  def checkInvariants(at: CantonTimestamp): Unit

  def resumeCheckingLiveness(at: CantonTimestamp): Unit

  def nodeStarted(at: CantonTimestamp, node: BftNodeId): Unit

  def aFutureHappened(node: BftNodeId): Unit
}

case object NoVerification extends SimulationVerifier {
  override def checkInvariants(at: CantonTimestamp): Unit = ()

  override def resumeCheckingLiveness(at: CantonTimestamp): Unit = ()

  override def nodeStarted(at: CantonTimestamp, node: BftNodeId): Unit = ()

  override def aFutureHappened(node: BftNodeId): Unit = ()
}

object SimulationVerifier {
  def onlyCheckInvariant(checker: CantonTimestamp => Unit): SimulationVerifier =
    new SimulationVerifier {
      override def checkInvariants(at: CantonTimestamp): Unit = checker(at)

      override def resumeCheckingLiveness(at: CantonTimestamp): Unit = ()

      override def nodeStarted(at: CantonTimestamp, node: BftNodeId): Unit = ()

      override def aFutureHappened(node: BftNodeId): Unit = ()
    }
}
