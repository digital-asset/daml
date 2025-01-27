// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.topology.SequencerId

trait SimulationVerifier {
  def checkInvariants(at: CantonTimestamp): Unit

  def resumeCheckingLiveness(at: CantonTimestamp): Unit

  def aFutureHappened(peer: SequencerId): Unit
}

case object NoVerification extends SimulationVerifier {
  override def checkInvariants(at: CantonTimestamp): Unit = ()

  override def resumeCheckingLiveness(at: CantonTimestamp): Unit = ()

  override def aFutureHappened(peer: SequencerId): Unit = ()
}

object SimulationVerifier {
  def onlyCheckInvariant(checker: CantonTimestamp => Unit): SimulationVerifier =
    new SimulationVerifier {
      override def checkInvariants(at: CantonTimestamp): Unit = checker(at)

      override def resumeCheckingLiveness(at: CantonTimestamp): Unit = ()

      override def aFutureHappened(peer: SequencerId): Unit = ()
    }
}
