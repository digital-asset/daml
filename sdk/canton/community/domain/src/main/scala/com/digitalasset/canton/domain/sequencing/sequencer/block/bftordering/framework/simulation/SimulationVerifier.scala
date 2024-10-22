// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.simulation

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.topology.SequencerId

trait SimulationVerifier {
  def checkInvariants(at: CantonTimestamp): Unit

  def simulationIsGoingHealthy(at: CantonTimestamp): Unit

  def aFutureHappened(peer: SequencerId): Unit
}

case object NoVerification extends SimulationVerifier {
  override def checkInvariants(at: CantonTimestamp): Unit = ()

  override def simulationIsGoingHealthy(at: CantonTimestamp): Unit = ()

  override def aFutureHappened(peer: SequencerId): Unit = ()
}

object SimulationVerifier {
  def onlyCheckInvariant(checker: CantonTimestamp => Unit): SimulationVerifier =
    new SimulationVerifier {
      override def checkInvariants(at: CantonTimestamp): Unit = checker(at)

      override def simulationIsGoingHealthy(at: CantonTimestamp): Unit = ()

      override def aFutureHappened(peer: SequencerId): Unit = ()
    }
}
