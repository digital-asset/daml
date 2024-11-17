// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.simulation.topology

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.topology.CryptoProvider
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.topology.OrderingTopology
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.simulation.SimulationModuleSystem.SimulationEnv
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.simulation.future.SimulationFuture
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.topology.processing.EffectiveTime
import com.digitalasset.canton.version.ReleaseProtocolVersion

object SimulationTopologyHelpers {

  def generateSimulationOnboardingInformation(
      peerEndpointsToOnboardingTimes: Map[Endpoint, EffectiveTime],
      loggerFactory: NamedLoggerFactory,
  ): Map[Endpoint, SimulationOnboardingInformation] = {
    val crypto =
      SymbolicCrypto.create(ReleaseProtocolVersion.latest, ProcessingTimeout(), loggerFactory)
    peerEndpointsToOnboardingTimes.view.mapValues { timestamp =>
      val keys = crypto.newSymbolicSigningKeyPair()
      SimulationOnboardingInformation(timestamp, keys.publicKey, keys.privateKey)
    }.toMap
  }

  def resolveOrderingTopology(
      orderingTopology: SimulationFuture[Option[(OrderingTopology, CryptoProvider[SimulationEnv])]]
  ): (OrderingTopology, CryptoProvider[SimulationEnv]) =
    orderingTopology
      .resolveValue()
      .toOption
      .flatten
      .getOrElse(
        throw new IllegalStateException(
          "Simulation ordering topology provider should never fail"
        )
      )
}
