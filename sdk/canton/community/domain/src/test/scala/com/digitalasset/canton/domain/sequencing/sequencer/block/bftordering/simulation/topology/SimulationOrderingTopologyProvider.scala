// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.simulation.topology

import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.topology.{
  CryptoProvider,
  OrderingTopologyProvider,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.topology.{
  OrderingTopology,
  SequencingParameters,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.simulation.SimulationModuleSystem.{
  SimulationEnv,
  SimulationP2PNetworkManager,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.simulation.future.SimulationFuture
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.topology.processing.EffectiveTime
import com.digitalasset.canton.tracing.TraceContext

import scala.util.Success

class SimulationOrderingTopologyProvider(
    peerEndpointsToOnboardingTimes: Map[Endpoint, EffectiveTime]
) extends OrderingTopologyProvider[SimulationEnv] {

  override def getOrderingTopologyAt(
      timestamp: EffectiveTime,
      assumePendingTopologyChanges: Boolean = false,
  )(implicit
      traceContext: TraceContext
  ): SimulationFuture[Option[(OrderingTopology, CryptoProvider[SimulationEnv])]] =
    SimulationFuture { () =>
      val peerIdsToOnboardingTimes = peerEndpointsToOnboardingTimes.view
        .filter { case (_, onboardingTime) =>
          timestamp.value >= onboardingTime.value
        }
        .map { case (endpoint, timestamp) =>
          SimulationP2PNetworkManager.fakeSequencerId(endpoint) -> timestamp
        }
        .toMap

      val topology =
        OrderingTopology(
          peerIdsToOnboardingTimes,
          SequencingParameters.Default,
          timestamp,
          areTherePendingCantonTopologyChanges = assumePendingTopologyChanges,
        )
      Success(Some(topology -> SimulationCryptoProvider))
    }
}
