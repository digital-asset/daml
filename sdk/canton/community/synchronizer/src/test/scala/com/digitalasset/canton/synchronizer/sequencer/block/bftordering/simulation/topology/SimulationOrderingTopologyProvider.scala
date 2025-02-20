// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.simulation.topology

import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking.GrpcNetworking.P2PEndpoint
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.{
  CryptoProvider,
  OrderingTopologyProvider,
  TopologyActivationTime,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.{
  OrderingTopology,
  SequencingParameters,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.SimulationModuleSystem.{
  SimulationEnv,
  SimulationP2PNetworkManager,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.future.SimulationFuture
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TraceContext

import scala.util.Success

class SimulationOrderingTopologyProvider(
    thisPeer: SequencerId,
    getPeerEndpointsToTopologyData: () => Map[P2PEndpoint, SimulationTopologyData],
    loggerFactory: NamedLoggerFactory,
) extends OrderingTopologyProvider[SimulationEnv] {

  override def getOrderingTopologyAt(activationTime: TopologyActivationTime)(implicit
      traceContext: TraceContext
  ): SimulationFuture[Option[(OrderingTopology, CryptoProvider[SimulationEnv])]] =
    SimulationFuture(s"getOrderingTopologyAt($activationTime)") { () =>
      val activeSequencerTopologyData =
        getPeerEndpointsToTopologyData().view
          .filter { case (_, topologyData) =>
            topologyData.onboardingTime.value <= activationTime.value
          }
          .map { case (endpoint, topologyData) =>
            SimulationP2PNetworkManager.fakeSequencerId(endpoint) -> topologyData
          }
          .toMap

      val topology =
        OrderingTopology(
          activeSequencerTopologyData.view.mapValues(_.onboardingTime).toMap,
          SequencingParameters.Default,
          activationTime,
          // Switch the value deterministically so that we trigger all code paths.
          areTherePendingCantonTopologyChanges = activationTime.value.toMicros % 2 == 0,
        )
      Success(
        Some(
          topology -> SimulationCryptoProvider.create(
            thisPeer,
            activeSequencerTopologyData,
            activationTime.value,
            loggerFactory,
          )
        )
      )
    }
}
