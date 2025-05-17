// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.simulation.topology

import com.digitalasset.canton.TestEssentials
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking.GrpcNetworking.P2PEndpoint
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.{
  CryptoProvider,
  TopologyActivationTime,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.OrderingTopology
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.SimulationModuleSystem.SimulationEnv
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.SimulationSettings
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.future.SimulationFuture

import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps
import scala.util.Random

object SimulationTopologyHelpers extends TestEssentials {

  def generateNodeOnboardingDelay(
      durationOfFirstPhaseWithFaults: FiniteDuration,
      random: Random,
  ): FiniteDuration =
    // We only onboard sequencers during the first phase, so that we can check liveness uniformly across sequencers
    //  in the second phase.
    FiniteDuration(
      random.nextLong(durationOfFirstPhaseWithFaults.toJava.toMillis),
      java.util.concurrent.TimeUnit.MILLISECONDS,
    )

  def sequencerBecomeOnlineTime(
      onboardingTime: TopologyActivationTime,
      simSettings: SimulationSettings,
  ): CantonTimestamp =
    onboardingTime.value.plus(simSettings.becomingOnlineAfterOnboardingDelay.toJava)

  private val crypto =
    SymbolicCrypto.create(testedReleaseProtocolVersion, timeouts, loggerFactory)

  def generateSingleSimulationTopologyData(
      stageStart: CantonTimestamp,
      topologyDataFactory: NodeSimulationTopologyDataFactory,
  ): NodeSimulationTopologyData =
    topologyDataFactory.toSimulationTopologyData(stageStart, crypto)

  def generateSimulationTopologyData(
      stageStart: CantonTimestamp,
      endpointsToTopologyDataFactories: Map[P2PEndpoint, NodeSimulationTopologyDataFactory],
  ): Map[P2PEndpoint, NodeSimulationTopologyData] =
    endpointsToTopologyDataFactories.view.mapValues { timestamp =>
      generateSingleSimulationTopologyData(stageStart, timestamp)
    }.toMap

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
