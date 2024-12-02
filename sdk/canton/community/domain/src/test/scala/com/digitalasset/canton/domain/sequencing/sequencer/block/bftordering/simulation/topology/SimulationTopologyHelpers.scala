// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.simulation.topology

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.topology.{
  CryptoProvider,
  TopologyActivationTime,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.topology.OrderingTopology
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.simulation.SimulationModuleSystem.SimulationEnv
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.simulation.SimulationSettings
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.simulation.future.SimulationFuture
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.simulation.BftOrderingSimulationTest.SimulationStartTime
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.version.ReleaseProtocolVersion

import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps
import scala.util.Random

object SimulationTopologyHelpers {

  def generatePeerOnboardingTime(
      durationOfFirstPhaseWithFaults: FiniteDuration,
      random: Random,
  )(initialTime: TopologyActivationTime): TopologyActivationTime = {
    val initialTimeEpochMilli = initialTime.value.toEpochMilli
    val simulationEndTimeEpochMilli = SimulationStartTime
      // We only onboard sequencers during the first phase, so that we can check liveness uniformly across sequencers
      //  in the second phase.
      .plus(durationOfFirstPhaseWithFaults.toJava)
      .toEpochMilli
    val onboardingTimeEpochMilli =
      initialTimeEpochMilli + random.nextLong(simulationEndTimeEpochMilli - initialTimeEpochMilli)
    TopologyActivationTime(CantonTimestamp.ofEpochMilli(onboardingTimeEpochMilli))
  }

  def sequencerBecomeOnlineTime(
      onboardingTime: TopologyActivationTime,
      simSettings: SimulationSettings,
  ): CantonTimestamp =
    onboardingTime.value.plus(simSettings.becomingOnlineAfterOnboardingDelay.toJava)

  def generateSimulationTopologyData(
      peerEndpointsToOnboardingTimes: Map[Endpoint, TopologyActivationTime],
      loggerFactory: NamedLoggerFactory,
  ): Map[Endpoint, SimulationTopologyData] = {
    val crypto =
      SymbolicCrypto.create(ReleaseProtocolVersion.latest, ProcessingTimeout(), loggerFactory)
    peerEndpointsToOnboardingTimes.view.mapValues { timestamp =>
      val keys = crypto.newSymbolicSigningKeyPair()
      SimulationTopologyData(timestamp, keys.publicKey, keys.privateKey)
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
