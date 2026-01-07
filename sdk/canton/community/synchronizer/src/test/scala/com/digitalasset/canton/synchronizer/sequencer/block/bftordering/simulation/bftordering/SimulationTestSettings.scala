// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.simulation.bftordering

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.P2PGrpcNetworking.P2PEndpoint
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig.DefaultEpochLength
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.EpochLength
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.{
  PowerDistribution,
  SimulationSettings,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.simulation.bftordering.TopologySettings.{
  defaultKeyAdditionDistribution,
  defaultKeyExpirationDistribution,
  defaultOffboardDistribution,
}

import scala.concurrent.duration.{DurationInt, FiniteDuration}

final case class TopologySettings(
    randomSeed: Long,
    shouldDoKeyRotations: Boolean = false,
    keyExpirationDistribution: PowerDistribution = defaultKeyExpirationDistribution,
    keyAdditionDistribution: PowerDistribution = defaultKeyAdditionDistribution,
    nodeOnboardingDelays: Iterable[FiniteDuration] = Iterable.empty,
    becomingOnlineAfterOnboardingDelay: FiniteDuration =
      TopologySettings.DefaultBecomingOnlineAfterOnboardingDelay,
    retryBecomingOnlineInterval: FiniteDuration = 1.second,
    nodesToOffboard: Seq[P2PEndpoint] = Seq.empty,
    offboardDistribution: PowerDistribution = defaultOffboardDistribution,
    crashAfterOnboardDistribution: Option[PowerDistribution] = None,
)

object TopologySettings {
  private val defaultKeyExpirationDistribution: PowerDistribution =
    PowerDistribution(10 seconds, 25 seconds)
  private val defaultKeyAdditionDistribution: PowerDistribution =
    PowerDistribution(10 seconds, 25 seconds)
  val DefaultBecomingOnlineAfterOnboardingDelay: FiniteDuration = 15.seconds
  private val defaultOffboardDistribution: PowerDistribution =
    PowerDistribution(10 seconds, 25 seconds)
}

final case class SimulationTestStageSettings(
    simulationSettings: SimulationSettings,
    topologySettings: TopologySettings,
    failOnViewChange: Boolean = false,
)

final case class SimulationTestSettings(
    numberOfInitialNodes: Int,
    epochLength: EpochLength = DefaultEpochLength,
    stages: NonEmpty[Seq[SimulationTestStageSettings]],
)
