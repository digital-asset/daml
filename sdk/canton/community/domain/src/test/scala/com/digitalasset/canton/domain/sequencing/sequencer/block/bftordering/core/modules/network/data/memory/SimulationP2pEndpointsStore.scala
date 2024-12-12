// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.network.data.memory

import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.networking.data.memory.GenericInMemoryP2pEndpointsStore
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.simulation.SimulationModuleSystem.SimulationEnv
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.simulation.future.SimulationFuture
import com.digitalasset.canton.networking.Endpoint

import scala.util.Try

final class SimulationP2pEndpointsStore(initialEndpoints: Set[Endpoint] = Set.empty)
    extends GenericInMemoryP2pEndpointsStore[SimulationEnv](initialEndpoints) {
  override def createFuture[A](action: String)(x: () => Try[A]): SimulationFuture[A] =
    SimulationFuture(x)
  override def close(): Unit = ()
}
