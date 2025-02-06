// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.network.data.memory

import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking.data.memory.GenericInMemoryP2pEndpointsStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.SimulationModuleSystem.SimulationEnv
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.future.SimulationFuture

import scala.util.Try

final class SimulationP2pEndpointsStore(initialEndpoints: Set[Endpoint] = Set.empty)
    extends GenericInMemoryP2pEndpointsStore[SimulationEnv](initialEndpoints) {
  override def createFuture[A](action: String)(x: () => Try[A]): SimulationFuture[A] =
    SimulationFuture(action)(x)
  override def close(): Unit = ()
}
