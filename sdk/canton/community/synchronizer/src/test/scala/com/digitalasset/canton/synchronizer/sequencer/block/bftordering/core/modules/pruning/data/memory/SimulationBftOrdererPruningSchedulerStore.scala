// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.pruning.data.memory

import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.logging.NoLogging
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.SimulationModuleSystem.SimulationEnv
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.future.SimulationFuture

import scala.concurrent.ExecutionContext
import scala.util.Try

class SimulationBftOrdererPruningSchedulerStore
    extends GenericInMemoryBftOrdererPruningSchedulerStore[SimulationEnv] {
  override protected def createFuture[T](action: String)(value: () => Try[T]): SimulationFuture[T] =
    SimulationFuture(action)(value)

  override def close(): Unit = ()

  override implicit def ec: ExecutionContext = DirectExecutionContext(NoLogging.logger)
}
