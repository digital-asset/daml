// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.memory

import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.memory.GenericInMemoryOutputMetadataStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.SimulationModuleSystem.SimulationEnv
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.future.SimulationFuture

import scala.util.Try

final class SimulationOutputMetadataStore(
    override val loggerFactory: NamedLoggerFactory
) extends GenericInMemoryOutputMetadataStore[SimulationEnv](loggerFactory) {

  override protected def createFuture[T](action: String)(value: () => Try[T]): SimulationFuture[T] =
    SimulationFuture(value)

  override def close(): Unit = ()
}
