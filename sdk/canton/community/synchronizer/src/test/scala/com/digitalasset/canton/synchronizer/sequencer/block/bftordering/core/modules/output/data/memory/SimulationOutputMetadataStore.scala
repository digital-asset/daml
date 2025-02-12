// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.memory

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.SimulationModuleSystem.SimulationEnv
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.future.SimulationFuture
import com.digitalasset.canton.tracing.TraceContext

import scala.util.Try

final class SimulationOutputMetadataStore(
    fail: String => Unit
) extends GenericInMemoryOutputMetadataStore[SimulationEnv] {

  override protected def createFuture[T](action: String)(value: () => Try[T]): SimulationFuture[T] =
    SimulationFuture(action)(value)

  override def close(): Unit = ()

  override protected def reportError(errorMessage: String)(implicit
      traceContext: TraceContext
  ): Unit = fail(errorMessage)
}
