// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.output.data.memory

import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.simulation.SimulationModuleSystem.SimulationEnv
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.simulation.future.SimulationFuture

import scala.util.Try

final class SimulationOutputBlockMetadataStore
    extends GenericInMemoryOutputBlockMetadataStore[SimulationEnv] {
  override protected def createFuture[T](action: String)(value: () => Try[T]): SimulationFuture[T] =
    SimulationFuture(value)
}
