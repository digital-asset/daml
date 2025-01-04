// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.availability.data.memory

import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.OrderingRequestBatch
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.availability.BatchId
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.simulation.SimulationModuleSystem.SimulationEnv
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.simulation.future.SimulationFuture

import scala.collection.mutable
import scala.util.Try

final class SimulationAvailabilityStore(
    allKnownBatchesById: mutable.Map[BatchId, OrderingRequestBatch] = mutable.Map.empty
) extends GenericInMemoryAvailabilityStore[SimulationEnv](allKnownBatchesById) {
  override def createFuture[A](action: String)(x: () => Try[A]): SimulationFuture[A] =
    SimulationFuture(x)
  override def close(): Unit = ()
}
