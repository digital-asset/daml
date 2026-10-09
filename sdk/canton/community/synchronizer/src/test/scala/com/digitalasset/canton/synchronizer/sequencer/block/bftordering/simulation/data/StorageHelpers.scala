// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.simulation.data

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.OutputMetadataStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  BlockNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.SimulationModuleSystem.SimulationEnv
import com.digitalasset.canton.tracing.TraceContext

object StorageHelpers {

  /** Finds an output metadata store with the highest consecutive block number and its node ID.
    */
  def findMostAdvancedOutputStore(stores: Map[BftNodeId, OutputMetadataStore[SimulationEnv]])(
      implicit traceContext: TraceContext
  ): (BftNodeId, OutputMetadataStore[SimulationEnv]) =
    stores.maxBy { case (_, store) => // `maxBy` can throw, it's fine in tests
      store.getLastConsecutiveBlock
        .resolveValue()
        .toOption
        .flatMap(_.map(_.blockNumber))
        .getOrElse(
          // We want to prefer stores that have a last consecutive block that is 0 over stores that don't have any
          // consecutive block. That is why we make None map to -1L
          BlockNumber(-1L)
        )
    }
}
