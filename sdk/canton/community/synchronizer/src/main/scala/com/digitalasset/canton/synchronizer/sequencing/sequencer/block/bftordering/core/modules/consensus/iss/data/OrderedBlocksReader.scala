// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.data

import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.Env
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.BlockNumber
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.ordering.OrderedBlockForOutput
import com.digitalasset.canton.tracing.TraceContext

trait OrderedBlocksReader[E <: Env[E]] {

  def loadOrderedBlocks(
      initialBlockNumber: BlockNumber
  )(implicit traceContext: TraceContext): E#FutureUnlessShutdownT[Seq[OrderedBlockForOutput]]

  protected def loadOrderedBlocksActionName(initialBlockNumber: BlockNumber): String =
    s"Load ordered blocks starting from $initialBlockNumber"
}
