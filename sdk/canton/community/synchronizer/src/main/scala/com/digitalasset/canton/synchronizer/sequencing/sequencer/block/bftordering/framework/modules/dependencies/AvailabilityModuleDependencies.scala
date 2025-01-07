// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.modules.dependencies

import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.ModuleRef
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.modules.{
  Consensus,
  Mempool,
  Output,
  P2PNetworkOut,
}

final case class AvailabilityModuleDependencies[E](
    mempool: ModuleRef[Mempool.Message],
    p2pNetworkOut: ModuleRef[P2PNetworkOut.Message],
    consensus: ModuleRef[Consensus.Message[E]],
    output: ModuleRef[Output.Message[E]],
)
