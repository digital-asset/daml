// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.dependencies

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.{
  Availability,
  Consensus,
  Mempool,
  Output,
  Pruning,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{
  Env,
  ModuleRef,
  P2PConnectionEventListener,
  P2PNetworkManager,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.BftOrderingMessage

final case class P2PNetworkOutModuleDependencies[
    E <: Env[E],
    P2PNetworkManagerT <: P2PNetworkManager[E, BftOrderingMessage],
](
    createP2PNetworkManager: (
        P2PConnectionEventListener,
        ModuleRef[BftOrderingMessage],
    ) => P2PNetworkManagerT,
    p2pNetworkIn: ModuleRef[BftOrderingMessage],
    mempool: ModuleRef[Mempool.Message],
    availability: ModuleRef[Availability.Message[E]],
    consensus: ModuleRef[Consensus.Message[E]],
    output: ModuleRef[Output.Message[E]],
    pruning: ModuleRef[Pruning.Message],
)
