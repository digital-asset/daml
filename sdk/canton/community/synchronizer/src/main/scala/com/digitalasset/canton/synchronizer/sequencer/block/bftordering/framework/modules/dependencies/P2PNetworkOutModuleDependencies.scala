// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
  P2PNetworkRefFactory,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.BftOrderingServiceReceiveRequest

final case class P2PNetworkOutModuleDependencies[
    E <: Env[E],
    P2PNetworkRefFactoryT <: P2PNetworkRefFactory[E, BftOrderingServiceReceiveRequest],
](
    createP2PNetworkRefFactory: P2PConnectionEventListener => P2PNetworkRefFactoryT,
    p2pNetworkIn: ModuleRef[BftOrderingServiceReceiveRequest],
    mempool: ModuleRef[Mempool.Message],
    availability: ModuleRef[Availability.Message[E]],
    consensus: ModuleRef[Consensus.Message[E]],
    output: ModuleRef[Output.Message[E]],
    pruning: ModuleRef[Pruning.Message],
)
