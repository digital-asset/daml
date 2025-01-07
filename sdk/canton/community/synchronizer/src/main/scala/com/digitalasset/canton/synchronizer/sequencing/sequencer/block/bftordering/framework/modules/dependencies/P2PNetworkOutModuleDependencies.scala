// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.modules.dependencies

import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v1.BftOrderingServiceReceiveRequest
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.modules.{
  Availability,
  Consensus,
  Mempool,
  Output,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.{
  ClientP2PNetworkManager,
  Env,
  ModuleRef,
}

final case class P2PNetworkOutModuleDependencies[E <: Env[E]](
    p2pNetworkManager: ClientP2PNetworkManager[E, BftOrderingServiceReceiveRequest],
    p2pNetworkIn: ModuleRef[BftOrderingServiceReceiveRequest],
    mempool: ModuleRef[Mempool.Message],
    availability: ModuleRef[Availability.Message[E]],
    consensus: ModuleRef[Consensus.Message[E]],
    output: ModuleRef[Output.Message[E]],
)
