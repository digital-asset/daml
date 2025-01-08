// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.modules.dependencies

import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.ModuleRef
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.modules.{
  Availability,
  Output,
  P2PNetworkOut,
}

final case class ConsensusModuleDependencies[E](
    availability: ModuleRef[Availability.Message[E]],
    output: ModuleRef[Output.Message[E]],
    p2pNetworkOut: ModuleRef[P2PNetworkOut.Message],
)
