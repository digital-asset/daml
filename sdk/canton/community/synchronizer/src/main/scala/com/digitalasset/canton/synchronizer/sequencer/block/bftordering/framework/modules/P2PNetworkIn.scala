// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{
  Env,
  Module,
  ModuleRef,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.BftOrderingMessage

trait P2PNetworkIn[E <: Env[E]] extends Module[E, BftOrderingMessage] {
  def availability: ModuleRef[Availability.Message[E]]
  def consensus: ModuleRef[Consensus.Message[E]]
}
