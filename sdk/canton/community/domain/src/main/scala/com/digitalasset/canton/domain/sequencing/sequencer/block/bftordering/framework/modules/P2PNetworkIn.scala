// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules

import com.digitalasset.canton.domain.sequencing.sequencer.bftordering.v1.BftOrderingServiceReceiveRequest
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.{
  Env,
  Module,
  ModuleRef,
}

trait P2PNetworkIn[E <: Env[E]] extends Module[E, BftOrderingServiceReceiveRequest] {
  def availability: ModuleRef[Availability.Message[E]]
  def consensus: ModuleRef[Consensus.Message[E]]
}
