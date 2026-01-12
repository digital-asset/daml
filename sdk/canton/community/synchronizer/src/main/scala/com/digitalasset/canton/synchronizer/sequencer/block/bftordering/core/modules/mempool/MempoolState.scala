// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.mempool

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Mempool
import io.opentelemetry.api.trace.Span

import scala.collection.mutable

@SuppressWarnings(Array("org.wartremover.warts.Var"))
class MempoolState(weakQuorumSize: Int) {
  val receivedOrderRequests: mutable.Queue[(Mempool.OrderRequest, Span)] = mutable.Queue()
  var toBeProvidedToAvailability: Int = 0
  var weakQuorum: Int = weakQuorumSize
  var authenticatedCount: Int = 1

  def canDisseminate: Boolean = authenticatedCount >= weakQuorum
}
