// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.mempool

import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.Mempool

import scala.collection.mutable

@SuppressWarnings(Array("org.wartremover.warts.Var"))
class MempoolState {
  val receivedOrderRequests: mutable.Queue[Mempool.OrderRequest] = mutable.Queue()
  var toBeProvidedToAvailability: Int = 0
}
