// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.mempool

import scala.concurrent.duration.FiniteDuration

final case class MempoolModuleConfig(
    maxQueueSize: Int,
    maxRequestPayloadBytes: Int,
    maxRequestsInBatch: Short,
    minRequestsInBatch: Short,
    maxBatchCreationInterval: FiniteDuration,
)
