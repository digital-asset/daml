// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.availability.data.model

import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.OrderingRequestBatch
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.availability.BatchId

sealed trait Command

object Command {
  final case class AddBatch(batchId: BatchId, batch: OrderingRequestBatch) extends Command

  final case class FetchBatches(batches: Seq[BatchId]) extends Command

  final case class GC(staleBatchIds: Seq[BatchId]) extends Command
}
