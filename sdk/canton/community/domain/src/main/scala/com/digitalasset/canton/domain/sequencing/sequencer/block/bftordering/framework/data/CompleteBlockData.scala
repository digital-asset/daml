// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data

import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.availability.BatchId
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.ordering.OrderedBlockForOutput
import com.digitalasset.canton.tracing.Traced

import scala.collection.View

// This datatype is temporary for the purpose of giving the Output Module everything
// necessary to persist to the database and recover across crashes
final case class CompleteBlockData(
    orderedBlockForOutput: OrderedBlockForOutput,
    batches: Seq[(BatchId, OrderingRequestBatch)],
) {

  def requestsView: View[Traced[OrderingRequest]] =
    batches.view.map { case (_, batch) => batch }.flatMap(_.requests)
}
