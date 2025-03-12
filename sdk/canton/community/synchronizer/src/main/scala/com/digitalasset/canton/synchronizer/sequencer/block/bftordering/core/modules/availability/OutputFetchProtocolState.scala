// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability.{
  BatchId,
  ProofOfAvailability,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.OrderedBlockForOutput

import scala.collection.mutable

final case class MissingBatchStatus(
    batchId: BatchId,
    originalProof: ProofOfAvailability,
    remainingNodesToTry: Seq[BftNodeId],
    mode: OrderedBlockForOutput.Mode,
)

final class MainOutputFetchProtocolState {
  val localOutputMissingBatches: mutable.SortedMap[BatchId, MissingBatchStatus] =
    mutable.SortedMap.empty
  val incomingBatchRequests: mutable.Map[BatchId, Set[BftNodeId]] = mutable.SortedMap.empty
  val pendingBatchesRequests: mutable.ArrayDeque[BatchesRequest] = mutable.ArrayDeque.empty

  def findProofOfAvailabilityForMissingBatchId(
      missingBatchId: BatchId
  ): Option[ProofOfAvailability] = for {
    batchesRequest <- pendingBatchesRequests.find(_.missingBatches.contains(missingBatchId))
    proof <- batchesRequest.blockForOutput.orderedBlock.batchRefs.find(_.batchId == missingBatchId)
  } yield proof

  def removeRequestsWithNoMissingBatches(): Unit = {
    val _ = pendingBatchesRequests.removeAll(_.missingBatches.isEmpty)
  }
}

final class BatchesRequest(
    val blockForOutput: OrderedBlockForOutput,
    val missingBatches: mutable.SortedSet[BatchId],
)
