// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability.{
  BatchId,
  ProofOfAvailability,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.OrderedBlockForOutput
import com.digitalasset.canton.util.retry.Jitter

import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration
import scala.util.Random

@SuppressWarnings(Array("org.wartremover.warts.Var"))
final case class JitterStream(jitter: Jitter, initialDelay: FiniteDuration) {
  private var lastDelay: FiniteDuration = initialDelay
  private var lastAttempt: Int = 1

  def next(attempt: Int): FiniteDuration = {
    require(attempt >= lastAttempt)
    if (attempt >= lastAttempt) {
      lastDelay = jitter(initialDelay, lastDelay, attempt)
      lastAttempt = attempt
    }
    lastDelay
  }
}

object JitterStream {
  def create(config: BftBlockOrdererConfig, random: Random): JitterStream =
    JitterStream(
      Jitter.full(config.outputFetchTimeoutCap, Jitter.randomSource(random.self)),
      config.outputFetchTimeout,
    )
}

final case class MissingBatchStatus(
    batchId: BatchId,
    originalProof: ProofOfAvailability,
    remainingNodesToTry: Seq[BftNodeId],
    numberOfAttempts: Int,
    jitterStream: JitterStream,
    mode: OrderedBlockForOutput.Mode,
) {
  def calculateTimeout(): FiniteDuration = jitterStream.next(numberOfAttempts)
}

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
