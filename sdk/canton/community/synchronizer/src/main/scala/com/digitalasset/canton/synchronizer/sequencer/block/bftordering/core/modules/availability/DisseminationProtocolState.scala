// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.NumberIdentifiers.EpochNumber
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability.{
  AvailabilityAck,
  BatchId,
  DisseminatedBatchMetadata,
  InProgressBatchMetadata,
  ProofOfAvailability,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.OrderingTopology

import scala.collection.mutable

final case class DisseminationProgress(
    orderingTopology: OrderingTopology,
    batchMetadata: InProgressBatchMetadata,
    votes: Set[AvailabilityAck],
) {

  def proofOfAvailability(): Option[ProofOfAvailability] =
    if (AvailabilityModule.hasQuorum(orderingTopology, votes.size))
      Some(ProofOfAvailability(batchMetadata.batchId, votes.toSeq, batchMetadata.expirationTime))
    else
      None
}

@SuppressWarnings(Array("org.wartremover.warts.Var"))
final class DisseminationProtocolState(
    var disseminationProgress: mutable.SortedMap[BatchId, DisseminationProgress] =
      mutable.SortedMap.empty,
    var batchesReadyForOrdering: mutable.LinkedHashMap[BatchId, DisseminatedBatchMetadata] =
      mutable.LinkedHashMap(),
    val toBeProvidedToConsensus: mutable.Queue[ToBeProvidedToConsensus] = mutable.Queue(),
    var lastProposalTime: Option[CantonTimestamp] = None,
)

final case class ToBeProvidedToConsensus(maxBatchesPerProposal: Short, forEpochNumber: EpochNumber)
