// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.DisseminationProgress.reviewAcks
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  EpochNumber,
}
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
    acks: Set[AvailabilityAck],
) {

  // We allow dissemination progress with acks not in the topology to allow dissemination
  //  when the node is not part of the topology yet during onboarding, but we always
  //  produce valid PoAs.

  def proofOfAvailability(): Option[ProofOfAvailability] = {
    val reviewedAcks = reviewAcks(acks, orderingTopology)
    if (AvailabilityModule.hasQuorum(orderingTopology, reviewedAcks.size))
      Some(
        ProofOfAvailability(batchMetadata.batchId, reviewedAcks.toSeq, batchMetadata.epochNumber)
      )
    else
      None
  }

  def voteOf(nodeId: BftNodeId): Option[AvailabilityAck] =
    acks.find(_.from == nodeId)

  def review(currentOrderingTopology: OrderingTopology): DisseminationProgress =
    copy(
      orderingTopology = currentOrderingTopology,
      acks = reviewAcks(acks, currentOrderingTopology),
    )
}

object DisseminationProgress {

  // TODO(#24403): test this method
  def reviewReadyForOrdering(
      batchMetadata: DisseminatedBatchMetadata,
      orderingTopology: OrderingTopology,
  ): DisseminationProgress = {
    val inProgressMetadata =
      InProgressBatchMetadata(
        batchMetadata.proofOfAvailability.batchId,
        batchMetadata.epochNumber,
        batchMetadata.stats,
      )
    val reviewedAcks = reviewAcks(batchMetadata.proofOfAvailability.acks, orderingTopology)
    DisseminationProgress(
      orderingTopology,
      inProgressMetadata,
      reviewedAcks,
    )
  }

  private def reviewAcks(
      acks: Iterable[AvailabilityAck],
      currentOrderingTopology: OrderingTopology,
  ): Set[AvailabilityAck] =
    acks.filter(_.validateIn(currentOrderingTopology).isRight).toSet
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
