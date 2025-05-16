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
import com.digitalasset.canton.util.BooleanUtil.implicits.*

import java.time.Instant
import scala.collection.mutable

final case class InitialSaveInProgress(
    // The following fields are only used for metrics
    availabilityEnterInstant: Option[Instant] = None
)

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
    DisseminationProgress.voteOf(nodeId, acks)

  def review(
      thisNodeId: BftNodeId,
      newOrderingTopology: OrderingTopology,
  ): DisseminationProgress = {
    val reviewedAcks = reviewAcks(acks, newOrderingTopology)
    lazy val lostAcks =
      math.max(0, acks.size - reviewedAcks.size)
    lazy val weakQuorumDecrease =
      math.max(0, orderingTopology.weakQuorum - newOrderingTopology.weakQuorum)
    val regressedToSigning = DisseminationProgress.voteOf(thisNodeId, reviewedAcks).isEmpty
    val disseminationRegressed = !regressedToSigning && lostAcks > weakQuorumDecrease
    copy(
      orderingTopology = newOrderingTopology,
      acks = reviewedAcks,
      batchMetadata = batchMetadata.copy(
        regressionsToSigning = batchMetadata.regressionsToSigning + regressedToSigning.toInt,
        disseminationRegressions =
          batchMetadata.disseminationRegressions + disseminationRegressed.toInt,
      ),
    )
  }
}

object DisseminationProgress {

  def reviewReadyForOrdering(
      batchMetadata: DisseminatedBatchMetadata,
      thisNodeId: BftNodeId,
      orderingTopology: OrderingTopology,
  ): Option[DisseminationProgress] = {
    val reviewedAcks = reviewAcks(batchMetadata.proofOfAvailability.acks, orderingTopology)
    // No need to update the acks in DisseminatedBatchMetadata, if the PoA is still valid
    Option.when(
      !AvailabilityModule.hasQuorum(orderingTopology, reviewedAcks.size)
    )(
      DisseminationProgress(
        orderingTopology,
        batchMetadata.regress(toSigning = voteOf(thisNodeId, reviewedAcks).isEmpty),
        reviewedAcks,
      )
    )
  }

  private def voteOf(nodeId: BftNodeId, acks: Set[AvailabilityAck]): Option[AvailabilityAck] =
    acks.find(_.from == nodeId)

  private def reviewAcks(
      acks: Iterable[AvailabilityAck],
      currentOrderingTopology: OrderingTopology,
  ): Set[AvailabilityAck] =
    acks.filter(_.validateIn(currentOrderingTopology).isRight).toSet
}

@SuppressWarnings(Array("org.wartremover.warts.Var"))
final class DisseminationProtocolState(
    val beingFirstSaved: mutable.Map[BatchId, InitialSaveInProgress] = mutable.Map.empty,
    var disseminationProgress: mutable.SortedMap[BatchId, DisseminationProgress] =
      mutable.SortedMap.empty,
    var batchesReadyForOrdering: mutable.LinkedHashMap[BatchId, DisseminatedBatchMetadata] =
      mutable.LinkedHashMap(),
    val toBeProvidedToConsensus: mutable.Queue[ToBeProvidedToConsensus] = mutable.Queue(),
    var lastProposalTime: Option[CantonTimestamp] = None,
    val disseminationQuotas: BatchDisseminationNodeQuotaTracker =
      new BatchDisseminationNodeQuotaTracker,
)

final case class ToBeProvidedToConsensus(maxBatchesPerProposal: Short, forEpochNumber: EpochNumber)
