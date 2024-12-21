// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.output.snapshot

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.output.data.OutputBlockMetadataStore
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.topology.TopologyActivationTime
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.EpochNumber
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.snapshot.{
  PeerActiveAt,
  SequencerSnapshotAdditionalInfo,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.topology.OrderingTopology
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.{
  Output,
  SequencerNode,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.{
  Env,
  ModuleRef,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.topology.processing.EffectiveTime
import com.digitalasset.canton.tracing.TraceContext

import scala.util.{Failure, Success}

class SequencerSnapshotAdditionalInfoProvider[E <: Env[E]](
    store: OutputBlockMetadataStore[E],
    override val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  def provide(
      snapshotTimestamp: CantonTimestamp,
      orderingTopology: OrderingTopology,
      requester: ModuleRef[SequencerNode.SnapshotMessage],
  )(implicit actorContext: E#ActorContextT[Output.Message[E]], traceContext: TraceContext): Unit = {
    // TODO(#23143): Consider returning an error if the `snapshotTimestamp` is too high, i.e., above the safe watermark.
    val peerActiveAtTimestamps =
      orderingTopology.peersActiveAt.view.filter { case (_, activeAt) =>
        // Take into account all peers that become active up to the latest activation time, which corresponds to
        //  the snapshot time.
        activeAt.value <= TopologyActivationTime
          .fromEffectiveTime(EffectiveTime(snapshotTimestamp))
          .value
      }.toSeq
    val activeAtBlockFutures = peerActiveAtTimestamps.map { case (_, timestamp) =>
      store.getLatestAtOrBefore(timestamp.value)
    }
    val activeAtBlocksF = actorContext.sequenceFuture(activeAtBlockFutures)

    actorContext.pipeToSelf(activeAtBlocksF) {
      case Failure(exception) =>
        val errorMessage = s"Failed to retrieve block metadata for a snapshot at $snapshotTimestamp"
        logger.error(errorMessage, exception)
        Some(Output.SequencerSnapshotMessage.AdditionalInfoRetrievalError(requester, errorMessage))
      case Success(blocks) =>
        logger.info(s"Retrieved blocks $blocks for sequencer snapshot at $snapshotTimestamp")
        val epochNumbers = blocks.map(_.map(_.epochNumber))
        provideWithEpochBasedInfo(epochNumbers, peerActiveAtTimestamps, requester)
        // We chain several `pipeToSelf` for simplicity, rather than continue via messages to the Output module.
        //  Based on Pekko documentation it's ok, as `pipeToSelf` can be called from other threads than the ordinary
        //  actor message processing thread.
        None
    }
  }

  private def provideWithEpochBasedInfo(
      epochNumbers: Seq[Option[EpochNumber]],
      peerActiveAtTimestamps: Seq[(SequencerId, TopologyActivationTime)],
      requester: ModuleRef[SequencerNode.SnapshotMessage],
  )(implicit actorContext: E#ActorContextT[Output.Message[E]], traceContext: TraceContext): Unit = {
    val firstBlockFutures = epochNumbers.map(maybeEpochNumber =>
      maybeEpochNumber
        .map(epochNumber => store.getFirstInEpoch(epochNumber))
        .getOrElse(
          actorContext.pureFuture(None: Option[OutputBlockMetadataStore.OutputBlockMetadata])
        )
    )
    val firstBlocksF = actorContext.sequenceFuture(firstBlockFutures)

    val lastBlockInPreviousEpochFutures = epochNumbers.map(maybeEpochNumber =>
      maybeEpochNumber
        .map(epochNumber => store.getLastInEpoch(EpochNumber(epochNumber - 1L)))
        .getOrElse(
          actorContext.pureFuture(None: Option[OutputBlockMetadataStore.OutputBlockMetadata])
        )
    )
    val lastBlocksInPreviousEpochsF = actorContext.sequenceFuture(lastBlockInPreviousEpochFutures)

    val zippedFuture = actorContext.zipFuture(firstBlocksF, lastBlocksInPreviousEpochsF)

    actorContext.pipeToSelf(zippedFuture) {
      case Failure(exception) =>
        val errorMessage = "Failed to retrieve additional block metadata for a snapshot"
        logger.error(errorMessage, exception)
        Some(Output.SequencerSnapshotMessage.AdditionalInfoRetrievalError(requester, errorMessage))
      case Success(firstBlocksInEpochs -> lastBlocksInPreviousEpochs) =>
        val peerIdsToActiveAt = peerActiveAtTimestamps
          .lazyZip(lastBlocksInPreviousEpochs)
          .lazyZip(firstBlocksInEpochs)
          .toList
          .map { case ((peerId, timestamp), previousLastBlockMetadata, firstBlockMetadata) =>
            peerId -> PeerActiveAt(
              Some(timestamp),
              firstBlockMetadata.map(_.epochNumber),
              firstBlockMetadata.map(_.blockNumber),
              previousLastBlockMetadata.map(_.pendingTopologyChangesInNextEpoch),
              previousLastBlockMetadata.map(_.blockBftTime),
            )
          }
          .toMap
        logger.info(s"Providing peers for sequencer snapshot: $peerIdsToActiveAt")
        Some(
          Output.SequencerSnapshotMessage
            .AdditionalInfo(
              requester,
              SequencerSnapshotAdditionalInfo(peerIdsToActiveAt),
            )
        )
    }
  }
}
