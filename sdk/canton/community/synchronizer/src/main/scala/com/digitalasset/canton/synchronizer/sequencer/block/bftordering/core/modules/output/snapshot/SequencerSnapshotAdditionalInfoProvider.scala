// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.snapshot

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStoreReader
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.OutputMetadataStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.TopologyActivationTime
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.NumberIdentifiers.EpochNumber
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.iss.EpochInfo
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.snapshot.{
  PeerActiveAt,
  SequencerSnapshotAdditionalInfo,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.OrderingTopology
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.{
  Output,
  SequencerNode,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{Env, ModuleRef}
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.topology.processing.EffectiveTime
import com.digitalasset.canton.tracing.TraceContext

import scala.util.{Failure, Success}

class SequencerSnapshotAdditionalInfoProvider[E <: Env[E]](
    outputMetadataStore: OutputMetadataStore[E],
    epochStoreReader: EpochStoreReader[E],
    override val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  def provide(
      snapshotTimestamp: CantonTimestamp,
      orderingTopology: OrderingTopology,
      requester: ModuleRef[SequencerNode.SnapshotMessage],
  )(implicit actorContext: E#ActorContextT[Output.Message[E]], traceContext: TraceContext): Unit = {
    // TODO(#23143) Consider returning an error if the `snapshotTimestamp` is too high, i.e., above the safe watermark.
    val peerActiveAtTimestamps =
      orderingTopology.peersActiveAt.view.filter { case (_, activeAt) =>
        activeAt.value <= TopologyActivationTime
          .fromEffectiveTime(EffectiveTime(snapshotTimestamp))
          .value
      }.toSeq
    val activeAtBlockFutures = peerActiveAtTimestamps.map { case (_, timestamp) =>
      // TODO(#23143) Get the first block with a timestamp greater or equal to `timestamp` instead.
      // The latest block up to `timestamp` is taken for easier simulation testing and simpler error handling.
      //  It can result however in transferring more data than needed (in particular, from before the onboarding) if:
      //  1) `timestamp` is around an epoch boundary
      //  2) `timestamp` hasn't been processed by the node that a snapshot is taken from (can happen only in simulation
      //      tests)
      //  Last but not least, if snapshots from different peers are compared for byte-for-byte equality,
      //  the comparison might fail it there are nodes that are not caught up.
      outputMetadataStore.getLatestBlockAtOrBefore(timestamp.value)
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
    val epochInfoFutures = epochNumbers.map(maybeEpochNumber =>
      maybeEpochNumber
        .map(epochNumber => epochStoreReader.loadEpochInfo(epochNumber))
        .getOrElse(
          actorContext.pureFuture(None: Option[EpochInfo])
        )
    )
    val epochInfoF = actorContext.sequenceFuture(epochInfoFutures)

    val epochMetadataFutures = epochNumbers.map(maybeEpochNumber =>
      maybeEpochNumber
        .map(epochNumber => outputMetadataStore.getEpoch(epochNumber))
        .getOrElse(
          actorContext.pureFuture(None: Option[OutputMetadataStore.OutputEpochMetadata])
        )
    )
    val epochMetadataF = actorContext.sequenceFuture(epochMetadataFutures)

    val firstBlockFutures = epochNumbers.map(maybeEpochNumber =>
      maybeEpochNumber
        .map(epochNumber => outputMetadataStore.getFirstBlockInEpoch(epochNumber))
        .getOrElse(
          actorContext.pureFuture(None: Option[OutputMetadataStore.OutputBlockMetadata])
        )
    )
    val firstBlocksF = actorContext.sequenceFuture(firstBlockFutures)

    val lastBlockInPreviousEpochFutures = epochNumbers.map(maybeEpochNumber =>
      maybeEpochNumber
        .map(epochNumber => outputMetadataStore.getLastBlockInEpoch(EpochNumber(epochNumber - 1L)))
        .getOrElse(
          actorContext.pureFuture(None: Option[OutputMetadataStore.OutputBlockMetadata])
        )
    )
    val lastBlocksInPreviousEpochsF = actorContext.sequenceFuture(lastBlockInPreviousEpochFutures)

    // Zip as if there's no tomorrow
    val zippedFuture =
      actorContext.zipFuture(
        epochInfoF,
        actorContext.zipFuture(
          epochMetadataF,
          actorContext.zipFuture(firstBlocksF, lastBlocksInPreviousEpochsF),
        ),
      )

    actorContext.pipeToSelf(zippedFuture) {
      case Failure(exception) =>
        val errorMessage = "Failed to retrieve additional block metadata for a snapshot"
        logger.error(errorMessage, exception)
        Some(Output.SequencerSnapshotMessage.AdditionalInfoRetrievalError(requester, errorMessage))
      case Success(
            (
              epochInfoObjects,
              (epochMetadataObjects, (firstBlocksInEpochs, lastBlocksInPreviousEpochs)),
            )
          ) =>
        val peerIdsToActiveAt =
          peerActiveAtTimestamps
            .lazyZip(epochInfoObjects)
            .lazyZip(epochMetadataObjects)
            .lazyZip(firstBlocksInEpochs)
            .lazyZip(lastBlocksInPreviousEpochs)
            .toList
            .map {
              case (
                    ((peerId, timestamp), epochInfo, epochMetadata, firstBlockMetadata),
                    // Too many zips result in more nesting
                    previousEpochLastBlockMetadata,
                  ) =>
                peerId -> PeerActiveAt(
                  timestamp,
                  epochInfo.map(_.number),
                  firstBlockMetadata.map(_.blockNumber),
                  epochInfo.map(_.topologyActivationTime),
                  epochMetadata.map(_.couldAlterOrderingTopology),
                  previousEpochLastBlockMetadata.map(_.blockBftTime),
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
