// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.output.snapshot

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.output.data.OutputBlockMetadataStore
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.EpochNumber
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.snapshot.{
  FirstKnownAt,
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
    // TODO(#19661): Consider returning an error if the `snapshotTimestamp` is too high, i.e., above the safe watermark.
    val peerFirstKnownAtTimestamps =
      orderingTopology.peersFirstKnownAt.view.filter(_._2.value <= snapshotTimestamp).toSeq
    val firstKnownAtBlockFutures = peerFirstKnownAtTimestamps.map { case (_, timestamp) =>
      store.getLatestAtOrBefore(timestamp.value)
    }
    val firstKnownAtBlocksF = actorContext.sequenceFuture(firstKnownAtBlockFutures)

    actorContext.pipeToSelf(firstKnownAtBlocksF) {
      case Failure(exception) =>
        val errorMessage = s"Failed to retrieve block metadata for a snapshot at $snapshotTimestamp"
        logger.error(errorMessage, exception)
        Some(Output.SequencerSnapshotMessage.AdditionalInfoRetrievalError(requester, errorMessage))
      case Success(blocks) =>
        logger.info(s"Retrieved blocks $blocks for sequencer snapshot at $snapshotTimestamp")
        val epochNumbers = blocks.map(_.map(_.epochNumber))
        provideWithEpochBasedInfo(epochNumbers, peerFirstKnownAtTimestamps, requester)
        // We chain several `pipeToSelf` for simplicity, rather than continue via messages to the Output module;
        //  this is OK, even though the execution context is not the actor's sequential one anymore,
        //  as we ensure not to change the actor state.
        None
    }
  }

  private def provideWithEpochBasedInfo(
      epochNumbers: Seq[Option[EpochNumber]],
      peerFirstKnownAtTimestamps: Seq[(SequencerId, EffectiveTime)],
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
        val previousBftTimes = lastBlocksInPreviousEpochs.map(_.map(_.blockBftTime))
        val peersFirstKnownAt = peerFirstKnownAtTimestamps
          .lazyZip(firstBlocksInEpochs)
          .lazyZip(previousBftTimes)
          .toList
          .map { case ((peerId, timestamp), blockMetadata, previousBftTime) =>
            peerId -> FirstKnownAt(
              Some(timestamp),
              blockMetadata.map(_.epochNumber),
              firstBlockNumberInEpoch = blockMetadata.map(_.blockNumber),
              previousBftTime,
            )
          }
          .toMap
        logger.info(s"Providing peers for sequencer snapshot: $peersFirstKnownAt")
        Some(
          Output.SequencerSnapshotMessage
            .AdditionalInfo(
              requester,
              SequencerSnapshotAdditionalInfo(peersFirstKnownAt),
            )
        )
    }
  }
}
