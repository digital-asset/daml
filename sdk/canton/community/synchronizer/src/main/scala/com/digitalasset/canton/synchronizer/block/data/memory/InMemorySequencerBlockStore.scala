// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.block.data.memory

import cats.data.EitherT
import cats.syntax.functor.*
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.block.data.{
  BlockEphemeralState,
  BlockInfo,
  SequencerBlockStore,
}
import com.digitalasset.canton.synchronizer.sequencer.errors.SequencerError
import com.digitalasset.canton.synchronizer.sequencer.store.InMemorySequencerStore
import com.digitalasset.canton.synchronizer.sequencer.{
  InFlightAggregationUpdates,
  SequencerInitialState,
}
import com.digitalasset.canton.synchronizer.sequencing.integrations.state.InMemorySequencerStateManagerStore
import com.digitalasset.canton.tracing.TraceContext

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext

import SequencerError.BlockNotFound

class InMemorySequencerBlockStore(
    inMemorySequencerStore: InMemorySequencerStore,
    protected val loggerFactory: NamedLoggerFactory,
) extends SequencerBlockStore
    with NamedLogging {

  private val sequencerStore = new InMemorySequencerStateManagerStore(loggerFactory)
  implicit override protected val executionContext: ExecutionContext =
    DirectExecutionContext(noTracingLogger)

  /** Stores for each block height the timestamp of the last event and the last topology client timestamp
    * up to and including this block
    */
  private val blockToTimestampMap =
    new TrieMap[Long, (CantonTimestamp, Option[CantonTimestamp])]

  override def setInitialState(
      initialSequencerState: SequencerInitialState,
      maybeOnboardingTopologyEffectiveTimestamp: Option[CantonTimestamp],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val initial = BlockEphemeralState.fromSequencerInitialState(initialSequencerState)
    updateBlockHeight(initial.latestBlock)
    for {
      _ <- sequencerStore.addInFlightAggregationUpdates(
        initial.inFlightAggregations.fmap(_.asUpdate)
      )
    } yield {
      ()
    }
  }

  override def partialBlockUpdate(
      inFlightAggregationUpdates: InFlightAggregationUpdates
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    sequencerStore.addInFlightAggregationUpdates(inFlightAggregationUpdates)

  override def finalizeBlockUpdate(block: BlockInfo)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    updateBlockHeight(block)
    FutureUnlessShutdown.unit
  }

  private def updateBlockHeight(block: BlockInfo): Unit =
    blockToTimestampMap
      .put(block.height, block.lastTs -> block.latestSequencerEventTimestamp)
      .discard

  override def readHead(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[BlockEphemeralState] =
    for {
      watermarkO <- inMemorySequencerStore.safeWatermark
      blockInfoO = watermarkO match {
        case Some(watermark) =>
          findBlockForCrashRecoveryForWatermark(watermark).orElse(None)
        case None =>
          None
      }
      state <- blockInfoO match {
        case None => FutureUnlessShutdown.pure(BlockEphemeralState.empty)
        case Some(blockInfo) =>
          sequencerStore
            .readInFlightAggregations(blockInfo.lastTs)
            .map(inFlightAggregations => BlockEphemeralState(blockInfo, inFlightAggregations))
      }

    } yield state

  private def findBlockForCrashRecoveryForWatermark(
      beforeInclusive: CantonTimestamp
  ): Option[BlockInfo] = blockToTimestampMap
    .readOnlySnapshot()
    .toSeq
    .collect {
      case (height, (latestEventTs, latestSequencerEventTsO)) if latestEventTs <= beforeInclusive =>
        BlockInfo(height, latestEventTs, latestSequencerEventTsO)
    }
    .maxByOption(_.height)

  override def readStateForBlockContainingTimestamp(
      timestamp: CantonTimestamp
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerError, BlockEphemeralState] =
    blockToTimestampMap.toList
      .sortBy(_._2._1)
      .find(_._2._1 >= timestamp)
      .fold[EitherT[FutureUnlessShutdown, SequencerError, BlockEphemeralState]](
        EitherT.leftT(BlockNotFound.InvalidTimestamp(timestamp))
      ) { case (blockHeight, (blockTimestamp, latestSequencerEventTs)) =>
        val block = BlockInfo(blockHeight, blockTimestamp, latestSequencerEventTs)
        EitherT
          .right(
            sequencerStore
              .readInFlightAggregations(blockTimestamp)
          )
          .map(inFlightAggregations => BlockEphemeralState(block, inFlightAggregations))
      }

  override def prune(requestedTimestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[String] = {
    sequencerStore.pruneExpiredInFlightAggregationsInternal(requestedTimestamp).discard
    val blocksToBeRemoved = blockToTimestampMap.collect {
      case (height, (latestEventTs, _)) if latestEventTs < requestedTimestamp =>
        height
    }
    blockToTimestampMap.subtractAll(blocksToBeRemoved)
    FutureUnlessShutdown.pure(
      s"Removed ${blocksToBeRemoved.size} blocks"
    )
  }

  override def close(): Unit = ()

}
