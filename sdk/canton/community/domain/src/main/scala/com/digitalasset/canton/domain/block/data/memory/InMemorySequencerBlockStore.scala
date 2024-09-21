// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.block.data.memory

import cats.data.EitherT
import cats.syntax.functor.*
import cats.syntax.parallel.*
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.domain.block.data.{
  BlockEphemeralState,
  BlockInfo,
  SequencerBlockStore,
}
import com.digitalasset.canton.domain.sequencing.integrations.state.InMemorySequencerStateManagerStore
import com.digitalasset.canton.domain.sequencing.integrations.state.statemanager.MemberCounters
import com.digitalasset.canton.domain.sequencing.sequencer.errors.SequencerError
import com.digitalasset.canton.domain.sequencing.sequencer.errors.SequencerError.BlockNotFound
import com.digitalasset.canton.domain.sequencing.sequencer.store.InMemorySequencerStore
import com.digitalasset.canton.domain.sequencing.sequencer.{
  InFlightAggregationUpdates,
  SequencerInitialState,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*

import java.util.concurrent.atomic.AtomicReference
import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

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
  private val memberInitialCounter = new AtomicReference[Map[Member, SequencerCounter]](Map.empty)

  override def setInitialState(
      initialSequencerState: SequencerInitialState,
      maybeOnboardingTopologyEffectiveTimestamp: Option[CantonTimestamp],
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val initial = BlockEphemeralState.fromSequencerInitialState(initialSequencerState)
    updateBlockHeight(initial.latestBlock)
    for {
      _ <- initialSequencerState.snapshot.heads.toSeq.parTraverse_ { case (member, counter) =>
        updateMemberCounterSupportedAfter(member, counter)
      }
      _ <- sequencerStore.addInFlightAggregationUpdates(
        initial.inFlightAggregations.fmap(_.asUpdate)
      )
    } yield {
      ()
    }
  }

  override def initialMemberCounters(implicit traceContext: TraceContext): Future[MemberCounters] =
    Future.successful(memberInitialCounter.get)

  override def partialBlockUpdate(
      inFlightAggregationUpdates: InFlightAggregationUpdates
  )(implicit traceContext: TraceContext): Future[Unit] =
    sequencerStore.addInFlightAggregationUpdates(inFlightAggregationUpdates)

  override def finalizeBlockUpdate(block: BlockInfo)(implicit
      traceContext: TraceContext
  ): Future[Unit] = {
    updateBlockHeight(block)
    Future.unit
  }

  private def updateBlockHeight(block: BlockInfo): Unit =
    blockToTimestampMap
      .put(block.height, block.lastTs -> block.latestSequencerEventTimestamp)
      .discard

  override def readHead(implicit traceContext: TraceContext): Future[BlockEphemeralState] =
    for {
      watermarkO <- inMemorySequencerStore.safeWatermark
      blockInfoO = watermarkO match {
        case Some(watermark) =>
          findBlockContainingTimestamp(watermark).orElse(readLatestBlockInfo())
        case None =>
          None
      }
      state <- blockInfoO match {
        case None => Future.successful(BlockEphemeralState.empty)
        case Some(blockInfo) =>
          sequencerStore
            .readInFlightAggregations(blockInfo.lastTs)
            .map(inFlightAggregations => BlockEphemeralState(blockInfo, inFlightAggregations))
      }

    } yield state

  private def findBlockContainingTimestamp(watermark: CantonTimestamp) =
    blockToTimestampMap
      .readOnlySnapshot()
      .toSeq
      .collect {
        case (height, (latestEventTs, latestSequencerEventTsO)) if latestEventTs >= watermark =>
          BlockInfo(height, latestEventTs, latestSequencerEventTsO)
      }
      .minByOption(_.height)

  private def readLatestBlockInfo() =
    blockToTimestampMap.readOnlySnapshot().maxByOption { case (height, _) => height }.map {
      case (height, (latestEventTs, latestSequencerEventTsO)) =>
        BlockInfo(height, latestEventTs, latestSequencerEventTsO)
    }

  override def readStateForBlockContainingTimestamp(
      timestamp: CantonTimestamp
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, SequencerError, BlockEphemeralState] =
    blockToTimestampMap.toList
      .sortBy(_._2._1)
      .find(_._2._1 >= timestamp)
      .fold[EitherT[Future, SequencerError, BlockEphemeralState]](
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
  ): Future[String] = {
    sequencerStore.pruneExpiredInFlightAggregationsInternal(requestedTimestamp).discard
    val blocksToBeRemoved = blockToTimestampMap.collect {
      case (height, (latestEventTs, _)) if latestEventTs < requestedTimestamp =>
        height
    }
    blockToTimestampMap.subtractAll(blocksToBeRemoved)
    Future.successful(
      s"Removed ${blocksToBeRemoved.size} blocks"
    )
  }

  override def updateMemberCounterSupportedAfter(
      member: Member,
      counterLastUnsupported: SequencerCounter,
  )(implicit
      traceContext: TraceContext
  ): Future[Unit] =
    Future.successful(memberInitialCounter.getAndUpdate { previousState =>
      // Don't update member counter if specified counter is less than or equal that previous counter.
      val prevCounter = previousState.get(member)
      if (prevCounter.exists(_ >= counterLastUnsupported)) previousState
      else
        previousState.updated(member, counterLastUnsupported)
    }.discard)

  override def close(): Unit = ()

}
