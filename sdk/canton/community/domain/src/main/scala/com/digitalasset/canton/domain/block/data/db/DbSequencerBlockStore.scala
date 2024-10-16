// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.block.data.db

import cats.data.EitherT
import cats.syntax.functor.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.block.data.{
  BlockEphemeralState,
  BlockInfo,
  SequencerBlockStore,
}
import com.digitalasset.canton.domain.sequencing.integrations.state.DbSequencerStateManagerStore
import com.digitalasset.canton.domain.sequencing.sequencer.errors.SequencerError
import com.digitalasset.canton.domain.sequencing.sequencer.errors.SequencerError.BlockNotFound
import com.digitalasset.canton.domain.sequencing.sequencer.{
  InFlightAggregationUpdates,
  SequencerInitialState,
}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.IdempotentInsert.insertVerifyingConflicts
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.{ExecutionContext, Future}

class DbSequencerBlockStore(
    override protected val storage: DbStorage,
    protocolVersion: ProtocolVersion,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit override protected val executionContext: ExecutionContext)
    extends SequencerBlockStore
    with DbStore {

  import DbStorage.Implicits.BuilderChain.*
  import storage.api.*

  private val topRow = storage.limitSql(1)

  private val sequencerStore = new DbSequencerStateManagerStore(
    storage,
    protocolVersion,
    timeouts,
    loggerFactory,
  )

  override def readHead(implicit traceContext: TraceContext): Future[BlockEphemeralState] =
    storage.query(
      for {
        watermark <- safeWaterMarkDBIO
        blockInfoO <- watermark match {
          case Some(watermark) => findBlockForCrashRecoveryForWatermark(watermark)
          case None => DBIO.successful(None)
        }
        state <- blockInfoO match {
          case None => DBIO.successful(BlockEphemeralState.empty)
          case Some(blockInfo) => readAtBlock(blockInfo)
        }
      } yield state,
      functionFullName,
    )

  private def safeWaterMarkDBIO: DBIOAction[Option[CantonTimestamp], NoStream, Effect.Read] = {
    val query =
      // TODO(#18401): Below only works for a single instance database sequencer
      sql"select min(watermark_ts) from sequencer_watermarks"
    // `min` may return null that is wrapped into None
    query.as[Option[CantonTimestamp]].headOption.map(_.flatten)
  }

  /** Find a completed earlier block that must be used for crash recovery.
    * Since `readAtBlockTimestampDBIO` returns the state after the block (by BlockInfo.lastTs),
    * and that's used to initialize the sequencer on the startup,
    * we select the block that has its latest event timestamp less than or equal to the watermark.
    * In the less than case it select the previous block.
    * In the equal case it means that all of the events of the block have been written and watermarked,
    * so we can start the sequencer from the following block.
    */
  private def findBlockForCrashRecoveryForWatermark(
      beforeInclusive: CantonTimestamp
  ): DBIOAction[Option[BlockInfo], NoStream, Effect.Read] =
    (sql"""select height, latest_event_ts, latest_sequencer_event_ts from seq_block_height where latest_event_ts <= $beforeInclusive order by height desc """ ++ topRow)
      .as[BlockInfo]
      .headOption

  private def findBlockContainingTimestamp(
      timestamp: CantonTimestamp
  ): DBIOAction[Option[BlockInfo], NoStream, Effect.Read] =
    (sql"""select height, latest_event_ts, latest_sequencer_event_ts from seq_block_height where latest_event_ts >= $timestamp order by height """ ++ topRow)
      .as[BlockInfo]
      .headOption

  override def readStateForBlockContainingTimestamp(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): EitherT[Future, SequencerError, BlockEphemeralState] =
    EitherT(
      storage.query(
        for {
          heightAndTimestamp <- findBlockContainingTimestamp(timestamp)
          state <- heightAndTimestamp match {
            case None => DBIO.successful(Left(BlockNotFound.InvalidTimestamp(timestamp)))
            case Some(block) => readAtBlock(block).map(Right.apply)
          }
        } yield state,
        functionFullName,
      )
    )

  private def readAtBlock(
      block: BlockInfo
  ): DBIOAction[BlockEphemeralState, NoStream, Effect.Read with Effect.Transactional] =
    sequencerStore
      .readInFlightAggregationsDBIO(
        block.lastTs
      )
      .map(inFlightAggregations => BlockEphemeralState(block, inFlightAggregations))

  override def partialBlockUpdate(
      inFlightAggregationUpdates: InFlightAggregationUpdates
  )(implicit traceContext: TraceContext): Future[Unit] =
    storage.queryAndUpdate(
      sequencerStore.addInFlightAggregationUpdatesDBIO(inFlightAggregationUpdates),
      functionFullName,
    )

  override def finalizeBlockUpdate(block: BlockInfo)(implicit
      traceContext: TraceContext
  ): Future[Unit] =
    storage.queryAndUpdate(updateBlockHeightDBIO(block), functionFullName)

  override def setInitialState(
      initial: SequencerInitialState,
      maybeOnboardingTopologyEffectiveTimestamp: Option[CantonTimestamp],
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val updateBlockHeight = updateBlockHeightDBIO(BlockInfo.fromSequencerInitialState(initial))
    val addInFlightAggregations =
      sequencerStore.addInFlightAggregationUpdatesDBIO(
        initial.snapshot.inFlightAggregations.fmap(_.asUpdate)
      )
    storage
      .queryAndUpdate(
        DBIO.seq(
          updateBlockHeight,
          addInFlightAggregations,
        ),
        functionFullName,
      )
  }

  private def updateBlockHeightDBIO(block: BlockInfo)(implicit traceContext: TraceContext) =
    insertVerifyingConflicts(
      sql"""insert into seq_block_height (height, latest_event_ts, latest_sequencer_event_ts)
            values (${block.height}, ${block.lastTs}, ${block.latestSequencerEventTimestamp})
            on conflict do nothing""".asUpdate,
      sql"select latest_event_ts, latest_sequencer_event_ts from seq_block_height where height = ${block.height}"
        .as[(CantonTimestamp, Option[CantonTimestamp])]
        .head,
    )(
      { case (lastEventTs, latestSequencerEventTs) =>
        // Allow updates to `latestSequencerEventTs` if it was not set before.
        lastEventTs == block.lastTs &&
        (latestSequencerEventTs.isEmpty || latestSequencerEventTs == block.latestSequencerEventTimestamp)
      },
      { case (lastEventTs, latestSequencerEventTs) =>
        s"Block height row for [${block.height}] had existing timestamp [$lastEventTs] and topology client timestamp [$latestSequencerEventTs], but we are attempting to insert [${block.lastTs}] and [${block.latestSequencerEventTimestamp}]"
      },
    )

  override def prune(requestedTimestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[String] = for {
    (count, maxHeight) <- storage.query(
      sql"select count(*), max(height) from seq_block_height where latest_event_ts < $requestedTimestamp "
        .as[(Long, Long)]
        .head,
      functionFullName,
    )
    _ <- storage.queryAndUpdate(
      sqlu"delete from seq_block_height where height < $maxHeight",
      functionFullName,
    )
    _ <- sequencerStore.pruneExpiredInFlightAggregations(requestedTimestamp)
  } yield {
    // the first element (with lowest height) in the seq_block_height can either represent an actual existing block
    // in the database in the case where we've never pruned and also not started this sequencer from a snapshot.
    // In this case we want to count that as a removed block now.
    // The other case is when we've started from a snapshot or have pruned before, and the first element of this table
    // merely represents the initial state from where new timestamps can be computed subsequently.
    // In that case we don't want to count it as a removed block, since it did not actually represent a block with existing data.
    val pruningFromBeginning = (maxHeight - count) == -1
    s"Removed ${if (pruningFromBeginning) count else Math.max(0, count - 1)} blocks"
  }

  private[this] def readLatestBlockInfo(): DBIOAction[Option[BlockInfo], NoStream, Effect.Read] =
    (sql"select height, latest_event_ts, latest_sequencer_event_ts from seq_block_height order by height desc " ++ topRow)
      .as[BlockInfo]
      .headOption

}
