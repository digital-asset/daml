// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.block.data.db

import cats.data.EitherT
import cats.syntax.foldable.*
import cats.syntax.functor.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.block.data.EphemeralState.counterToCheckpoint
import com.digitalasset.canton.domain.block.data.SequencerBlockStore.InvalidTimestamp
import com.digitalasset.canton.domain.block.data.{
  BlockEphemeralState,
  BlockInfo,
  EphemeralState,
  SequencerBlockStore,
}
import com.digitalasset.canton.domain.sequencing.integrations.state.DbSequencerStateManagerStore
import com.digitalasset.canton.domain.sequencing.integrations.state.statemanager.{
  MemberCounters,
  MemberSignedEvents,
  MemberTimestamps,
}
import com.digitalasset.canton.domain.sequencing.sequencer.store.CounterCheckpoint
import com.digitalasset.canton.domain.sequencing.sequencer.{
  InFlightAggregationUpdates,
  InternalSequencerPruningStatus,
}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.DbStorage.Profile.{H2, Oracle, Postgres}
import com.digitalasset.canton.resource.IdempotentInsert.insertVerifyingConflicts
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.sequencing.OrdinarySerializedEvent
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{SequencerCounter, resource}
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.{ExecutionContext, Future}

/** @param checkedInvariant Defines whether all methods should check the block invariant when they modify the state.
  *                          Invariant checking is slow.
  *                          It should only be enabled for testing and debugging.
  *                          [[scala.Some$]] defines the member under whom the sequencer's topology client subscribes.
  */
class DbSequencerBlockStore(
    override protected val storage: DbStorage,
    protocolVersion: ProtocolVersion,
    override protected val timeouts: ProcessingTimeout,
    enableAdditionalConsistencyChecks: Boolean,
    private val checkedInvariant: Option[Member],
    override protected val loggerFactory: NamedLoggerFactory,
    unifiedSequencer: Boolean,
)(implicit override protected val executionContext: ExecutionContext)
    extends SequencerBlockStore
    with DbStore {

  import DbStorage.Implicits.BuilderChain.*
  import Member.DbStorageImplicits.*
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
        blockInfoO <- {
          if (unifiedSequencer) {
            for {
              watermark <- safeWaterMarkDBIO
              blockInfoO <- watermark match {
                case Some(watermark) => findBlockContainingTimestamp(watermark)
                case None => readLatestBlockInfo()
              }
            } yield blockInfoO
          } else {
            readLatestBlockInfo()
          }
        }
        state <- blockInfoO match {
          case None => DBIO.successful(BlockEphemeralState.empty)
          case Some(blockInfo) =>
            for {
              initialCounters <- initialMemberCountersDBIO
              headState <- sequencerStore.readAtBlockTimestampDBIO(blockInfo.lastTs)
            } yield {
              BlockEphemeralState(
                blockInfo,
                mergeWithInitialCounters(headState, initialCounters),
              )
            }
        }
      } yield state,
      functionFullName,
    )

  private def safeWaterMarkDBIO: DBIOAction[Option[CantonTimestamp], NoStream, Effect.Read] = {
    val query = storage.profile match {
      case _: H2 | _: Postgres =>
        // TODO(#18401): Below only works for a single instance database sequencer
        sql"select min(watermark_ts) from sequencer_watermarks"
      case _: Oracle =>
        sql"select min(watermark_ts) from sequencer_watermarks"
    }
    // `min` may return null that is wrapped into None
    query.as[Option[CantonTimestamp]].headOption.map(_.flatten)
  }

  private def findBlockContainingTimestamp(
      timestamp: CantonTimestamp
  ): DBIOAction[Option[BlockInfo], NoStream, Effect.Read] =
    (sql"""select height, latest_event_ts, latest_sequencer_event_ts from seq_block_height where latest_event_ts >= $timestamp order by height """ ++ topRow)
      .as[BlockInfo]
      .headOption

  override def readStateForBlockContainingTimestamp(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): EitherT[Future, InvalidTimestamp, BlockEphemeralState] =
    EitherT(
      storage.query(
        for {
          heightAndTimestamp <- findBlockContainingTimestamp(timestamp)
          state <- heightAndTimestamp match {
            case None => DBIO.successful(Left(InvalidTimestamp(timestamp)))
            case Some(block) => readAtBlock(block).map(Right.apply)
          }
        } yield state,
        functionFullName,
      )
    )

  private def readAtBlock(
      block: BlockInfo
  ): DBIOAction[BlockEphemeralState, NoStream, Effect.Read with Effect.Transactional] = {
    for {
      initialCounters <- initialMemberCountersDBIO
      stateAtTimestamp <- sequencerStore.readAtBlockTimestampDBIO(block.lastTs)
    } yield BlockEphemeralState(
      block,
      mergeWithInitialCounters(stateAtTimestamp, initialCounters),
    )
  }

  private def mergeWithInitialCounters(
      state: EphemeralState,
      initialCounters: Vector[(Member, SequencerCounter)],
  ): EphemeralState =
    state.copy(
      checkpoints = initialCounters.toMap
        .fmap(counterToCheckpoint)
        // only include counters for registered members
        .filter(c => state.registeredMembers.contains(c._1)) ++ state.checkpoints
    )

  override def partialBlockUpdate(
      newMembers: MemberTimestamps,
      events: Seq[MemberSignedEvents],
      acknowledgments: MemberTimestamps,
      membersDisabled: Seq[Member],
      inFlightAggregationUpdates: InFlightAggregationUpdates,
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val addMember = sequencerStore.addMemberDBIO(_, _)
    val addEvents = sequencerStore.addEventsDBIO(_)
    val disableMember = sequencerStore.disableMemberDBIO _

    val membersDbio = DBIO
      .seq(
        newMembers.toSeq.map(addMember.tupled) ++
          membersDisabled.map(disableMember): _*
      )
      .transactionally

    for {
      _ <- storage.queryAndUpdate(membersDbio, functionFullName)
      _ <- {
        // as an optimization, we run the 3 below in parallel by starting at the same time
        val acksF = storage.queryAndUpdate(
          sequencerStore.bulkUpdateAcknowledgementsDBIO(acknowledgments),
          functionFullName,
        )
        val inFlightF = storage.queryAndUpdate(
          sequencerStore.addInFlightAggregationUpdatesDBIO(inFlightAggregationUpdates),
          functionFullName,
        )
        val eventsF = storage.queryAndUpdate(
          if (enableAdditionalConsistencyChecks) DBIO.seq(events.map(addEvents)*).transactionally
          else sequencerStore.bulkInsertEventsDBIO(events),
          functionFullName,
        )
        for {
          _ <- acksF
          _ <- inFlightF
          _ <- eventsF
        } yield ()
      }
    } yield ()
  }

  override def finalizeBlockUpdate(block: BlockInfo)(implicit
      traceContext: TraceContext
  ): Future[Unit] = {
    storage
      .queryAndUpdate(updateBlockHeightDBIO(block), functionFullName)
      .flatMap((_: Unit) => checkBlockInvariantIfEnabled(block.height))
  }

  override def readRange(
      member: Member,
      startInclusive: SequencerCounter,
      endExclusive: SequencerCounter,
  )(implicit traceContext: TraceContext): Source[OrdinarySerializedEvent, NotUsed] =
    sequencerStore.readRange(member, startInclusive, endExclusive)

  override def setInitialState(
      initial: BlockEphemeralState,
      maybeOnboardingTopologyEffectiveTimestamp: Option[CantonTimestamp],
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val members = initial.state.status.members
    val updateBlockHeight = updateBlockHeightDBIO(initial.latestBlock)
    val updateLowerBound =
      sequencerStore.saveLowerBoundDBIO(
        initial.state.status.lowerBound,
        maybeOnboardingTopologyEffectiveTimestamp,
      )
    val writeInitialCounters = initial.state.checkpoints.toSeq.map {
      case (member, CounterCheckpoint(counter, _, _)) =>
        upsertMemberInitialState(member, counter)
    }
    val addMembers =
      members.map(member => sequencerStore.addMemberDBIO(member.member, member.registeredAt))
    val addAcknowledgements =
      members.map(m => (m.member, m.lastAcknowledged)).collect { case (member, Some(lastAck)) =>
        sequencerStore.acknowledgeDBIO(member, lastAck)
      }
    val addDisableMembers =
      members.filterNot(_.enabled).map(m => sequencerStore.disableMemberDBIO(m.member))
    val addInFlightAggregations =
      sequencerStore.addInFlightAggregationUpdatesDBIO(
        initial.state.inFlightAggregations.fmap(_.asUpdate)
      )

    storage
      .queryAndUpdate(
        DBIO.seq(
          updateBlockHeight +: updateLowerBound +: addInFlightAggregations +: (addMembers ++ addAcknowledgements ++ addDisableMembers ++ writeInitialCounters): _*
        ),
        functionFullName,
      )
      .flatMap((_: Unit) => checkBlockInvariantIfEnabled(initial.latestBlock.height))
  }

  override def getInitialTopologySnapshotTimestamp(implicit
      traceContext: TraceContext
  ): Future[Option[CantonTimestamp]] = sequencerStore.getInitialTopologySnapshotTimestamp

  private def upsertMemberInitialState(
      member: Member,
      counter: SequencerCounter,
  ): resource.DbStorage.DbAction.All[Unit] =
    (storage.profile match {
      case _: DbStorage.Profile.H2 =>
        sqlu"""merge into seq_initial_state as sis using (values ($member, $counter))
                 mis (member, counter)
                   on sis.member = mis.member
                   when matched and mis.counter > sis.counter then update set counter = mis.counter
                   when not matched then insert values(mis.member, mis.counter);"""
      case _: DbStorage.Profile.Postgres =>
        sqlu"""insert into seq_initial_state as sis (member, counter) values ($member, $counter)
                 on conflict (member) do update set counter = excluded.counter where sis.counter < excluded.counter"""
      case _: DbStorage.Profile.Oracle =>
        sqlu"""merge into seq_initial_state inits
                 using (
                  select
                    $member member,
                    $counter counter
                    from dual
                  ) parameters
                 on (inits.member = parameters.member)
                 when matched then
                  update set inits.counter = parameters.counter where inits.counter < parameters.counter
                 when not matched then
                  insert (member, counter) values (parameters.member, parameters.counter)
                  """
    }).map(_ => ())

  override def getInitialState(implicit traceContext: TraceContext): Future[BlockEphemeralState] = {
    val query = readFirstBlockInfo().flatMap {
      case None => DBIO.successful(BlockEphemeralState.empty)
      case Some(firstBlock) =>
        sequencerStore
          .readAtBlockTimestampDBIO(firstBlock.lastTs)
          .zip(initialMemberCountersDBIO)
          .map { case (state, counters) =>
            BlockEphemeralState(
              firstBlock,
              EphemeralState.fromHeads(
                counters.toMap.filter { case (member, _) =>
                  // the initial counters query will give us counters for all members, but we just want the ones
                  // that have been registered at or before the timestamp used to compute the state
                  state.registeredMembers.contains(member)
                },
                state.inFlightAggregations,
                state.status,
              ),
            )
          }
    }
    storage.query(query, functionFullName)
  }

  override def initialMemberCounters(implicit traceContext: TraceContext): Future[MemberCounters] =
    storage.query(initialMemberCountersDBIO, functionFullName).map(_.toMap)

  private def initialMemberCountersDBIO =
    sql"select member, counter from seq_initial_state".as[(Member, SequencerCounter)]

  private def updateBlockHeightDBIO(block: BlockInfo)(implicit traceContext: TraceContext) =
    insertVerifyingConflicts(
      storage,
      "seq_block_height ( height )",
      sql"""seq_block_height (height, latest_event_ts, latest_sequencer_event_ts)
            values (${block.height}, ${block.lastTs}, ${block.latestSequencerEventTimestamp})""",
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

  override def pruningStatus()(implicit
      traceContext: TraceContext
  ): Future[InternalSequencerPruningStatus] =
    sequencerStore.status()

  override def locatePruningTimestamp(skip: NonNegativeInt)(implicit
      traceContext: TraceContext
  ): Future[Option[CantonTimestamp]] = storage
    .querySingle(
      sql"""select ts from seq_state_manager_events order by ts #${storage.limit(
          1,
          skipItems = skip.value.toLong,
        )}""".as[CantonTimestamp].headOption,
      functionFullName,
    )
    .value

  override def prune(requestedTimestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[String] = for {
    (count, maxHeight) <- storage.query(
      sql"select count(*), max(height) from seq_block_height where latest_event_ts < $requestedTimestamp "
        .as[(Long, Long)]
        .head,
      functionFullName,
    )
    // TODO(#12676) Pruning at the requested timestamp may leave a partial block in the store.
    //  It would make more sense to prune all events up to the latest_event_ts of the previous block
    //  and delete in-flight aggregations that have expired by latest_event_ts.
    pruningResult <- sequencerStore.prune(requestedTimestamp)
    writeInitialCounters = pruningResult.newMinimumCountersSupported.toSeq
      .map { case (member, counter) =>
        // the initial state holds the counters immediately before the ones sequencer actually supports from
        upsertMemberInitialState(member, counter - 1)
      }
    _ <- storage.queryAndUpdate(
      DBIO.seq(
        sqlu"delete from seq_block_height where height < $maxHeight" +: writeInitialCounters: _*
      ),
      functionFullName,
    )
    _ <- sequencerStore.pruneExpiredInFlightAggregations(requestedTimestamp)
    _ <- checkBlockInvariantIfEnabled(maxHeight)
  } yield {
    // the first element (with lowest height) in the seq_block_height can either represent an actual existing block
    // in the database in the case where we've never pruned and also not started this sequencer from a snapshot.
    // In this case we want to count that as a removed block now.
    // The other case is when we've started from a snapshot or have pruned before, and the first element of this table
    // merely represents the initial state from where new timestamps can be computed subsequently.
    // In that case we don't want to count it as a removed block, since it did not actually represent a block with existing data.
    val pruningFromBeginning = pruningResult.eventsPruned > 0 && (maxHeight - count) == -1
    s"Removed ${pruningResult.eventsPruned} events and ${if (pruningFromBeginning) count
      else Math.max(0, count - 1)} blocks"
  }

  override def updateMemberCounterSupportedAfter(
      member: Member,
      counterLastUnsupported: SequencerCounter,
  )(implicit traceContext: TraceContext): Future[Unit] =
    storage.queryAndUpdate(
      upsertMemberInitialState(member, counterLastUnsupported),
      functionFullName,
    )

  private[this] def checkBlockInvariantIfEnabled(
      blockHeight: Long
  )(implicit traceContext: TraceContext): Future[Unit] =
    checkedInvariant.traverse_ { topologyClientMember =>
      checkBlockInvariant(topologyClientMember, blockHeight)
    }

  private[this] def checkBlockInvariant(
      topologyClientMember: Member,
      blockHeight: Long,
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val prevHeight = blockHeight - 1
    storage
      .query(readBlockInfo(prevHeight).zip(readBlockInfo(blockHeight)), functionFullName)
      .flatMap {
        case (prev, Some(block)) =>
          val prevLastTs = prev.fold(CantonTimestamp.MinValue)(_.lastTs)
          for {
            eventsInBlock <- sequencerStore.readEventsInTimeRange(
              prevLastTs,
              block.lastTs,
            )
            newMembers <- sequencerStore.readRegistrationsInTimeRange(prevLastTs, block.lastTs)
            inFlightAggregations <- storage.query(
              sequencerStore.readAggregationsAtBlockTimestamp(block.lastTs),
              "readAggregationsAtBlockTimestamp",
            )
          } yield blockInvariant(
            topologyClientMember,
            block,
            prev,
            eventsInBlock,
            newMembers,
            inFlightAggregations,
          )
        case _ => Future.unit
      }
  }

  private[this] def readBlockInfo(
      height: Long
  ): DBIOAction[Option[BlockInfo], NoStream, Effect.Read] =
    sql"select height, latest_event_ts, latest_sequencer_event_ts from seq_block_height where height = $height"
      .as[BlockInfo]
      .headOption

  private[this] def readLatestBlockInfo(): DBIOAction[Option[BlockInfo], NoStream, Effect.Read] =
    (sql"select height, latest_event_ts, latest_sequencer_event_ts from seq_block_height order by height desc " ++ topRow)
      .as[BlockInfo]
      .headOption

  private[this] def readFirstBlockInfo(): DBIOAction[Option[BlockInfo], NoStream, Effect.Read] =
    (sql"select height, latest_event_ts, latest_sequencer_event_ts from seq_block_height order by height asc " ++ topRow)
      .as[BlockInfo]
      .headOption

}
