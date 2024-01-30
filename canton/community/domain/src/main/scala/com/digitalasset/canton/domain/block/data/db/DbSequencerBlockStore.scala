// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.block.data.db

import cats.data.EitherT
import cats.syntax.foldable.*
import cats.syntax.functor.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.block.data.SequencerBlockStore.InvalidTimestamp
import com.digitalasset.canton.domain.block.data.{
  BlockEphemeralState,
  BlockInfo,
  SequencerBlockStore,
}
import com.digitalasset.canton.domain.sequencing.integrations.state.EphemeralState.counterToCheckpoint
import com.digitalasset.canton.domain.sequencing.integrations.state.statemanager.{
  MemberCounters,
  MemberSignedEvents,
  MemberTimestamps,
}
import com.digitalasset.canton.domain.sequencing.integrations.state.{
  DbSequencerStateManagerStore,
  EphemeralState,
}
import com.digitalasset.canton.domain.sequencing.sequencer.{
  InFlightAggregationUpdates,
  InternalSequencerPruningStatus,
}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.IdempotentInsert.insertVerifyingConflicts
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.sequencing.OrdinarySerializedEvent
import com.digitalasset.canton.sequencing.protocol.TrafficState
import com.digitalasset.canton.topology.{Member, UnauthenticatedMemberId}
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
    private val checkedInvariant: Option[Member],
    override protected val loggerFactory: NamedLoggerFactory,
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
        blockInfoO <- readLatestBlockInfo()
        state <- blockInfoO match {
          case None => DBIO.successful(BlockEphemeralState.empty)
          case Some(blockInfo) =>
            for {
              initialCounters <- initialMemberCountersDBIO
              initialTrafficState <- initialMemberTrafficStateDBIO
              headState <- sequencerStore.readAtBlockTimestampDBIO(blockInfo.lastTs)
            } yield {
              BlockEphemeralState(
                blockInfo,
                mergeWithInitialCountersAndTraffic(headState, initialCounters, initialTrafficState),
              )
            }
        }
      } yield state,
      functionFullName,
    )

  override def readStateForBlockContainingTimestamp(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): EitherT[Future, InvalidTimestamp, BlockEphemeralState] =
    EitherT(
      storage.query(
        for {
          heightAndTimestamp <-
            (sql"""select height, latest_event_ts, latest_topology_client_ts from sequencer_block_height where latest_event_ts >= $timestamp order by height """ ++ topRow)
              .as[BlockInfo]
              .headOption
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
      initialTraffic <- initialMemberTrafficStateDBIO
      stateAtTimestamp <- sequencerStore.readAtBlockTimestampDBIO(block.lastTs)
    } yield BlockEphemeralState(
      block,
      mergeWithInitialCountersAndTraffic(stateAtTimestamp, initialCounters, initialTraffic),
    )
  }

  private def mergeWithInitialCountersAndTraffic(
      state: EphemeralState,
      initialCounters: Vector[(Member, SequencerCounter)],
      initialTrafficState: Vector[(Member, Option[TrafficState])],
  ): EphemeralState =
    state.copy(
      checkpoints = initialCounters.toMap
        .fmap(counterToCheckpoint)
        // only include counters for registered members
        .filter(c => state.registeredMembers.contains(c._1)) ++ state.checkpoints,
      trafficState = initialTrafficState.flatMap { case (member, maybeState) =>
        maybeState.map(member -> _)
      }.toMap ++ state.trafficState,
    )

  override def partialBlockUpdate(
      newMembers: MemberTimestamps,
      events: Seq[MemberSignedEvents],
      acknowledgments: MemberTimestamps,
      membersDisabled: Seq[Member],
      inFlightAggregationUpdates: InFlightAggregationUpdates,
      trafficState: Map[Member, TrafficState],
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val addMember = sequencerStore.addMemberDBIO(_, _)
    val addEvents = sequencerStore.addEventsDBIO(trafficState)(_)
    val addAcks = sequencerStore.acknowledgeDBIO _
    val (unauthenticated, disabledMembers) = membersDisabled.partitionMap {
      case unauthenticated: UnauthenticatedMemberId => Left(unauthenticated)
      case other => Right(other)
    }
    val disableMember = sequencerStore.disableMemberDBIO _
    val unregisterUnauthenticatedMember = sequencerStore.unregisterUnauthenticatedMember _

    val dbio = DBIO
      .seq(
        newMembers.toSeq.map(addMember.tupled) ++
          events.map(addEvents) ++
          acknowledgments.toSeq.map(addAcks.tupled) ++
          unauthenticated.map(unregisterUnauthenticatedMember) ++
          Seq(sequencerStore.addInFlightAggregationUpdatesDBIO(inFlightAggregationUpdates)) ++
          disabledMembers.map(disableMember): _*
      )
      .transactionally

    storage.queryAndUpdate(dbio, functionFullName)
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
    val members = initial.state.status.members.toSeq
    val updateBlockHeight = updateBlockHeightDBIO(initial.latestBlock)
    val updateLowerBound =
      sequencerStore.saveLowerBoundDBIO(
        initial.state.status.lowerBound,
        maybeOnboardingTopologyEffectiveTimestamp,
      )
    val writeInitialCounters = initial.state.heads.toSeq.map { case (member, counter) =>
      initial.state.trafficState
        .get(member)
        .map { memberTrafficState =>
          upsertMemberInitialStateWithTraffic(
            member,
            counter,
            memberTrafficState,
          )
        }
        .getOrElse(upsertMemberInitialState(member, counter))
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

  private def upsertMemberInitialStateWithTraffic(
      member: Member,
      counter: SequencerCounter,
      trafficState: TrafficState,
  ): resource.DbStorage.DbAction.All[Unit] = {
    (storage.profile match {
      case _: DbStorage.Profile.H2 =>
        sqlu"""merge into sequencer_initial_state(member, counter, extra_traffic_remainder, extra_traffic_consumed, base_traffic_remainder, sequenced_timestamp) values
               ($member, $counter, ${trafficState.extraTrafficRemainder}, ${trafficState.extraTrafficConsumed}, ${trafficState.baseTrafficRemainder}, ${trafficState.timestamp})"""
      case _: DbStorage.Profile.Postgres =>
        sqlu"""insert into sequencer_initial_state(member, counter, extra_traffic_remainder, extra_traffic_consumed, base_traffic_remainder, sequenced_timestamp) values
                ($member, $counter, ${trafficState.extraTrafficRemainder}, ${trafficState.extraTrafficConsumed}, ${trafficState.baseTrafficRemainder}, ${trafficState.timestamp})
                 on conflict (member) do update set counter = $counter"""
      case _: DbStorage.Profile.Oracle =>
        sqlu"""merge into sequencer_initial_state inits
                 using (
                  select
                    $member member,
                    $counter counter,
                    ${trafficState.extraTrafficRemainder} extra_traffic_remainder,
                    ${trafficState.extraTrafficConsumed} extra_traffic_consumed,
                    ${trafficState.baseTrafficRemainder} base_traffic_remainder,
                    ${trafficState.timestamp} sequenced_timestamp
                    from dual
                  ) parameters
                 on (inits.member = parameters.member)
                 when matched then update
                  set inits.counter = parameters.counter,
                  inits.extra_traffic_remainder = parameters.extra_traffic_remainder,
                  inits.extra_traffic_consumed = parameters.extra_traffic_consumed,
                  inits.base_traffic_remainder = parameters.base_traffic_remainder,
                  inits.sequenced_timestamp = parameters.sequenced_timestamp
                 when not matched then
                  insert (member, counter, extra_traffic_remainder, extra_traffic_consumed, base_traffic_remainder, sequenced_timestamp) values (parameters.member, parameters.counter, parameters.extra_traffic_remainder, parameters.extra_traffic_consumed, parameters.base_traffic_remainder, parameters.sequenced_timestamp)
                  """
    }).map(_ => ())
  }

  private def upsertMemberInitialState(
      member: Member,
      counter: SequencerCounter,
  ): resource.DbStorage.DbAction.All[Unit] =
    (storage.profile match {
      case _: DbStorage.Profile.H2 =>
        sqlu"""merge into sequencer_initial_state as sis using (values ($member, $counter, NULL, NULL, NULL, NULL))
                 mis (member, counter, extra_traffic_remainder, extra_traffic_consumed, base_traffic_remainder, sequenced_timestamp)
                   on sis.member = mis.member
                   when matched and mis.counter > sis.counter then update set counter = mis.counter
                   when not matched then insert values(mis.member, mis.counter, mis.extra_traffic_remainder, mis.extra_traffic_consumed, mis.base_traffic_remainder, mis.sequenced_timestamp);"""
      case _: DbStorage.Profile.Postgres =>
        sqlu"""insert into sequencer_initial_state as sis (member, counter) values ($member, $counter)
                 on conflict (member) do update set counter = excluded.counter where sis.counter < excluded.counter"""
      case _: DbStorage.Profile.Oracle =>
        sqlu"""merge into sequencer_initial_state inits
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
          .zip(initialMemberTrafficStateDBIO)
          .map { case ((state, counters), traffic) =>
            BlockEphemeralState(
              firstBlock,
              EphemeralState(
                counters.toMap.filter { case (member, _) =>
                  // the initial counters query will give us counters for all members, but we just want the ones
                  // that have been registered at or before the timestamp used to compute the state
                  state.registeredMembers.contains(member)
                },
                state.inFlightAggregations,
                state.status,
                trafficState = traffic.flatMap { case (member, trafficOpt) =>
                  trafficOpt.map(member -> _)
                }.toMap,
              ),
            )
          }
    }
    storage.query(query, functionFullName)
  }

  override def initialMemberCounters(implicit traceContext: TraceContext): Future[MemberCounters] =
    storage.query(initialMemberCountersDBIO, functionFullName).map(_.toMap)

  private def initialMemberCountersDBIO =
    sql"select member, counter from sequencer_initial_state".as[(Member, SequencerCounter)]

  private def initialMemberTrafficStateDBIO =
    sql"select member, extra_traffic_remainder, extra_traffic_consumed, base_traffic_remainder, sequenced_timestamp from sequencer_initial_state"
      .as[(Member, Option[TrafficState])]

  private def updateBlockHeightDBIO(block: BlockInfo)(implicit traceContext: TraceContext) =
    insertVerifyingConflicts(
      storage,
      "sequencer_block_height ( height )",
      sql"""sequencer_block_height (height, latest_event_ts, latest_topology_client_ts)
            values (${block.height}, ${block.lastTs}, ${block.latestTopologyClientTimestamp})""",
      sql"select latest_event_ts, latest_topology_client_ts from sequencer_block_height where height = ${block.height}"
        .as[(CantonTimestamp, Option[CantonTimestamp])]
        .head,
    )(
      { case (lastEventTs, latestTopologyClientTs) =>
        // Allow updates to `latestTopologyClientTs` if it was not set before.
        lastEventTs == block.lastTs &&
        (latestTopologyClientTs.isEmpty || latestTopologyClientTs == block.latestTopologyClientTimestamp)
      },
      { case (lastEventTs, latestTopologyClientTs) =>
        s"Block height row for [${block.height}] had existing timestamp [$lastEventTs] and topology client timestamp [$latestTopologyClientTs], but we are attempting to insert [${block.lastTs}] and [${block.latestTopologyClientTimestamp}]"
      },
    )

  override def pruningStatus()(implicit
      traceContext: TraceContext
  ): Future[InternalSequencerPruningStatus] =
    sequencerStore.status()

  override def prune(requestedTimestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[String] = for {
    (count, maxHeight) <- storage.query(
      sql"select count(*), max(height) from sequencer_block_height where latest_event_ts < $requestedTimestamp "
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
        sqlu"delete from sequencer_block_height where height < $maxHeight" +: writeInitialCounters: _*
      ),
      functionFullName,
    )
    _ <- sequencerStore.pruneExpiredInFlightAggregations(requestedTimestamp)
    _ <- checkBlockInvariantIfEnabled(maxHeight)
  } yield {
    // the first element (with lowest height) in the sequencer_block_height can either represent an actual existing block
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
    sql"select height, latest_event_ts, latest_topology_client_ts from sequencer_block_height where height = $height"
      .as[BlockInfo]
      .headOption

  private[this] def readLatestBlockInfo(): DBIOAction[Option[BlockInfo], NoStream, Effect.Read] =
    (sql"select height, latest_event_ts, latest_topology_client_ts from sequencer_block_height order by height desc " ++ topRow)
      .as[BlockInfo]
      .headOption

  private[this] def readFirstBlockInfo(): DBIOAction[Option[BlockInfo], NoStream, Effect.Read] =
    (sql"select height, latest_event_ts, latest_topology_client_ts from sequencer_block_height order by height asc " ++ topRow)
      .as[BlockInfo]
      .headOption

}
