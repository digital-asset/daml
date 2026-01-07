// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.traffic.store.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.{BatchingConfig, ProcessingTimeout}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.sequencing.traffic.TrafficConsumed
import com.digitalasset.canton.synchronizer.sequencer.store.{
  RegisteredMember,
  SequencerMemberId,
  SequencerStore,
}
import com.digitalasset.canton.synchronizer.sequencing.traffic.store.TrafficConsumedStore
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.collection.MapsUtil
import com.digitalasset.canton.util.{ErrorUtil, MonadUtil}

import scala.concurrent.ExecutionContext

/** Stores traffic consumed entries in a database. The store is organized as a journal, every
  * traffic consumption is added as a new row.
  */
class DbTrafficConsumedStore(
    override protected val storage: DbStorage,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
    batchingConfig: BatchingConfig,
    sequencerStore: SequencerStore,
)(implicit executionContext: ExecutionContext)
    extends TrafficConsumedStore
    with DbStore {

  import Member.DbStorageImplicits.*
  import storage.api.*

  private def withMemberId[A](member: Member)(
      fn: RegisteredMember => FutureUnlessShutdown[A],
      ifNotExist: => A,
  )(implicit tc: TraceContext): FutureUnlessShutdown[A] =
    sequencerStore.lookupMember(member).flatMap {
      case None => FutureUnlessShutdown.pure(ifNotExist)
      case Some(registeredMember) => fn(registeredMember)
    }

  override def store(
      trafficUpdates: Seq[TrafficConsumed]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val uniqueMembers = trafficUpdates.map(_.member).distinct

    for {
      memberMap <- sequencerStore.lookupMembers(uniqueMembers)
      _ = if (!uniqueMembers.forall(memberMap.contains))
        ErrorUtil.invalidState(
          s"All members must be registered, not registered: ${uniqueMembers.diff(memberMap.keys.toSeq)}."
        )

      _ <- doStore(trafficUpdates, MapsUtil.skipEmpty(memberMap))
    } yield ()
  }

  private def doStore(
      trafficUpdates: Seq[TrafficConsumed],
      memberMap: Map[Member, RegisteredMember],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val insertSql =
      """insert into seq_traffic_control_consumed_journal (member_id, sequencing_timestamp, extra_traffic_consumed, base_traffic_remainder, last_consumed_cost)
             values (?, ?, ?, ?, ?)
             on conflict do nothing"""

    val bulkInsert = DbStorage
      .bulkOperation(insertSql, trafficUpdates, storage.profile) { pp => trafficConsumed =>
        val memberId = memberMap(trafficConsumed.member).memberId

        val TrafficConsumed(
          _,
          sequencingTimestamp,
          extraTrafficConsumed,
          baseTrafficRemainder,
          lastConsumedCost,
        ) = trafficConsumed
        pp >> memberId
        pp >> sequencingTimestamp
        pp >> extraTrafficConsumed
        pp >> baseTrafficRemainder
        pp >> lastConsumedCost
      }
      .map(_.sum)
    storage
      .queryAndUpdate(bulkInsert, functionFullName)
      .map(updateCount => logger.debug(s"Stored $updateCount traffic consumed entries"))
      .tapOnShutdown {
        logger.debug(
          "DbTrafficConsumedStore is shutting down, cancelling storing traffic consumed entries"
        )
      }
  }

  override def lookup(
      member: Member
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Seq[TrafficConsumed]] =
    withMemberId(member)(
      { registeredMember =>
        val query =
          sql"""select $member, sequencing_timestamp, extra_traffic_consumed, base_traffic_remainder, last_consumed_cost
              from seq_traffic_control_consumed_journal
              where member_id = ${registeredMember.memberId}
              order by sequencing_timestamp asc"""
        storage.query(query.as[TrafficConsumed], functionFullName)
      },
      Seq.empty,
    )

  override def lookupLast(
      member: Member
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Option[TrafficConsumed]] =
    withMemberId(member)(
      { registeredMember =>
        val query =
          sql"""select $member, sequencing_timestamp, extra_traffic_consumed, base_traffic_remainder, last_consumed_cost
             from seq_traffic_control_consumed_journal
             where member_id = ${registeredMember.memberId}
             order by sequencing_timestamp desc
             limit 1"""
        storage.querySingle(query.as[TrafficConsumed].headOption, functionFullName).value
      },
      None,
    )

  override def lookupLatestBeforeInclusive(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[TrafficConsumed]] = {
    val query = storage.profile match {
      case _: DbStorage.Profile.Postgres =>
        sql"""select m.member, tc.sequencing_timestamp, tc.extra_traffic_consumed, tc.base_traffic_remainder, tc.last_consumed_cost
            from sequencer_members m
            inner join lateral (
              select sequencing_timestamp, extra_traffic_consumed, base_traffic_remainder, last_consumed_cost
              from seq_traffic_control_consumed_journal journal
              where member_id = m.id and sequencing_timestamp <= $timestamp
              order by sequencing_timestamp desc
              limit 1) tc
            on true"""
      case _ =>
        // H2 does't support lateral joins
        sql"""select member, sequencing_timestamp, extra_traffic_consumed, base_traffic_remainder, last_consumed_cost
                    from
                      (select member, sequencing_timestamp, extra_traffic_consumed, base_traffic_remainder, last_consumed_cost,
                              rank() over (partition by member order by sequencing_timestamp desc) as pos
                       from seq_traffic_control_consumed_journal journal
                       inner join sequencer_members members on journal.member_id = members.id
                       where sequencing_timestamp <= $timestamp
                      ) as with_pos
                    where pos = 1
                   """
    }
    storage.query(query.as[TrafficConsumed], functionFullName)
  }

  def lookupLatestBeforeInclusiveForMember(member: Member, timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[TrafficConsumed]] =
    withMemberId(member)(
      { registeredMember =>
        val query =
          sql"""select $member, sequencing_timestamp, extra_traffic_consumed, base_traffic_remainder, last_consumed_cost
            from seq_traffic_control_consumed_journal
            where sequencing_timestamp <= $timestamp and member_id = ${registeredMember.memberId}
            order by member_id, sequencing_timestamp desc
            limit 1""".as[TrafficConsumed].headOption

        storage.querySingle(query, functionFullName).value
      },
      None,
    )

  override def lookupAt(member: Member, timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[TrafficConsumed]] =
    withMemberId(member)(
      { registeredMember =>
        val query =
          sql"""select $member, sequencing_timestamp, extra_traffic_consumed, base_traffic_remainder, last_consumed_cost
           from seq_traffic_control_consumed_journal
           where member_id = ${registeredMember.memberId} and sequencing_timestamp = $timestamp
           limit 1"""
        storage.querySingle(query.as[TrafficConsumed].headOption, functionFullName).value
      },
      None,
    )

  override def pruneBelowExclusive(
      upToExclusive: CantonTimestamp
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[String] = {
    // We need to delete all rows with sequencing_timestamp below the closest row to upToExclusive, by member.
    // That is because the closest row contains the value which are valid at upToExclusive. So even if it's below
    // upToExclusive, we need to keep it.
    // To do that we first find the latest timestamp for all members before the pruning timestamp.
    // Then we delete all rows below that timestamp for each member,
    // using parallelized batched non-transactional DbStorage.bulkOperation.
    val lookupQuery = storage.profile match {
      case _: DbStorage.Profile.Postgres =>
        sql"""select tc.member_id, tc.sequencing_timestamp
              from sequencer_members m
              inner join lateral (
                select member_id, sequencing_timestamp, extra_traffic_consumed, base_traffic_remainder, last_consumed_cost
                from seq_traffic_control_consumed_journal
                where member_id = m.id and sequencing_timestamp <= $upToExclusive
                order by member_id, sequencing_timestamp desc
                limit 1) tc
              on true"""
      case _ =>
        sql"""select member_id, max(sequencing_timestamp) as sequencing_timestamp
              from seq_traffic_control_consumed_journal
              where sequencing_timestamp <= $upToExclusive
              group by member_id"""
    }

    val deleteQuery =
      """delete from seq_traffic_control_consumed_journal
        |where member_id = ? and sequencing_timestamp < ?
        |""".stripMargin

    val deletedTotalCountFUS = for {
      membersTimestamps <- storage.query(
        lookupQuery.as[(SequencerMemberId, CantonTimestamp)],
        functionFullName,
      )
      deletedTotalCount <-
        if (membersTimestamps.isEmpty) {
          FutureUnlessShutdown.pure(0)
        } else {
          MonadUtil
            .batchedSequentialTraverse(
              batchingConfig.pruningParallelism,
              batchingConfig.maxItemsInBatch,
            )(
              membersTimestamps
            ) { membersTimestampsChunk =>
              val bulkDelete = DbStorage
                .bulkOperation(
                  deleteQuery,
                  membersTimestampsChunk,
                  storage.profile,
                  transactional = false,
                ) { pp => memberTimestamp =>
                  val (memberId, timestamp) = memberTimestamp
                  pp >> memberId
                  pp >> timestamp
                }
                .map(_.toSeq)
              storage.queryAndUpdate(bulkDelete, functionFullName)
            }
            .map(_.sum)
        }
    } yield deletedTotalCount

    deletedTotalCountFUS
      .tapOnShutdown {
        logger.debug(
          "DbTrafficConsumedStore is shutting down, cancelling pruning traffic consumed entries"
        )
      }
      .transformOnShutdown(0)
      .map { pruned =>
        s"Removed $pruned traffic consumed entries"
      }
  }

  override def deleteRecordsPastTimestamp(
      timestampExclusive: CantonTimestamp
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val deleteQuery =
      s"""delete from seq_traffic_control_consumed_journal
        |where member_id = ? and sequencing_timestamp > ?
        |""".stripMargin

    val cleanUpQuery = for {
      // First lookup all members
      memberIds <- sql"select id from sequencer_members".as[SequencerMemberId]
      // Then delete all consumed record above the timestamp for each member
      // This force the DB to use the index on (member, sequencing_timestamp)
      deletedCount <- DbStorage
        .bulkOperation(deleteQuery, memberIds, storage.profile) { pp => memberId =>
          pp >> memberId
          pp >> timestampExclusive
        }
        .map(_.sum)
    } yield deletedCount

    storage
      .queryAndUpdate(cleanUpQuery.transactionally, functionFullName)
      .transformOnShutdown(0)
      .map { deletedCount =>
        logger.debug(
          s"Deleted $deletedCount traffic consumed with timestamps > $timestampExclusive"
        )
      }
  }
}
