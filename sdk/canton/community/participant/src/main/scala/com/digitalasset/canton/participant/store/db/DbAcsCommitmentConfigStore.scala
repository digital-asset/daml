// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.store.AcsCounterParticipantConfigStore
import com.digitalasset.canton.pruning.{
  ConfigForNoWaitCounterParticipants,
  ConfigForSlowCounterParticipants,
  ConfigForSynchronizerThresholds,
}
import com.digitalasset.canton.resource.DbStorage.Implicits.BuilderChain
import com.digitalasset.canton.resource.DbStorage.SQLActionBuilderChain
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.concurrent.ExecutionContext

class DbAcsCommitmentConfigStore(
    override protected val storage: DbStorage,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends AcsCounterParticipantConfigStore
    with DbStore {

  private val firstFetch = new AtomicBoolean(true)
  private val slowCounterParticipantConfigs =
    new AtomicReference[
      (Seq[ConfigForSlowCounterParticipants], Seq[ConfigForSynchronizerThresholds])
    ](
      (Seq.empty, Seq.empty)
    )

  import storage.api.*

  private def refreshSlowCounterParticipantConfigsCache()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    val query =
      sql"""select participant.synchronizer_id, participant.participant_id, participant.is_distinguished, participant.is_added_to_metrics
               from acs_slow_counter_participants participant
         """

    val queryThreshold =
      sql"""
           select synchronizer_id, threshold_distinguished,threshold_default
                  from acs_slow_participant_config
         """

    val mapped = for {
      data <- query.as[(SynchronizerId, ParticipantId, Boolean, Boolean)]
    } yield data.map {
      case (
            synchronizerId,
            participantId,
            isDistinguished,
            isAddedToMetrics,
          ) =>
        ConfigForSlowCounterParticipants(
          synchronizerId,
          participantId,
          isDistinguished,
          isAddedToMetrics,
        )
    }
    val mappedThreshold = for {
      data <- queryThreshold.as[(SynchronizerId, Long, Long)]
    } yield data.map { case (synchronizerId, thresholdDistinguished, thresholdDefault) =>
      ConfigForSynchronizerThresholds(
        synchronizerId,
        NonNegativeLong.tryCreate(thresholdDistinguished),
        NonNegativeLong.tryCreate(thresholdDefault),
      )
    }

    for {
      configs <- storage.query(mapped, functionFullName)
      thresholds <- storage.query(mappedThreshold, functionFullName)
    } yield slowCounterParticipantConfigs.set((configs, thresholds))
  }

  override def fetchAllSlowCounterParticipantConfig()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[
    (Seq[ConfigForSlowCounterParticipants], Seq[ConfigForSynchronizerThresholds])
  ] =
    for {
      _ <-
        if (firstFetch.get()) {
          firstFetch.set(false)
          refreshSlowCounterParticipantConfigsCache()
        } else FutureUnlessShutdown.unit
    } yield slowCounterParticipantConfigs.get()

  override def createOrUpdateCounterParticipantConfigs(
      configs: Seq[ConfigForSlowCounterParticipants],
      thresholds: Seq[ConfigForSynchronizerThresholds],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val updateSlowParticipantConfig: String =
      storage.profile match {
        case _: DbStorage.Profile.H2 =>
          """merge into acs_slow_counter_participants (synchronizer_id, participant_id, is_distinguished, is_added_to_metrics)
                   values (?, ?, ?, ?)"""

        case _: DbStorage.Profile.Postgres =>
          """insert into acs_slow_counter_participants (synchronizer_id, participant_id, is_distinguished, is_added_to_metrics)
                 values (?, ?, ?, ?) on conflict (synchronizer_id, participant_id) do update set is_distinguished = excluded.is_distinguished, is_added_to_metrics = excluded.is_added_to_metrics"""
      }
    val updateSynchronizerConfig: String =
      storage.profile match {
        case _: DbStorage.Profile.H2 =>
          """merge into acs_slow_participant_config (synchronizer_id,threshold_distinguished,threshold_default)
                   values (?, ?, ?)"""

        case _: DbStorage.Profile.Postgres =>
          """insert into acs_slow_participant_config (synchronizer_id,threshold_distinguished,threshold_default)
                 values (?, ?, ?) on conflict (synchronizer_id) do update set threshold_distinguished = excluded.threshold_distinguished, threshold_default = excluded.threshold_default"""
      }

    for {
      _ <- storage.queryAndUpdate(
        DBIO.seq(
          clearSlowCounterParticipantsDBIO(configs.collect(_.synchronizerId)),
          DbStorage.bulkOperation_(
            updateSlowParticipantConfig,
            configs,
            storage.profile,
          ) { pp => config =>
            pp >> config.synchronizerId
            pp >> config.participantId
            pp >> config.isDistinguished
            pp >> config.isAddedToMetrics
          },
          DbStorage.bulkOperation_(
            updateSynchronizerConfig,
            thresholds,
            storage.profile,
          ) { pp => config =>
            pp >> config.synchronizerId
            pp >> config.thresholdDistinguished
            pp >> config.thresholdDefault
          },
        ),
        functionFullName,
      )
      // we fetch from the DB to make this concurrently safe and also to ensure we have the entire set,
      // since configs might only hold a subset
      _ <- refreshSlowCounterParticipantConfigsCache()
    } yield ()
  }

  private def clearSlowCounterParticipantsDBIO(
      synchronizerIds: Seq[SynchronizerId]
  )(implicit traceContext: TraceContext): DBIOAction[Unit, NoStream, Effect.All] =
    DBIO.seq(
      if (synchronizerIds.isEmpty) {
        sqlu"""DELETE FROM acs_slow_counter_participants"""
      } else {
        DbStorage.bulkOperation_(
          """DELETE FROM acs_slow_counter_participants
                WHERE synchronizer_id = ?
             """,
          synchronizerIds,
          storage.profile,
        ) { pp => synchronizer =>
          pp >> synchronizer
        }
      },
      if (synchronizerIds.isEmpty) {
        sqlu"""DELETE FROM acs_slow_participant_config"""
      } else {
        DbStorage.bulkOperation_(
          """DELETE FROM acs_slow_participant_config
                WHERE synchronizer_id = ?
             """,
          synchronizerIds,
          storage.profile,
        ) { pp => synchronizer =>
          pp >> synchronizer
        }
      },
    )

  override def clearSlowCounterParticipants(
      synchronizerIds: Seq[SynchronizerId]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    for {
      _ <- storage.queryAndUpdate(
        clearSlowCounterParticipantsDBIO(synchronizerIds),
        functionFullName,
      )
      _ <- refreshSlowCounterParticipantConfigsCache()
    } yield ()

  override def addNoWaitCounterParticipant(
      configs: Seq[ConfigForNoWaitCounterParticipants]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val updateNoWait: String =
      storage.profile match {
        case _: DbStorage.Profile.H2 =>
          """merge into acs_no_wait_counter_participants (synchronizer_id,participant_id)
                   values (?, ?)"""
        case _: DbStorage.Profile.Postgres =>
          """insert into acs_no_wait_counter_participants (synchronizer_id,participant_id)
                 values (?, ?) on conflict do nothing"""
      }

    storage.queryAndUpdate(
      DbStorage.bulkOperation_(
        updateNoWait,
        configs,
        storage.profile,
      ) { pp => config =>
        pp >> config.synchronizerId
        pp >> config.participantId
      },
      functionFullName,
    )
  }

  override def removeNoWaitCounterParticipant(
      synchronizers: Seq[SynchronizerId],
      participants: Seq[ParticipantId],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val crossProduct = for {
      synchronizer <- synchronizers
      participant <- participants
    } yield (synchronizer, participant)
    storage.queryAndUpdate(
      DbStorage.bulkOperation_(
        """DELETE FROM acs_no_wait_counter_participants
                WHERE synchronizer_id = ? AND participant_id = ?
             """,
        crossProduct,
        storage.profile,
      ) { pp => synchronizerAndParticipant =>
        pp >> synchronizerAndParticipant._1
        pp >> synchronizerAndParticipant._2
      },
      functionFullName,
    )

  }

  override def getAllActiveNoWaitCounterParticipants(
      filterSynchronizers: Seq[SynchronizerId],
      filterParticipants: Seq[ParticipantId],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[ConfigForNoWaitCounterParticipants]] = {
    import DbStorage.Implicits.BuilderChain.*
    val baseQuery = sql"""select cs.synchronizer_id, cs.participant_id
               from acs_no_wait_counter_participants cs
               where 1=1 """

    def querySynchronizer(chain: SQLActionBuilderChain, synchronizerClause: SQLActionBuilderChain) =
      storage.profile match {
        case _ =>
          chain ++ sql"""  and """ ++ synchronizerClause
      }

    def queryParticipant(
        chain: SQLActionBuilderChain,
        participantClause: SQLActionBuilderChain,
    ) = storage.profile match {
      case _ => chain ++ sql""" and """ ++ participantClause
    }

    val synchronizers = NonEmpty.from(filterSynchronizers)
    val participants = NonEmpty.from(filterParticipants)

    val queries = (synchronizers, participants) match {
      case (None, None) =>
        BuilderChain.toSQLActionBuilderChain(baseQuery).as[(SynchronizerId, ParticipantId)]
      case (Some(dom), None) =>
        querySynchronizer(
          baseQuery,
          DbStorage
            .toInClause("cs.synchronizer_id", dom),
        ).as[(SynchronizerId, ParticipantId)]
      case (None, Some(par)) =>
        queryParticipant(
          baseQuery,
          DbStorage
            .toInClause("cs.participant_id", par),
        ).as[(SynchronizerId, ParticipantId)]
      case (Some(dom), Some(par)) =>
        queryParticipant(
          querySynchronizer(
            baseQuery,
            DbStorage
              .toInClause("cs.synchronizer_id", dom),
          ),
          DbStorage.toInClause("cs.participant_id", par),
        )
          .as[(SynchronizerId, ParticipantId)]

    }

    for {
      data <- storage
        .query(queries, functionFullName)

    } yield data.map {
      case (
            synchronizerId,
            participantId,
          ) =>
        ConfigForNoWaitCounterParticipants(
          synchronizerId,
          participantId,
        )
    }
  }
}
