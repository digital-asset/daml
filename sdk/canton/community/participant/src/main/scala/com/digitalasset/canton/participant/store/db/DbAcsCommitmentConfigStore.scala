// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
  ConfigForDomainThresholds,
  ConfigForNoWaitCounterParticipants,
  ConfigForSlowCounterParticipants,
}
import com.digitalasset.canton.resource.DbStorage.Implicits.BuilderChain
import com.digitalasset.canton.resource.DbStorage.SQLActionBuilderChain
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
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
    new AtomicReference[(Seq[ConfigForSlowCounterParticipants], Seq[ConfigForDomainThresholds])](
      (Seq.empty, Seq.empty)
    )

  import storage.api.*

  private def refreshSlowCounterParticipantConfigsCache()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    val query =
      sql"""select participant.domain_id, participant.participant_id, participant.is_distinguished, participant.is_added_to_metrics
               from acs_slow_counter_participants participant
         """

    val queryThreshold =
      sql"""
           select domain_id, threshold_distinguished,threshold_default
                  from acs_slow_participant_config
         """

    val mapped = for {
      data <- query.as[(DomainId, ParticipantId, Boolean, Boolean)]
    } yield data.map {
      case (
            domainId,
            participantId,
            isDistinguished,
            isAddedToMetrics,
          ) =>
        ConfigForSlowCounterParticipants(
          domainId,
          participantId,
          isDistinguished,
          isAddedToMetrics,
        )
    }
    val mappedThreshold = for {
      data <- queryThreshold.as[(DomainId, Long, Long)]
    } yield data.map { case (domainId, thresholdDistinguished, thresholdDefault) =>
      ConfigForDomainThresholds(
        domainId,
        NonNegativeLong.tryCreate(thresholdDistinguished),
        NonNegativeLong.tryCreate(thresholdDefault),
      )
    }

    for {
      configs <- storage.queryUnlessShutdown(mapped, functionFullName)
      thresholds <- storage.queryUnlessShutdown(mappedThreshold, functionFullName)
    } yield slowCounterParticipantConfigs.set((configs, thresholds))
  }

  override def fetchAllSlowCounterParticipantConfig()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[
    (Seq[ConfigForSlowCounterParticipants], Seq[ConfigForDomainThresholds])
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
      thresholds: Seq[ConfigForDomainThresholds],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val updateSlowParticipantConfig: String =
      storage.profile match {
        case _: DbStorage.Profile.H2 =>
          """merge into acs_slow_counter_participants (domain_id, participant_id, is_distinguished, is_added_to_metrics)
                   values (?, ?, ?, ?)"""

        case _: DbStorage.Profile.Postgres =>
          """insert into acs_slow_counter_participants (domain_id, participant_id, is_distinguished, is_added_to_metrics)
                 values (?, ?, ?, ?) on conflict (domain_id, participant_id) do update set is_distinguished = excluded.is_distinguished, is_added_to_metrics = excluded.is_added_to_metrics"""
      }
    val updateDomainConfig: String =
      storage.profile match {
        case _: DbStorage.Profile.H2 =>
          """merge into acs_slow_participant_config (domain_id,threshold_distinguished,threshold_default)
                   values (?, ?, ?)"""

        case _: DbStorage.Profile.Postgres =>
          """insert into acs_slow_participant_config (domain_id,threshold_distinguished,threshold_default)
                 values (?, ?, ?) on conflict (domain_id) do update set threshold_distinguished = excluded.threshold_distinguished, threshold_default = excluded.threshold_default"""
      }

    for {
      _ <- storage.queryAndUpdateUnlessShutdown(
        DBIO.seq(
          clearSlowCounterParticipantsDBIO(configs.collect(_.domainId)),
          DbStorage.bulkOperation_(
            updateSlowParticipantConfig,
            configs,
            storage.profile,
          ) { pp => config =>
            pp >> config.domainId
            pp >> config.participantId
            pp >> config.isDistinguished
            pp >> config.isAddedToMetrics
          },
          DbStorage.bulkOperation_(
            updateDomainConfig,
            thresholds,
            storage.profile,
          ) { pp => config =>
            pp >> config.domainId
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
      domainIds: Seq[DomainId]
  )(implicit traceContext: TraceContext): DBIOAction[Unit, NoStream, Effect.All] =
    DBIO.seq(
      if (domainIds.isEmpty) {
        sqlu"""DELETE FROM acs_slow_counter_participants"""
      } else {
        DbStorage.bulkOperation_(
          """DELETE FROM acs_slow_counter_participants
                WHERE domain_id = ?
             """,
          domainIds,
          storage.profile,
        ) { pp => domain =>
          pp >> domain
        }
      },
      if (domainIds.isEmpty) {
        sqlu"""DELETE FROM acs_slow_participant_config"""
      } else {
        DbStorage.bulkOperation_(
          """DELETE FROM acs_slow_participant_config
                WHERE domain_id = ?
             """,
          domainIds,
          storage.profile,
        ) { pp => domain =>
          pp >> domain
        }
      },
    )

  override def clearSlowCounterParticipants(
      domainIds: Seq[DomainId]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    for {
      _ <- storage.queryAndUpdateUnlessShutdown(
        clearSlowCounterParticipantsDBIO(domainIds),
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
          """merge into acs_no_wait_counter_participants (domain_id,participant_id)
                   values (?, ?)"""
        case _: DbStorage.Profile.Postgres =>
          """insert into acs_no_wait_counter_participants (domain_id,participant_id)
                 values (?, ?) on conflict do nothing"""
      }

    storage.queryAndUpdateUnlessShutdown(
      DbStorage.bulkOperation_(
        updateNoWait,
        configs,
        storage.profile,
      ) { pp => config =>
        pp >> config.domainId
        pp >> config.participantId
      },
      functionFullName,
    )
  }

  override def removeNoWaitCounterParticipant(
      domains: Seq[DomainId],
      participants: Seq[ParticipantId],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val crossProduct = for {
      domain <- domains
      participant <- participants
    } yield (domain, participant)
    storage.queryAndUpdateUnlessShutdown(
      DbStorage.bulkOperation_(
        """DELETE FROM acs_no_wait_counter_participants
                WHERE domain_id = ? AND participant_id = ?
             """,
        crossProduct,
        storage.profile,
      ) { pp => domainParticipant =>
        pp >> domainParticipant._1
        pp >> domainParticipant._2
      },
      functionFullName,
    )

  }

  override def getAllActiveNoWaitCounterParticipants(
      filterDomains: Seq[DomainId],
      filterParticipants: Seq[ParticipantId],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[ConfigForNoWaitCounterParticipants]] = {
    import DbStorage.Implicits.BuilderChain.*
    val baseQuery = sql"""select cs.domain_id, cs.participant_id
               from acs_no_wait_counter_participants cs
               where 1=1 """

    def queryDomain(chain: SQLActionBuilderChain, domainClause: SQLActionBuilderChain) =
      storage.profile match {
        case _ =>
          chain ++ sql"""  and """ ++ domainClause
      }

    def queryParticipant(
        chain: SQLActionBuilderChain,
        participantClause: SQLActionBuilderChain,
    ) = storage.profile match {
      case _ => chain ++ sql""" and """ ++ participantClause
    }

    val domains = NonEmpty.from(filterDomains)
    val participants = NonEmpty.from(filterParticipants)

    val queries = (domains, participants) match {
      case (None, None) =>
        BuilderChain.toSQLActionBuilderChain(baseQuery).as[(DomainId, ParticipantId)]
      case (Some(dom), None) =>
        queryDomain(
          baseQuery,
          DbStorage
            .toInClause("cs.domain_id", dom),
        ).as[(DomainId, ParticipantId)]
      case (None, Some(par)) =>
        queryParticipant(
          baseQuery,
          DbStorage
            .toInClause("cs.participant_id", par),
        ).as[(DomainId, ParticipantId)]
      case (Some(dom), Some(par)) =>
        queryParticipant(
          queryDomain(
            baseQuery,
            DbStorage
              .toInClause("cs.domain_id", dom),
          ),
          DbStorage.toInClause("cs.participant_id", par),
        )
          .as[(DomainId, ParticipantId)]

    }

    for {
      data <- storage
        .queryUnlessShutdown(queries, functionFullName)

    } yield data.map {
      case (
            domainId,
            participantId,
          ) =>
        ConfigForNoWaitCounterParticipants(
          domainId,
          participantId,
        )
    }
  }
}
