// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.CantonRequireTypes.String36
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.TimedLoadGauge
import com.digitalasset.canton.participant.GlobalOffset
import com.digitalasset.canton.participant.store.ParticipantPruningStore
import com.digitalasset.canton.participant.store.ParticipantPruningStore.ParticipantPruningStatus
import com.digitalasset.canton.resource.DbStorage.Profile.{H2, Oracle, Postgres}
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.tracing.TraceContext
import slick.jdbc.GetResult

import scala.concurrent.{ExecutionContext, Future}

class DbParticipantPruningStore(
    name: String36,
    override protected val storage: DbStorage,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends ParticipantPruningStore
    with DbStore {

  import storage.api.*

  private val processingTime: TimedLoadGauge =
    storage.metrics.loadGaugeM("participant-pruning-store")

  override def markPruningStarted(
      upToInclusive: GlobalOffset
  )(implicit traceContext: TraceContext): Future[Unit] =
    processingTime.event {
      val upsertQuery = storage.profile match {
        case _: Postgres =>
          sqlu"""insert into pruning_operation as po (name, started_up_to_inclusive, completed_up_to_inclusive)
                 values ($name, $upToInclusive, null)
                 on conflict (name) do
                   update set started_up_to_inclusive = $upToInclusive
                   where po.started_up_to_inclusive is null or po.started_up_to_inclusive < $upToInclusive"""
        case _: H2 =>
          sqlu"""merge into pruning_operation using dual on (name = $name)
                 when matched and (started_up_to_inclusive is null or started_up_to_inclusive < $upToInclusive) then
                   update set started_up_to_inclusive = $upToInclusive
                 when not matched then
                   insert (name, started_up_to_inclusive, completed_up_to_inclusive)
                   values ($name, $upToInclusive, null)"""
        case _: Oracle =>
          sqlu"""merge into pruning_operation using dual on (name = $name)
                 when matched then
                   update set started_up_to_inclusive = $upToInclusive
                   where started_up_to_inclusive is null or started_up_to_inclusive < $upToInclusive
                 when not matched then
                   insert (name, started_up_to_inclusive, completed_up_to_inclusive)
                   values ($name, $upToInclusive, null)"""
      }

      storage.update_(upsertQuery, functionFullName)
    }

  override def markPruningDone(
      upToInclusive: GlobalOffset
  )(implicit traceContext: TraceContext): Future[Unit] =
    processingTime.event {
      storage.update_(
        sqlu"""update pruning_operation set completed_up_to_inclusive = $upToInclusive
                       where name = $name and (completed_up_to_inclusive is null or completed_up_to_inclusive < $upToInclusive)""",
        functionFullName,
      )
    }

  private implicit val readParticipantPruningStatus: GetResult[ParticipantPruningStatus] =
    GetResult { r =>
      val started = r.<<[Option[GlobalOffset]]
      val completed = r.<<[Option[GlobalOffset]]
      ParticipantPruningStatus(started, completed)
    }

  override def pruningStatus()(implicit
      traceContext: TraceContext
  ): Future[ParticipantPruningStatus] =
    processingTime.event {
      for {
        statusO <- storage.query(
          sql"""select started_up_to_inclusive, completed_up_to_inclusive from pruning_operation
               where name = $name""".as[ParticipantPruningStatus].headOption,
          functionFullName,
        )
      } yield statusO.getOrElse(ParticipantPruningStatus(None, None))
    }
}
