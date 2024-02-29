// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.traffic.store.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{PositiveInt, PositiveLong}
import com.digitalasset.canton.domain.sequencing.traffic.store.TrafficLimitsStore
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.DbStorage.{DbAction, Profile}
import com.digitalasset.canton.resource.IdempotentInsert.insertVerifyingConflicts
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.store.db.RequiredTypesCodec.*
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.traffic.TopUpEvent
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.{ExecutionContext, Future}

class DbTrafficLimitsStore(
    override protected val storage: DbStorage,
    protocolVersion: ProtocolVersion,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
) extends TrafficLimitsStore
    with DbStore {

  import Member.DbStorageImplicits.*
  import storage.api.*

  /** Updates the total extra traffic limit for some members.
    *
    * @param partialUpdate members to update, this is expected to contain only members which should be updated, not the whole set of all known members.
    */
  override def updateTotalExtraTrafficLimit(
      partialUpdate: Map[Member, TopUpEvent]
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): Future[Unit] = {
    val dbio = DBIO
      .seq(
        partialUpdate.toSeq.map { case (member, limit) =>
          addLimitDBIO(member, limit)
        }*
      )
      .transactionally

    storage.queryAndUpdate(dbio, functionFullName)
  }

  def addLimitDBIO(member: Member, limit: TopUpEvent)(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): DbAction.All[Unit] =
    insertVerifyingConflicts(
      storage,
      "seq_top_up_events (member, serial)",
      sql"seq_top_up_events (member, effective_timestamp, extra_traffic_limit, serial) values ($member, ${limit.validFromInclusive}, ${limit.limit}, ${limit.serial})",
      sql"select extra_traffic_limit from seq_top_up_events where member = $member and serial = ${limit.serial}"
        .as[PositiveLong]
        .head,
    )(
      _ == limit.limit,
      existingLimit =>
        s"Member [$member] has existing extra_traffic_limit value of [$existingLimit] but we are attempting to insert [$limit]",
    )

  override def getExtraTrafficLimits(member: Member)(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): Future[Seq[TopUpEvent]] = {
    val query = storage.profile match {
      case _: Profile.H2 | _: Profile.Postgres =>
        sql"select effective_timestamp, extra_traffic_limit, serial from seq_top_up_events where member = $member order by (effective_timestamp, serial) asc"
      case Profile.Oracle(_) =>
        sql"select effective_timestamp, extra_traffic_limit, serial from seq_top_up_events where member = $member order by effective_timestamp asc, serial asc"
    }

    storage.query(query.as[TopUpEvent], functionFullName)
  }

  override def pruneBelowSerial(member: Member, upToExclusive: PositiveInt)(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): Future[Unit] = {
    storage.update_(
      sqlu"""delete from seq_top_up_events where member = $member and serial < $upToExclusive""",
      functionFullName,
    )
  }

  override def initialize(
      topUpEvents: Map[Member, TopUpEvent]
  )(implicit ec: ExecutionContext, traceContext: TraceContext): Future[Unit] = {
    val inserts = topUpEvents.map { case (member, topUp) =>
      addLimitDBIO(member, topUp)
    }.toSeq

    val dbio = DBIO.seq(inserts*)

    storage.queryAndUpdate(dbio, functionFullName)
  }
}
