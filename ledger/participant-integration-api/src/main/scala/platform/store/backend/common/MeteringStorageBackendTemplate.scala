// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.common

import anorm.SqlParser.int
import anorm.{RowParser, ~}
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.index.v2.MeteringStore.{
  ParticipantMetering,
  TransactionMetering,
}
import com.daml.lf.data.Ref.ApplicationId
import com.daml.lf.data.Time
import com.daml.lf.data.Time.Timestamp
import com.daml.platform.store.Conversions.{applicationId, offset, timestampFromMicros}
import com.daml.platform.store.SimpleSqlAsVectorOf.SimpleSqlAsVectorOf
import com.daml.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
import com.daml.platform.store.backend.common.MeteringStorageBackendTemplate.transactionMeteringParser
import com.daml.platform.store.backend.{MeteringStorageReadBackend, MeteringStorageWriteBackend}
import com.daml.platform.store.cache.LedgerEndCache

import java.sql.Connection

private[backend] object MeteringStorageBackendTemplate {

  val transactionMeteringParser: RowParser[TransactionMetering] = {
    (
      applicationId("application_id") ~
        int("action_count") ~
        timestampFromMicros("metering_timestamp") ~
        offset("ledger_offset")
    ).map {
      case applicationId ~
          actionCount ~
          meteringTimestamp ~
          ledgerOffset =>
        TransactionMetering(
          applicationId = applicationId,
          actionCount = actionCount,
          meteringTimestamp = meteringTimestamp,
          ledgerOffset = ledgerOffset,
        )
    }

  }

}

private[backend] class MeteringStorageBackendReadTemplate(ledgerEndCache: LedgerEndCache)
    extends MeteringStorageReadBackend {

  override def transactionMetering(
      from: Time.Timestamp,
      to: Option[Time.Timestamp],
      applicationId: Option[ApplicationId],
  )(connection: Connection): Vector[TransactionMetering] = {
    SQL"""
      select
        application_id,
        action_count,
        metering_timestamp,
        ledger_offset
      from transaction_metering
      where ledger_offset <= ${ledgerEndCache()._1.toHexString.toString}
      and   metering_timestamp >= ${from.micros}
      and   (${isSet(to)} = 0 or metering_timestamp < ${to.map(_.micros)})
      and   (${isSet(applicationId)} = 0 or application_id = ${applicationId.map(_.toString)})
    """
      .asVectorOf(transactionMeteringParser)(connection)
  }

  /** Oracle does not understand true/false so compare against number
    * @return 0 if the option is unset and non-zero otherwise
    */
  private def isSet(o: Option[_]): Int = o.fold(0)(_ => 1)

}
private[backend] object MeteringStorageBackendWriteTemplate extends MeteringStorageWriteBackend {

  val participantMeteringParser: RowParser[ParticipantMetering] = {
    (
      applicationId("application_id") ~
        timestampFromMicros("from_timestamp") ~
        timestampFromMicros("to_timestamp") ~
        int("action_count") ~
        offset("ledger_offset")
    ).map {
      case applicationId ~
          from ~
          to ~
          actionCount ~
          ledgerOffset =>
        ParticipantMetering(
          applicationId,
          from,
          to,
          actionCount,
          ledgerOffset,
        )
    }

  }

  def transactionMeteringMaxOffset(from: Offset, to: Timestamp)(
      connection: Connection
  ): Option[Offset] = {

    import com.daml.platform.store.Conversions.OffsetToStatement
    import com.daml.platform.store.Conversions.TimestampToStatement

    SQL"""
      select max(ledger_offset)
      from transaction_metering
      where ledger_offset > $from
      and metering_timestamp < $to
    """
      .as(offset(1).?.single)(connection)
  }

  def transactionMetering(from: Offset, to: Offset)(
      connection: Connection
  ): Vector[TransactionMetering] = {

    import com.daml.platform.store.Conversions.OffsetToStatement

    SQL"""
      select
        application_id,
        action_count,
        metering_timestamp,
        ledger_offset
      from transaction_metering
      where ledger_offset > $from
      and ledger_offset <= $to
    """
      .asVectorOf(transactionMeteringParser)(connection)
  }

  def insertParticipantMetering(metering: Vector[ParticipantMetering])(
      connection: Connection
  ): Unit = {

    import com.daml.platform.store.Conversions.OffsetToStatement
    import com.daml.platform.store.Conversions.TimestampToStatement

    metering.foreach { participantMetering =>
      import participantMetering._
      SQL"""
        insert into participant_metering(application_id, from_timestamp, to_timestamp, action_count, ledger_offset)
        values (${participantMetering.applicationId.toString}, $from, $to, $actionCount, $ledgerOffset)
      """.execute()(connection)
    }

  }

  def allParticipantMetering()(connection: Connection): Vector[ParticipantMetering] = {
    SQL"""
      select
        application_id,
        from_timestamp,
        to_timestamp,
        action_count,
        ledger_offset
      from participant_metering
    """
      .asVectorOf(participantMeteringParser)(connection)
  }

}
