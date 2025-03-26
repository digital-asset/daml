// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.common

import anorm.SqlParser.{int, long}
import anorm.{ParameterMetaData, RowParser, ToStatement, ~}
import com.daml.scalautil.Statement.discard
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.participant.state.index.MeteringStore.{
  ParticipantMetering,
  ReportData,
}
import com.digitalasset.canton.platform.UserId
import com.digitalasset.canton.platform.store.backend.Conversions.{
  offset,
  timestampFromMicros,
  userId,
}
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.{
  CompositeSql,
  SqlStringInterpolation,
}
import com.digitalasset.canton.platform.store.backend.common.MeteringParameterStorageBackendImpl.assertLedgerMeteringEnd
import com.digitalasset.canton.platform.store.backend.common.MeteringStorageBackendImpl.*
import com.digitalasset.canton.platform.store.backend.common.SimpleSqlExtensions.*
import com.digitalasset.canton.platform.store.backend.{
  Conversions,
  MeteringStorageReadBackend,
  MeteringStorageWriteBackend,
}
import com.digitalasset.daml.lf.data.Time
import com.digitalasset.daml.lf.data.Time.Timestamp

import java.sql.Connection

private[backend] object MeteringStorageBackendImpl {

  val participantMeteringParser: RowParser[ParticipantMetering] =
    (
      userId("user_id") ~
        timestampFromMicros("from_timestamp") ~
        timestampFromMicros("to_timestamp") ~
        int("action_count") ~
        offset("ledger_offset").?
    ).map {
      case userId ~
          from ~
          to ~
          actionCount ~
          ledgerOffset =>
        ParticipantMetering(
          userId,
          from,
          to,
          actionCount,
          ledgerOffset,
        )
    }

  /** Evaluate to the passed condition if the option is non-empty or return true otherwise
    */
  def ifSet[A](o: Option[A], expr: A => CompositeSql): CompositeSql =
    o.fold(cSQL"1=1")(expr)

}

private[backend] object MeteringStorageBackendReadTemplate extends MeteringStorageReadBackend {

  implicit val OffsetToStatement: ToStatement[Offset] =
    Conversions.OffsetToStatement
  implicit val timestampToStatement: ToStatement[Timestamp] =
    Conversions.TimestampToStatement
  implicit val timestampParamMeta: ParameterMetaData[Timestamp] =
    Conversions.TimestampParamMeta

  def userCountParser: RowParser[(UserId, Long)] =
    (userId(columnName = "user_id") ~ long(columnPosition = 2))
      .map { case userId ~ count => userId -> count }

  override def reportData(
      from: Time.Timestamp,
      to: Option[Time.Timestamp],
      maybeUserId: Option[UserId],
  )(connection: Connection): ReportData = {

    val ledgerMeteringEnd = assertLedgerMeteringEnd(connection)
    val participantData = participantMetering(from, to, maybeUserId)(connection)
    val isFinal = to.fold(false)(ledgerMeteringEnd.timestamp >= _)
    val data = if (isFinal) {
      participantData
    } else {
      val transactionData =
        transactionMetering(
          from = ledgerMeteringEnd.offset.fold(Offset.firstOffset)(_.increment),
          to = to,
          userId = maybeUserId,
        )(connection)
      val users: Set[UserId] = participantData.keySet ++ transactionData.keySet
      users.toList.map { a =>
        a -> (participantData.getOrElse(a, 0L) + transactionData.getOrElse(a, 0L))
      }.toMap
    }

    ReportData(data, isFinal)

  }

  /** @param from
    *   Include rows at or after this offset
    * @param to
    *   If specified include rows before this timestamp
    * @param userId
    *   If specified only return rows for this user
    */
  private def transactionMetering(
      from: Offset,
      to: Option[Time.Timestamp],
      userId: Option[String],
  )(connection: Connection): Map[UserId, Long] =
    SQL"""
      select
        user_id,
        sum(action_count)
      from lapi_transaction_metering
      where ledger_offset is not null
      and ledger_offset >= $from
      and ${ifSet[Timestamp](to, t => cSQL"metering_timestamp < $t")}
      and ${ifSet[String](userId, a => cSQL"user_id = $a")}
      group by user_id
    """
      .asVectorOf(userCountParser)(connection)
      .toMap

  /** @param from
    *   Include rows whose aggregation period starts on or after this date
    * @param to
    *   If specified include rows whose aggregation period ends on or before this date
    * @param userId
    *   If specified only return rows for this user
    */
  private def participantMetering(
      from: Time.Timestamp,
      to: Option[Time.Timestamp],
      userId: Option[String],
  )(connection: Connection): Map[UserId, Long] =
    SQL"""
      select
        user_id,
        sum(action_count)
      from lapi_participant_metering
      where from_timestamp >= $from
      and ${ifSet[Timestamp](to, t => cSQL"to_timestamp <= $t")}
      and ${ifSet[String](userId, a => cSQL"user_id = $a")}
      group by user_id
    """
      .asVectorOf(userCountParser)(connection)
      .toMap

}
private[backend] object MeteringStorageBackendWriteTemplate extends MeteringStorageWriteBackend {

  implicit val OffsetToStatement: ToStatement[Offset] =
    Conversions.OffsetToStatement
  implicit val timestampToStatement: ToStatement[Timestamp] =
    Conversions.TimestampToStatement
  implicit val timestampParamMeta: ParameterMetaData[Timestamp] =
    Conversions.TimestampParamMeta

  def userCountParser: RowParser[(UserId, Int)] =
    (userId(columnName = "user_id") ~ int(columnPosition = 2))
      .map { case userId ~ count => userId -> count }

  def transactionMeteringMaxOffset(from: Option[Offset], to: Timestamp)(
      connection: Connection
  ): Option[Offset] =
    SQL"""
      select max(ledger_offset)
      from lapi_transaction_metering
      where ledger_offset is not null
      and ${ifSet[Offset](from, f => cSQL"ledger_offset > $f")}
      and metering_timestamp < $to
    """
      .as(offset(1).?.single)(connection)

  def selectTransactionMetering(from: Option[Offset], to: Offset)(
      connection: Connection
  ): Map[UserId, Int] =
    SQL"""
      select
        user_id,
        sum(action_count)
      from lapi_transaction_metering
      where ledger_offset is not null
      and ${ifSet[Offset](from, f => cSQL"ledger_offset > $f")}
      and ledger_offset <= $to
      group by user_id
    """
      .asVectorOf(userCountParser)(connection)
      .toMap

  def deleteTransactionMetering(from: Option[Offset], to: Offset)(
      connection: Connection
  ): Unit =
    discard(
      SQL"""
      delete from lapi_transaction_metering
      where ledger_offset is not null
      and ${ifSet[Offset](from, f => cSQL"ledger_offset > $f")}
      and ledger_offset <= $to
    """
        .execute()(connection)
    )

  def insertParticipantMetering(metering: Vector[ParticipantMetering])(
      connection: Connection
  ): Unit =
    metering.foreach { participantMetering =>
      import participantMetering.*
      SQL"""
        insert into lapi_participant_metering(user_id, from_timestamp, to_timestamp, action_count, ledger_offset)
        values (${participantMetering.userId.toString}, $from, $to, $actionCount, ${ledgerOffset
          .map(_.unwrap)})
      """.execute()(connection).discard
    }

  def allParticipantMetering()(connection: Connection): Vector[ParticipantMetering] =
    SQL"""
      select
        user_id,
        from_timestamp,
        to_timestamp,
        action_count,
        ledger_offset
      from lapi_participant_metering
    """
      .asVectorOf(participantMeteringParser)(connection)

}
