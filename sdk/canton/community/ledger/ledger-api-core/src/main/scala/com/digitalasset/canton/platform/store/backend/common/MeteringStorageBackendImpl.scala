// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.common

import anorm.SqlParser.{int, long}
import anorm.{ParameterMetaData, RowParser, ToStatement, ~}
import com.daml.lf.data.Time
import com.daml.lf.data.Time.Timestamp
import com.daml.scalautil.Statement.discard
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.participant.state.index.MeteringStore.{
  ParticipantMetering,
  ReportData,
}
import com.digitalasset.canton.platform.ApplicationId
import com.digitalasset.canton.platform.store.backend.Conversions.{
  applicationId,
  offset,
  timestampFromMicros,
}
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.{
  CompositeSql,
  SqlStringInterpolation,
}
import com.digitalasset.canton.platform.store.backend.common.MeteringParameterStorageBackendImpl.assertLedgerMeteringEnd
import com.digitalasset.canton.platform.store.backend.common.MeteringStorageBackendImpl.*
import com.digitalasset.canton.platform.store.backend.common.SimpleSqlAsVectorOf.*
import com.digitalasset.canton.platform.store.backend.{
  Conversions,
  MeteringStorageReadBackend,
  MeteringStorageWriteBackend,
}

import java.sql.Connection

private[backend] object MeteringStorageBackendImpl {

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

  /**  Evaluate to the passed condition if the option is non-empty or return true otherwise
    */
  def ifSet[A](o: Option[A], expr: A => CompositeSql): CompositeSql = {
    o.fold(cSQL"1=1")(expr)
  }

  /**  Evaluate to the passed condition if the offset > `beforeBegin` or return true otherwise
    */
  def ifBegun(offset: Offset, expr: Offset => CompositeSql): CompositeSql = {
    if (offset == Offset.beforeBegin) cSQL"1=1" else expr(offset)
  }

}

private[backend] object MeteringStorageBackendReadTemplate extends MeteringStorageReadBackend {

  implicit val offsetToStatement: ToStatement[Offset] =
    Conversions.OffsetToStatement
  implicit val timestampToStatement: ToStatement[Timestamp] =
    Conversions.TimestampToStatement
  implicit val timestampParamMeta: ParameterMetaData[Timestamp] =
    Conversions.TimestampParamMeta

  def applicationCountParser: RowParser[(ApplicationId, Long)] =
    (applicationId(columnName = "application_id") ~ long(columnPosition = 2))
      .map { case applicationId ~ count => applicationId -> count }

  override def reportData(
      from: Time.Timestamp,
      to: Option[Time.Timestamp],
      maybeApplicationId: Option[ApplicationId],
  )(connection: Connection): ReportData = {

    val ledgerMeteringEnd = assertLedgerMeteringEnd(connection)
    val participantData = participantMetering(from, to, maybeApplicationId)(connection)
    val isFinal = to.fold(false)(ledgerMeteringEnd.timestamp >= _)
    val data = if (isFinal) {
      participantData
    } else {
      val transactionData =
        transactionMetering(ledgerMeteringEnd.offset, to, maybeApplicationId)(connection)
      val apps: Set[ApplicationId] = participantData.keySet ++ transactionData.keySet
      apps.toList.map { a =>
        a -> (participantData.getOrElse(a, 0L) + transactionData.getOrElse(a, 0L))
      }.toMap
    }

    ReportData(data, isFinal)

  }

  /** @param from - Include rows after this offset
    * @param to - If specified include rows before this timestamp
    * @param appId - If specified only return rows for this application
    */
  private def transactionMetering(
      from: Offset,
      to: Option[Time.Timestamp],
      appId: Option[String],
  )(connection: Connection): Map[ApplicationId, Long] = {

    SQL"""
      select
        application_id,
        sum(action_count)
      from lapi_transaction_metering
      where ${ifBegun(from, f => cSQL"ledger_offset > $f")}
      and   ${ifSet[Timestamp](to, t => cSQL"metering_timestamp < $t")}
      and   ${ifSet[String](appId, a => cSQL"application_id = $a")}
      group by application_id
    """
      .asVectorOf(applicationCountParser)(connection)
      .toMap

  }

  /** @param from - Include rows whose aggregation period starts on or after this date
    * @param to - If specified include rows whose aggregation period ends on or before this date
    * @param appId - If specified only return rows for this application
    */
  private def participantMetering(
      from: Time.Timestamp,
      to: Option[Time.Timestamp],
      appId: Option[String],
  )(connection: Connection): Map[ApplicationId, Long] = {

    SQL"""
      select
        application_id,
        sum(action_count)
      from lapi_participant_metering
      where from_timestamp >= $from
      and ${ifSet[Timestamp](to, t => cSQL"to_timestamp <= $t")}
      and ${ifSet[String](appId, a => cSQL"application_id = $a")}
      group by application_id
    """
      .asVectorOf(applicationCountParser)(connection)
      .toMap

  }

}
private[backend] object MeteringStorageBackendWriteTemplate extends MeteringStorageWriteBackend {

  implicit val offsetToStatement: ToStatement[Offset] =
    Conversions.OffsetToStatement
  implicit val timestampToStatement: ToStatement[Timestamp] =
    Conversions.TimestampToStatement
  implicit val timestampParamMeta: ParameterMetaData[Timestamp] =
    Conversions.TimestampParamMeta

  def applicationCountParser: RowParser[(ApplicationId, Int)] =
    (applicationId(columnName = "application_id") ~ int(columnPosition = 2))
      .map { case applicationId ~ count => applicationId -> count }

  def transactionMeteringMaxOffset(from: Offset, to: Timestamp)(
      connection: Connection
  ): Option[Offset] = {

    SQL"""
      select max(ledger_offset)
      from lapi_transaction_metering
      where ${ifBegun(from, f => cSQL"ledger_offset > $f")}
      and metering_timestamp < $to
    """
      .as(offset(1).?.single)(connection)
  }

  def selectTransactionMetering(from: Offset, to: Offset)(
      connection: Connection
  ): Map[ApplicationId, Int] = {

    SQL"""
      select
        application_id,
        sum(action_count)
      from lapi_transaction_metering
      where ${ifBegun(from, f => cSQL"ledger_offset > $f")}
      and ledger_offset <= $to
      group by application_id
    """
      .asVectorOf(applicationCountParser)(connection)
      .toMap

  }

  def deleteTransactionMetering(from: Offset, to: Offset)(
      connection: Connection
  ): Unit = {

    discard(
      SQL"""
      delete from lapi_transaction_metering
      where ${ifBegun(from, f => cSQL"ledger_offset > $f")}
      and ledger_offset <= $to
    """
        .execute()(connection)
    )
  }

  def insertParticipantMetering(metering: Vector[ParticipantMetering])(
      connection: Connection
  ): Unit = {

    metering.foreach { participantMetering =>
      import participantMetering.*
      SQL"""
        insert into lapi_participant_metering(application_id, from_timestamp, to_timestamp, action_count, ledger_offset)
        values (${participantMetering.applicationId.toString}, $from, $to, $actionCount, $ledgerOffset)
      """.execute()(connection).discard
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
      from lapi_participant_metering
    """
      .asVectorOf(participantMeteringParser)(connection)
  }

}
