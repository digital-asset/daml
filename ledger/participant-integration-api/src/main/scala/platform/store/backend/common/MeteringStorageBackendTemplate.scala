// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.common

import anorm.SqlParser.{int, long}
import anorm.{ParameterMetaData, RowParser, ToStatement, ~}
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.index.v2.MeteringStore.{
  ParticipantMetering,
  ReportData,
  TransactionMetering,
}
import com.daml.lf.data.Ref.ApplicationId
import com.daml.lf.data.Time
import com.daml.lf.data.Time.Timestamp
import com.daml.platform.store.Conversions.{applicationId, offset, timestampFromMicros}
import com.daml.platform.store.SimpleSqlAsVectorOf.SimpleSqlAsVectorOf
import com.daml.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
import com.daml.platform.store.backend.common.MeteringParameterStorageBackendTemplate.assertLedgerMeteringEnd
import com.daml.platform.store.backend.common.MeteringStorageBackendTemplate._
import com.daml.platform.store.backend.{MeteringStorageReadBackend, MeteringStorageWriteBackend}
import com.daml.scalautil.Statement.discard

import java.sql.Connection

private[backend] object MeteringStorageBackendTemplate {

  val applicationCountParser: RowParser[(ApplicationId, Long)] =
    (applicationId(columnName = "application_id") ~ long(columnPosition = 2))
      .map { case applicationId ~ count => applicationId -> count }

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

  /** Oracle does not understand true/false so compare against number
    * @return 0 if the option is unset and non-zero otherwise
    */
  def isSet(o: Option[_]): Int = o.fold(0)(_ => 1)

  /** Oracle treats zero length strings as null and then does null comparison
    * @return 0 if the offset is beforeBegin and non-zero otherwise
    */
  def hasBegun(o: Offset): Int = if (o == Offset.beforeBegin) 0 else 1

}

private[backend] object MeteringStorageBackendReadTemplate extends MeteringStorageReadBackend {

  implicit val offsetToStatement: ToStatement[Offset] =
    com.daml.platform.store.Conversions.OffsetToStatement
  implicit val timestampToStatement: ToStatement[Timestamp] =
    com.daml.platform.store.Conversions.TimestampToStatement
  implicit val timestampParamMeta: ParameterMetaData[Timestamp] =
    com.daml.platform.store.Conversions.TimestampParamMeta

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

  private def transactionMetering(
      from: Offset,
      to: Option[Time.Timestamp],
      appId: Option[String],
  )(connection: Connection): Map[ApplicationId, Long] = {

    SQL"""
      select
        application_id,
        sum(action_count)
      from transaction_metering
      where (${hasBegun(from)} = 0 or ledger_offset > $from)
      and   (${isSet(to)} = 0 or metering_timestamp < $to)
      and   (${isSet(appId)} = 0 or application_id = $appId)
      group by application_id
    """
      .asVectorOf(applicationCountParser)(connection)
      .toMap

  }

  private def participantMetering(
      from: Time.Timestamp,
      to: Option[Time.Timestamp],
      appId: Option[String],
  )(connection: Connection): Map[ApplicationId, Long] = {

    SQL"""
      select
        application_id,
        sum(action_count)
      from participant_metering
      where from_timestamp >= $from
      and   (${isSet(to)} = 0 or to_timestamp <= $to)
      and   (${isSet(appId)} = 0 or application_id = $appId)
      group by application_id
    """
      .asVectorOf(applicationCountParser)(connection)
      .toMap

  }

}
private[backend] object MeteringStorageBackendWriteTemplate extends MeteringStorageWriteBackend {

  implicit val offsetToStatement: ToStatement[Offset] =
    com.daml.platform.store.Conversions.OffsetToStatement
  implicit val timestampToStatement: ToStatement[Timestamp] =
    com.daml.platform.store.Conversions.TimestampToStatement
  implicit val timestampParamMeta: ParameterMetaData[Timestamp] =
    com.daml.platform.store.Conversions.TimestampParamMeta

  def transactionMeteringMaxOffset(from: Offset, to: Timestamp)(
      connection: Connection
  ): Option[Offset] = {

    SQL"""
      select max(ledger_offset)
      from transaction_metering
      where (${hasBegun(from)} = 0 or ledger_offset > $from)
      and metering_timestamp < $to
    """
      .as(offset(1).?.single)(connection)
  }

  def selectTransactionMetering(from: Offset, to: Offset)(
      connection: Connection
  ): Vector[TransactionMetering] = {

    SQL"""
      select
        application_id,
        action_count,
        metering_timestamp,
        ledger_offset
      from transaction_metering
      where (${hasBegun(from)} = 0 or ledger_offset > $from)
      and ledger_offset <= $to
    """
      .asVectorOf(transactionMeteringParser)(connection)
  }

  def deleteTransactionMetering(from: Offset, to: Offset)(
      connection: Connection
  ): Unit = {

    discard(
      SQL"""
      delete from transaction_metering
      where (${hasBegun(from)} = 0 or ledger_offset > $from)
      and ledger_offset <= $to
    """
        .execute()(connection)
    )
  }

  def insertParticipantMetering(metering: Vector[ParticipantMetering])(
      connection: Connection
  ): Unit = {

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
