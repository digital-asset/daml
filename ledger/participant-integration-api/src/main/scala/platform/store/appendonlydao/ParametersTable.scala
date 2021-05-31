// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.appendonlydao

import java.sql.Connection

import anorm.SqlParser.long
import anorm.{Row, RowParser, SimpleSql, SqlParser, SqlStringInterpolation}
import com.daml.ledger.api.domain.{LedgerId, ParticipantId}
import com.daml.ledger.participant.state.v1.Offset
import com.daml.platform.store.Conversions.{ledgerString, offset, participantId}
import com.daml.scalautil.Statement.discard

private[appendonlydao] object ParametersTable {
  private val TableName: String = "parameters"
  private val LedgerIdColumnName: String = "ledger_id"
  private val ParticipantIdColumnName: String = "participant_id"
  private val LedgerEndColumnName: String = "ledger_end"
  private val LedgerEndSequentialIdColumnName: String = "ledger_end_sequential_id"

  private val LedgerIdParser: RowParser[LedgerId] =
    ledgerString(LedgerIdColumnName).map(LedgerId(_))

  private val ParticipantIdParser: RowParser[Option[ParticipantId]] =
    participantId(ParticipantIdColumnName).map(ParticipantId(_)).?

  private val LedgerEndParser: RowParser[Option[Offset]] =
    offset(LedgerEndColumnName).?

  private val LedgerEndOrBeforeBeginParser: RowParser[Offset] =
    LedgerEndParser.map(_.getOrElse(Offset.beforeBegin))

  private val LedgerEndOffsetAndSequentialIdParser =
    (offset(LedgerEndColumnName).? ~ long(LedgerEndSequentialIdColumnName).?)
      .map(SqlParser.flatten)
      .map {
        case (Some(offset), Some(seqId)) => (offset, seqId)
        case (Some(offset), None) => (offset, EventSequentialId.beforeBegin)
        case (None, None) => (Offset.beforeBegin, EventSequentialId.beforeBegin)
        case (None, Some(_)) =>
          throw InvalidLedgerEnd("Parameters table in invalid state: ledger_end is not set")
      }

  private val SelectLedgerEnd: SimpleSql[Row] = SQL"select #$LedgerEndColumnName from #$TableName"

  private val SelectLedgerEndOffsetAndSequentialId =
    SQL"select #$LedgerEndColumnName, #$LedgerEndSequentialIdColumnName from #$TableName"

  def getLedgerId(connection: Connection): Option[LedgerId] =
    SQL"select #$LedgerIdColumnName from #$TableName".as(LedgerIdParser.singleOpt)(connection)

  def setLedgerId(ledgerId: String)(connection: Connection): Unit =
    discard(
      SQL"insert into #$TableName(#$LedgerIdColumnName) values($ledgerId)".execute()(connection)
    )

  def getParticipantId(connection: Connection): Option[ParticipantId] =
    SQL"select #$ParticipantIdColumnName from #$TableName".as(ParticipantIdParser.single)(
      connection
    )

  def setParticipantId(participantId: String)(connection: Connection): Unit =
    discard(
      SQL"update #$TableName set #$ParticipantIdColumnName = $participantId".execute()(
        connection
      )
    )

  def getLedgerEnd(connection: Connection): Offset =
    SelectLedgerEnd.as(LedgerEndOrBeforeBeginParser.single)(connection)

  // TODO mutable contract state cache - use only one getLedgerEnd method
  def getLedgerEndOffsetAndSequentialId(connection: Connection): (Offset, Long) =
    SelectLedgerEndOffsetAndSequentialId.as(LedgerEndOffsetAndSequentialIdParser.single)(connection)

  def getInitialLedgerEnd(connection: Connection): Option[Offset] =
    SelectLedgerEnd.as(LedgerEndParser.single)(connection)

  case class InvalidLedgerEnd(msg: String) extends RuntimeException(msg)

}

private[platform] object EventSequentialId {
  val beforeBegin = 0L
}
