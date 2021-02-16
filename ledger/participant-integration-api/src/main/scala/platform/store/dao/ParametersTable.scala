// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import java.sql.Connection

import anorm.SqlParser.byteArray
import anorm.{Row, RowParser, SimpleSql, SqlStringInterpolation, ~}
import com.daml.ledger.api.domain.{LedgerId, ParticipantId}
import com.daml.ledger.participant.state.v1.{Configuration, Offset}
import com.daml.platform.indexer.{IncrementalOffsetStep, CurrentOffset, OffsetStep}
import com.daml.platform.store.Conversions.{OffsetToStatement, ledgerString, offset, participantId}
import com.daml.scalautil.Statement.discard

private[dao] object ParametersTable {

  private val TableName: String = "parameters"
  private val LedgerIdColumnName: String = "ledger_id"
  private val ParticipantIdColumnName: String = "participant_id"
  private val LedgerEndColumnName: String = "ledger_end"
  private val ConfigurationColumnName: String = "configuration"

  private val LedgerIdParser: RowParser[LedgerId] =
    ledgerString(LedgerIdColumnName).map(LedgerId(_))

  private val ParticipantIdParser: RowParser[Option[ParticipantId]] =
    participantId(ParticipantIdColumnName).map(ParticipantId(_)).?

  private val LedgerEndParser: RowParser[Option[Offset]] =
    offset(LedgerEndColumnName).?

  private val LedgerEndOrBeforeBeginParser: RowParser[Offset] =
    LedgerEndParser.map(_.getOrElse(Offset.beforeBegin))

  private val ConfigurationParser: RowParser[Option[Configuration]] =
    byteArray(ConfigurationColumnName).? map (_.flatMap(Configuration.decode(_).toOption))

  private val LedgerEndAndConfigurationParser: RowParser[Option[(Offset, Configuration)]] =
    LedgerEndParser ~ ConfigurationParser map { case ledgerEnd ~ configuration =>
      for {
        e <- ledgerEnd
        c <- configuration
      } yield (e, c)
    }

  private val SelectLedgerEnd: SimpleSql[Row] = SQL"select #$LedgerEndColumnName from #$TableName"

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

  def getInitialLedgerEnd(connection: Connection): Option[Offset] =
    SelectLedgerEnd.as(LedgerEndParser.single)(connection)

  /** Updates the ledger end.
    *
    * When provided with a (previous, current) ledger end tuple ([[IncrementalOffsetStep]],
    * the update is performed conditioned by the match between the persisted ledger end and the
    * provided previous ledger end.
    *
    * This mechanism is used to protect callers that cannot provide strong durability guarantees
    * ([[JdbcLedgerDao]] when used with asynchronous commits on PostgreSQL).
    *
    * @param offsetStep The offset step.
    * @param connection The SQL connection.
    */
  def updateLedgerEnd(offsetStep: OffsetStep)(implicit connection: Connection): Unit =
    offsetStep match {
      case CurrentOffset(ledgerEnd) =>
        // TODO FIXME crude initial approach to track the ledger end for the sequential id as well. needs to be implemented properly later. @simon@ I would not place high emphasiz on this ATM since parallel ingestion implementation likely change this naturally.
        discard(
          SQL"update #$TableName set #$LedgerEndColumnName = $ledgerEnd, ledger_end_sequential_id = nextval('participant_events_event_sequential_id_seq') where (#$LedgerEndColumnName is null or #$LedgerEndColumnName < $ledgerEnd)"
            .execute()
        )
      case IncrementalOffsetStep(previousOffset, ledgerEnd) =>
        val sqlStatement =
          SQL"update #$TableName set #$LedgerEndColumnName = $ledgerEnd, ledger_end_sequential_id = nextval('participant_events_event_sequential_id_seq') where #$LedgerEndColumnName = $previousOffset"
        if (sqlStatement.executeUpdate() == 0) {
          throw LedgerEndUpdateError(previousOffset)
        }
    }

  def updateConfiguration(configuration: Array[Byte])(implicit
      connection: Connection
  ): Unit =
    discard(SQL"update #$TableName set #$ConfigurationColumnName = $configuration".execute())

  def getLedgerEndAndConfiguration(connection: Connection): Option[(Offset, Configuration)] =
    SQL"select #$LedgerEndColumnName, #$ConfigurationColumnName from #$TableName".as(
      LedgerEndAndConfigurationParser.single
    )(connection)

  case class LedgerEndUpdateError(expected: Offset)
      extends RuntimeException(
        s"Could not update ledger end. Previous ledger end does not match expected ${expected.toHexString}"
      )
}
