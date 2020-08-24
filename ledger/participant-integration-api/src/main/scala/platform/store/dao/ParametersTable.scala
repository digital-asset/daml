// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import java.sql.Connection

import anorm.SqlParser.byteArray
import anorm.{Row, RowParser, SimpleSql, SqlStringInterpolation, ~}
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.participant.state.v1.{Configuration, Offset}
import com.daml.platform.store.Conversions.{ledgerString, offset, OffsetToStatement}
import com.daml.scalautil.Statement.discard

private[dao] object ParametersTable {

  private val TableName: String = "parameters"
  private val LedgerIdColumnName: String = "ledger_id"
  private val LedgerEndColumnName: String = "ledger_end"
  private val ConfigurationColumnName: String = "configuration"

  private val LedgerIdParser: RowParser[LedgerId] =
    ledgerString(LedgerIdColumnName).map(LedgerId(_))

  private val LedgerEndParser: RowParser[Option[Offset]] =
    offset(LedgerEndColumnName).?

  private val LedgerEndOrBeforeBeginParser: RowParser[Offset] =
    LedgerEndParser.map(_.getOrElse(Offset.beforeBegin))

  private val ConfigurationParser: RowParser[Option[Configuration]] =
    byteArray(ConfigurationColumnName).? map (_.flatMap(Configuration.decode(_).toOption))

  private val LedgerEndAndConfigurationParser: RowParser[Option[(Offset, Configuration)]] =
    LedgerEndParser ~ ConfigurationParser map {
      case ledgerEnd ~ configuration =>
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

  def getLedgerEnd(connection: Connection): Offset =
    SelectLedgerEnd.as(LedgerEndOrBeforeBeginParser.single)(connection)

  def getInitialLedgerEnd(connection: Connection): Option[Offset] =
    SelectLedgerEnd.as(LedgerEndParser.single)(connection)

  def updateLedgerEnd(ledgerEnd: Offset)(implicit connection: Connection): Unit =
    discard(
      SQL"update #$TableName set #$LedgerEndColumnName = $ledgerEnd where (#$LedgerEndColumnName is null or #$LedgerEndColumnName < $ledgerEnd)"
        .execute()
    )

  def updateConfiguration(configuration: Array[Byte])(
      implicit connection: Connection,
  ): Unit =
    discard(SQL"update #$TableName set #$ConfigurationColumnName = $configuration".execute())

  def getLedgerEndAndConfiguration(connection: Connection): Option[(Offset, Configuration)] =
    SQL"select #$LedgerEndColumnName, #$ConfigurationColumnName from #$TableName".as(
      LedgerEndAndConfigurationParser.single
    )(connection)

}
