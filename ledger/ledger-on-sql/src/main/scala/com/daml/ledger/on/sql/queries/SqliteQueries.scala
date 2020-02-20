// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql.queries

import java.sql.Connection

import anorm.SqlParser._
import anorm._
import com.daml.ledger.on.sql.Index
import com.daml.ledger.on.sql.queries.Queries._
import com.daml.ledger.participant.state.v1.LedgerId
import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}

final class SqliteQueries(override protected implicit val connection: Connection)
    extends Queries
    with CommonQueries {
  override def updateOrRetrieveLedgerId(providedLedgerId: LedgerId): LedgerId = {
    SQL"INSERT INTO #$MetaTable (table_key, ledger_id) VALUES ($MetaTableKey, $providedLedgerId) ON CONFLICT DO NOTHING"
      .executeInsert()
    SQL"SELECT ledger_id FROM #$MetaTable WHERE table_key = $MetaTableKey"
      .as(str("ledger_id").single)
  }

  override def insertIntoLog(key: Key, value: Value): Index = {
    SQL"INSERT INTO #$LogTable (entry_id, envelope) VALUES ($key, $value)"
      .executeInsert()
    SQL"SELECT LAST_INSERT_ROWID()"
      .as(long("LAST_INSERT_ROWID()").single)
  }

  override protected val updateStateQuery: String =
    s"INSERT INTO $StateTable VALUES ({key}, {value}) ON CONFLICT(key) DO UPDATE SET value = {value}"
}

object SqliteQueries {
  def apply(connection: Connection): Queries = {
    implicit val conn: Connection = connection
    new SqliteQueries
  }
}
