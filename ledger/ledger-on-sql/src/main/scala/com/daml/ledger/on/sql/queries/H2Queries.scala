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

final class H2Queries(override protected implicit val connection: Connection)
    extends Queries
    with CommonQueries {
  override def updateOrRetrieveLedgerId(providedLedgerId: LedgerId): LedgerId = {
    SQL"MERGE INTO #$MetaTable USING DUAL ON table_key = $MetaTableKey WHEN NOT MATCHED THEN INSERT (table_key, ledger_id) VALUES ($MetaTableKey, $providedLedgerId)"
      .executeInsert()
    SQL"SELECT ledger_id FROM #$MetaTable WHERE table_key = $MetaTableKey"
      .as(str("ledger_id").single)
  }

  override def insertIntoLog(key: Key, value: Value): Index = {
    SQL"INSERT INTO #$LogTable (entry_id, envelope) VALUES ($key, $value)"
      .executeInsert()
    SQL"CALL IDENTITY()"
      .as(long("IDENTITY()").single)
  }

  override protected val updateStateQuery: String =
    s"MERGE INTO $StateTable VALUES ({key}, {value})"
}

object H2Queries {
  def apply(connection: Connection): Queries = {
    implicit val conn: Connection = connection
    new H2Queries
  }
}
