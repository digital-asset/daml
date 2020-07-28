// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql.queries

import java.sql.Connection

import anorm.SqlParser._
import anorm._
import com.daml.ledger.on.sql.Index
import com.daml.ledger.on.sql.queries.Queries._
import com.daml.ledger.participant.state.v1.LedgerId
import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}

import scala.util.Try

final class PostgresqlQueries(override protected implicit val connection: Connection)
    extends Queries
    with CommonQueries {
  override def updateOrRetrieveLedgerId(providedLedgerId: LedgerId): Try[LedgerId] = Try {
    SQL"INSERT INTO #$MetaTable (table_key, ledger_id) VALUES ($MetaTableKey, $providedLedgerId) ON CONFLICT DO NOTHING"
      .executeInsert()
    SQL"SELECT ledger_id FROM #$MetaTable WHERE table_key = $MetaTableKey"
      .as(str("ledger_id").single)
  }

  override def insertRecordIntoLog(key: Key, value: Value): Try[Index] = Try {
    SQL"INSERT INTO #$LogTable (entry_id, envelope) VALUES ($key, $value) RETURNING sequence_no"
      .as(long("sequence_no").single)
  }

  override protected val updateStateQuery: String =
    s"INSERT INTO $StateTable VALUES ({key}, {value}) ON CONFLICT(key) DO UPDATE SET value = {value}"

  override final def truncate(): Try[Unit] = Try {
    SQL"truncate #$StateTable".executeUpdate()
    SQL"truncate #$LogTable".executeUpdate()
    SQL"truncate #$MetaTable".executeUpdate()
    ()
  }
}

object PostgresqlQueries {
  def apply(connection: Connection): Queries = {
    implicit val conn: Connection = connection
    new PostgresqlQueries
  }
}
