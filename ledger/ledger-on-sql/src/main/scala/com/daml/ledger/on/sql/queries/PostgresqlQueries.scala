// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql.queries

import java.sql.Connection

import anorm.SqlParser._
import anorm._
import com.daml.ledger.configuration.LedgerId
import com.daml.ledger.on.sql.Index
import com.daml.ledger.on.sql.queries.Queries._
import com.daml.ledger.participant.state.kvutils.{Raw, VersionedOffsetBuilder}

import scala.util.Try

final class PostgresqlQueries(
    offsetBuilder: VersionedOffsetBuilder
)(implicit connection: Connection)
    extends CommonQueries(offsetBuilder) {
  override def updateOrRetrieveLedgerId(providedLedgerId: LedgerId): Try[LedgerId] = Try {
    SQL"INSERT INTO #$MetaTable (table_key, ledger_id) VALUES ($MetaTableKey, $providedLedgerId) ON CONFLICT DO NOTHING"
      .executeInsert()
    SQL"SELECT ledger_id FROM #$MetaTable WHERE table_key = $MetaTableKey"
      .as(str("ledger_id").single)
  }

  override def insertRecordIntoLog(key: Raw.LogEntryId, value: Raw.Envelope): Try[Index] = Try {
    SQL"INSERT INTO #$LogTable (entry_id, envelope) VALUES ($key, $value) RETURNING sequence_no"
      .as(long("sequence_no").single)
  }

  override protected val updateStateQuery: String =
    s"INSERT INTO $StateTable (key, key_hash, value) VALUES ({key}, {key_hash}, {value}) ON CONFLICT(key_hash) DO UPDATE SET value = {value}"

  override def truncate(): Try[Unit] = Try {
    SQL"truncate #$StateTable".executeUpdate()
    SQL"truncate #$LogTable".executeUpdate()
    SQL"truncate #$MetaTable".executeUpdate()
    ()
  }
}

object PostgresqlQueries extends QueriesFactory {
  override def apply(offsetBuilder: VersionedOffsetBuilder, connection: Connection): Queries = {
    implicit val conn: Connection = connection
    new PostgresqlQueries(offsetBuilder)
  }
}
