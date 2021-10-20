// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql.queries

import java.sql.Connection

import anorm.SqlParser._
import anorm._
import com.daml.ledger.configuration.LedgerId
import com.daml.ledger.on.sql.Index
import com.daml.ledger.on.sql.queries.Queries._
import com.daml.ledger.participant.state.kvutils.{KVOffsetBuilder, Raw}

import scala.util.Try

final class H2Queries(offsetBuilder: KVOffsetBuilder)(implicit connection: Connection)
    extends CommonQueries(offsetBuilder) {
  override def updateOrRetrieveLedgerId(providedLedgerId: LedgerId): Try[LedgerId] = Try {
    SQL"MERGE INTO #$MetaTable USING DUAL ON table_key = $MetaTableKey WHEN NOT MATCHED THEN INSERT (table_key, ledger_id) VALUES ($MetaTableKey, $providedLedgerId)"
      .executeInsert()
    SQL"SELECT ledger_id FROM #$MetaTable WHERE table_key = $MetaTableKey"
      .as(str("ledger_id").single)
  }

  override def insertRecordIntoLog(key: Raw.LogEntryId, value: Raw.Envelope): Try[Index] =
    Try {
      SQL"INSERT INTO #$LogTable (entry_id, envelope) VALUES ($key, $value)"
        .executeInsert()
      ()
    }.flatMap(_ => lastInsertId())

  override protected val updateStateQuery: String =
    s"MERGE INTO $StateTable(key, key_hash, value) KEY(key_hash) VALUES ({key}, {key_hash}, {value})"

  private def lastInsertId(): Try[Index] = Try {
    SQL"CALL IDENTITY()"
      .as(long("IDENTITY()").single)
  }

  override def truncate(): Try[Unit] = Try {
    SQL"truncate #$StateTable".executeUpdate()
    SQL"truncate #$LogTable".executeUpdate()
    SQL"truncate #$MetaTable".executeUpdate()
    ()
  }
}

object H2Queries extends QueriesFactory {
  override def apply(offsetBuilder: KVOffsetBuilder, connection: Connection): Queries = {
    implicit val conn: Connection = connection
    new H2Queries(offsetBuilder)
  }
}
