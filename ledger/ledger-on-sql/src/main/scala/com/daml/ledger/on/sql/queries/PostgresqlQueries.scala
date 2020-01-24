// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql.queries

import java.sql.Connection

import anorm.SqlParser._
import anorm._
import com.daml.ledger.on.sql.queries.Queries.Index
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlLogEntryId
import com.google.protobuf.ByteString

class PostgresqlQueries extends Queries with CommonQueries {
  override def createLogTable()(implicit connection: Connection): Unit = {
    SQL"CREATE TABLE IF NOT EXISTS log (sequence_no SERIAL PRIMARY KEY, entry_id BYTEA NOT NULL, envelope BYTEA NOT NULL)"
      .execute()
    ()
  }

  override def createStateTable()(implicit connection: Connection): Unit = {
    SQL"CREATE TABLE IF NOT EXISTS state (key BYTEA PRIMARY KEY NOT NULL, value BYTEA NOT NULL)"
      .execute()
    ()
  }

  override def insertIntoLog(
      entry: DamlLogEntryId,
      envelope: ByteString,
  )(implicit connection: Connection): Index = {
    SQL"INSERT INTO log (entry_id, envelope) VALUES (${entry.getEntryId.toByteArray}, ${envelope.toByteArray}) RETURNING sequence_no"
      .as(long("sequence_no").single)
  }

  override protected val updateStateQuery: String =
    "INSERT INTO state VALUES ({key}, {value}) ON CONFLICT(key) DO UPDATE SET value = {value}"
}
