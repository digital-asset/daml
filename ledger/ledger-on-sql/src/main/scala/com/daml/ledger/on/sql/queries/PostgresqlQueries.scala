// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql.queries

import java.sql.Connection

import anorm.SqlParser._
import anorm._
import com.daml.ledger.on.sql.queries.Queries.Index

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

  override def lastLogInsertId()(implicit connection: Connection): Index = {
    SQL"SELECT last_value FROM log_sequence_no_seq"
      .as(long("last_value").single)
  }

  override protected val updateStateQuery: String =
    "INSERT INTO state VALUES ({key}, {value}) ON CONFLICT(key) DO UPDATE SET value = {value}"
}
