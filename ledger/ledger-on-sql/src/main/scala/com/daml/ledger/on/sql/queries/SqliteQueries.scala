// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql.queries

import java.sql.Connection

import anorm.SqlParser._
import anorm._
import com.daml.ledger.on.sql.queries.Queries.Index

class SqliteQueries extends Queries with CommonQueries {
  override def createLogTable()(implicit connection: Connection): Unit = {
    SQL"CREATE TABLE IF NOT EXISTS log (sequence_no INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, entry_id VARBINARY(16384) NOT NULL, envelope BLOB NOT NULL)"
      .execute()
    ()
  }

  override def lastLogInsertId()(implicit connection: Connection): Index = {
    SQL"SELECT LAST_INSERT_ROWID()"
      .as(long("LAST_INSERT_ROWID()").single)
  }

  override protected val updateStateQuery: String =
    "INSERT INTO state VALUES ({key}, {value}) ON CONFLICT(key) DO UPDATE SET value = {value}"
}
