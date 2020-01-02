// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql.queries

import java.sql.Connection

import anorm.SqlParser._
import anorm._
import com.daml.ledger.on.sql.queries.Queries.Index

class H2Queries extends Queries with CommonQueries {
  override def createLogTable()(implicit connection: Connection): Unit = {
    SQL"CREATE TABLE IF NOT EXISTS log (sequence_no IDENTITY PRIMARY KEY, entry_id VARBINARY(16384), envelope BLOB)"
      .execute()
    ()
  }

  override def lastLogInsertId()(implicit connection: Connection): Index =
    SQL"CALL IDENTITY()"
      .as(long("IDENTITY()").single)

  override protected val updateStateQuery: String =
    "MERGE INTO state VALUES ({key}, {value})"
}
