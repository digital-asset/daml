// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql

import com.daml.testing.postgresql.PostgresAroundAll

import scala.collection.mutable

class PostgresqlSqlLedgerReaderWriterIntegrationSpec
    extends SqlLedgerReaderWriterIntegrationSpecBase("SQL implementation using PostgreSQL")
    with PostgresAroundAll {

  private val databases: mutable.Map[String, String] = mutable.Map.empty

  override protected def jdbcUrl(id: String): String = {
    if (!databases.contains(id)) {
      val database = createNewDatabase(id)
      databases += id -> database.url
    }
    databases(id)
  }
}
