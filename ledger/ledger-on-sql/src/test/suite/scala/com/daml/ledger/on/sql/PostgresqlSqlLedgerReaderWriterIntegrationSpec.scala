// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql

import java.sql.DriverManager

import com.digitalasset.testing.postgresql.PostgresAroundAll

class PostgresqlSqlLedgerReaderWriterIntegrationSpec
    extends SqlLedgerReaderWriterIntegrationSpecBase("SQL implementation using PostgreSQL")
    with PostgresAroundAll {

  override protected def jdbcUrl: String = postgresFixture.jdbcUrl

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    val connection = DriverManager.getConnection(postgresFixture.jdbcUrl)
    try {
      connection.prepareStatement("TRUNCATE log RESTART IDENTITY").execute()
      connection.prepareStatement("TRUNCATE state RESTART IDENTITY").execute()
      ()
    } finally {
      connection.close()
    }
  }
}
