// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql

import com.digitalasset.testing.postgresql.PostgresAroundAll

class PostgresqlSqlLedgerReaderWriterIntegrationSpec
    extends SqlLedgerReaderWriterIntegrationSpecBase("SQL implementation using PostgreSQL")
    with PostgresAroundAll {

  override protected def newJdbcUrl(): String =
    createNewDatabase().jdbcUrl
}
