// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql

import scala.util.Random

class SqliteMemorySqlLedgerReaderWriterIntegrationSpec
    extends SqlLedgerReaderWriterIntegrationSpecBase("SQL implementation using SQLite in memory") {

  override protected def newJdbcUrl() =
    s"jdbc:sqlite:file:${getClass.getSimpleName.toLowerCase()}_${Random.nextInt()}?mode=memory&cache=shared"
}
