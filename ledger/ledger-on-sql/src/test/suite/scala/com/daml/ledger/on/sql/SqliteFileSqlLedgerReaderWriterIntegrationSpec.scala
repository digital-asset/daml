// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql

import java.nio.file.Files

class SqliteFileSqlLedgerReaderWriterIntegrationSpec
    extends SqlLedgerReaderWriterIntegrationSpecBase("SQL implementation using SQLite with a file") {

  override protected def newJdbcUrl(): String =
    s"jdbc:sqlite:${Files.createTempDirectory(getClass.getSimpleName)}/test.sqlite"
}
