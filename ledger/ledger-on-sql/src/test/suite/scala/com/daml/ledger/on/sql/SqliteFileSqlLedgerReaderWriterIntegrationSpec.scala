// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql

import java.nio.file.Files

class SqliteFileSqlLedgerReaderWriterIntegrationSpec
    extends SqlLedgerReaderWriterIntegrationSpecBase("SQL implementation using SQLite with a file") {

  private val root = Files.createTempDirectory(getClass.getSimpleName)

  override protected def jdbcUrl(id: String): String =
    s"jdbc:sqlite:$root/$id.sqlite"
}
