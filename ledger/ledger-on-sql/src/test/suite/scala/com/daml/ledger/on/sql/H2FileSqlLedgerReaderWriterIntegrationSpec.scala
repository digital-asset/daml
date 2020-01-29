// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql

import java.nio.file.Files

class H2FileSqlLedgerReaderWriterIntegrationSpec
    extends SqlLedgerReaderWriterIntegrationSpecBase("SQL implementation using H2 with a file") {

  override protected def newJdbcUrl(): String =
    s"jdbc:h2:file:${Files.createTempDirectory(getClass.getSimpleName)}/test"
}
