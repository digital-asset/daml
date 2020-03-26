// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql

import java.nio.file.Files

class H2FileSqlLedgerReaderWriterIntegrationSpec
    extends SqlLedgerReaderWriterIntegrationSpecBase("SQL implementation using H2 with a file") {

  private val root = Files.createTempDirectory(getClass.getSimpleName)

  override protected def jdbcUrl(id: String): String =
    s"jdbc:h2:file:$root/$id"
}
