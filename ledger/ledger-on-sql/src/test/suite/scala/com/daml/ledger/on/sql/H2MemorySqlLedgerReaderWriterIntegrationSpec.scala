// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql

class H2MemorySqlLedgerReaderWriterIntegrationSpec
    extends SqlLedgerReaderWriterIntegrationSpecBase("SQL implementation using H2 in memory") {

  override protected val isPersistent: Boolean = false

  override protected def jdbcUrl(id: String): String =
    s"jdbc:h2:mem:$id"
}
