// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql

import scala.util.Random

class H2MemorySqlLedgerReaderWriterIntegrationSpec
    extends SqlLedgerReaderWriterIntegrationSpecBase("SQL implementation using H2 in memory") {

  override protected def newJdbcUrl(): String =
    s"jdbc:h2:mem:${getClass.getSimpleName.toLowerCase()}_${Random.nextInt()}"
}
