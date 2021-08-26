// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql.queries

import com.daml.ledger.on.sql.Database
import org.scalatest.flatspec.AsyncFlatSpec

class H2QueriesSpec extends AsyncFlatSpec with QueryBehaviors {
  private val rdbms = Database.RDBMS.H2
  private val jdbcUrl = s"jdbc:h2:mem:${getClass.getSimpleName}"

  it should behave like queriesOnInsertion(rdbms, jdbcUrl)
}
