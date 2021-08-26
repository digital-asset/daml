// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql.queries

import com.daml.ledger.on.sql.Database
import com.daml.testing.postgresql.PostgresAroundAll
import org.scalatest.flatspec.AsyncFlatSpec

class PostgresqlQueriesSpec extends AsyncFlatSpec with QueryBehaviors with PostgresAroundAll {
  private val rdbms = Database.RDBMS.PostgreSQL

  it should behave like queriesOnInsertion(rdbms, postgresDatabase.url)
}
