// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.migration.postgres

import com.digitalasset.canton.TestEssentials
import com.digitalasset.canton.ledger.resources.TestResourceContext
import com.digitalasset.canton.platform.store.FlywayMigrations
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

class RemovalOfJavaMigrationsPostgres
    extends AsyncFlatSpec
    with Matchers
    with TestResourceContext
    with PostgresAroundEachForMigrations
    with TestEssentials {

  behavior of "Flyway migrations after the removal of Java migrations"

  it should "migrate an empty database to the latest schema" in {
    val migration =
      new FlywayMigrations(postgresDatabase.url, loggerFactory = loggerFactory)
    for {
      _ <- migration.migrate()
    } yield {
      succeed
    }
  }
}
