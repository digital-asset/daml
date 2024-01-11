// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.migration.postgres

import com.daml.ledger.resources.TestResourceContext
import com.digitalasset.canton.TestEssentials
import com.digitalasset.canton.platform.store.FlywayMigrations
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Future

class RemovalOfJavaMigrationsPostgres
    extends AsyncFlatSpec
    with Matchers
    with TestResourceContext
    with PostgresAroundEachForMigrations
    with TestEssentials {
  import com.digitalasset.canton.platform.store.migration.MigrationTestSupport.*

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

  // Last version before the last Java migration
  it should "fail to migration from V37 to the latest schema" in {
    val migration =
      new FlywayMigrations(postgresDatabase.url, loggerFactory = loggerFactory)
    for {
      _ <- Future(migrateTo("37"))
      err <- migration.migrate().failed
    } yield {
      err shouldBe a[FlywayMigrations.SchemaVersionIsTooOld]
    }
  }

  // Version of the last Java migration
  it should "migrate from V38 to the latest schema" in {
    val migration =
      new FlywayMigrations(postgresDatabase.url, loggerFactory = loggerFactory)
    for {
      _ <- Future(migrateTo("38"))
      _ <- migration.migrate()
    } yield {
      succeed
    }
  }

  // First version after the last Java migration
  it should "migrate from V39 to the latest schema" in {
    val migration =
      new FlywayMigrations(postgresDatabase.url, loggerFactory = loggerFactory)
    for {
      _ <- Future(migrateTo("39"))
      _ <- migration.migrate()
    } yield {
      succeed
    }
  }
}
