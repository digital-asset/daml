// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.migration.postgres

import com.daml.ledger.resources.TestResourceContext
import com.daml.logging.LoggingContext
import com.daml.platform.store.FlywayMigrations
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Future

class PostgresRemovalOfJavaMigrations
    extends AsyncFlatSpec
    with Matchers
    with TestResourceContext
    with PostgresConnectionSupport {
  import com.daml.platform.store.migration.MigrationTestSupport._

  implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  behavior of "Flyway migrations after the removal of Java migrations"

  it should "migrate an empty database to the latest schema" in {
    val migration =
      new FlywayMigrations(postgresDatabase.url)
    for {
      _ <- migration.migrate()
    } yield {
      succeed
    }
  }

  // Last version before the last Java migration
  it should "fail to migration from V37 to the latest schema" in {
    val migration =
      new FlywayMigrations(postgresDatabase.url)
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
      new FlywayMigrations(postgresDatabase.url)
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
      new FlywayMigrations(postgresDatabase.url)
    for {
      _ <- Future(migrateTo("39"))
      _ <- migration.migrate()
    } yield {
      succeed
    }
  }
}
