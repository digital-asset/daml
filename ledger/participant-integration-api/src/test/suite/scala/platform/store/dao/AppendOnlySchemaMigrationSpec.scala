// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.resources.{Resource, ResourceContext}
import com.daml.logging.LoggingContext.newLoggingContext
import com.daml.platform.store.FlywayMigrations
import com.daml.testing.postgresql.PostgresResource
import org.scalatest.wordspec.AsyncWordSpec

class AppendOnlySchemaMigrationSpec extends AsyncWordSpec with AkkaBeforeAndAfterAll {
  "Postgres flyway migration" should {

    "refuse to migrate from the append-only schema to the mutating schema" in {
      implicit val resourceContext: ResourceContext = ResourceContext(system.dispatcher)
      val resource = newLoggingContext { implicit loggingContext =>
        for {
          // Create an empty database
          db <- PostgresResource.owner[ResourceContext]().acquire()

          // Migrate to the append-only schema
          _ <- Resource.fromFuture(
            new FlywayMigrations(db.url, enableAppendOnlySchema = true).migrate()
          )

          // Attempt to migrate to the (older) mutating schema - this must fail
          error <- Resource.fromFuture(
            new FlywayMigrations(db.url, enableAppendOnlySchema = false).migrate().failed
          )
        } yield error
      }

      for {
        error <- resource.asFuture
        _ <- resource.release()
      } yield {
        // If started without the append-only flag, flyway should complain that the database
        // contains an unknown migration.
        assert(error.getMessage.contains("applied migration not resolved locally"))
      }
    }

  }
}
