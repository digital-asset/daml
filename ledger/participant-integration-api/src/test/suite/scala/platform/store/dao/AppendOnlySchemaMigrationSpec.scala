// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.api.domain.{LedgerId, ParticipantId}
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.lf.data.Ref
import com.daml.lf.engine.{Engine, ValueEnricher}
import com.daml.logging.LoggingContext
import com.daml.logging.LoggingContext.newLoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.configuration.ServerRole
import com.daml.platform.store.{DbType, FlywayMigrations, LfValueTranslationCache}
import com.daml.testing.postgresql.PostgresResource
import org.scalatest.wordspec.AsyncWordSpec

class AppendOnlySchemaMigrationSpec extends AsyncWordSpec with AkkaBeforeAndAfterAll {
  "Postgres flyway migration" should {

    // TODO append-only: remove this test after implementing data migration
    "refuse to migrate to append-only schema if the database is not empty" in {
      implicit val resourceContext: ResourceContext = ResourceContext(system.dispatcher)
      val resource = newLoggingContext { implicit loggingContext =>
        for {
          // Create an empty database
          db <- PostgresResource.owner[ResourceContext]().acquire()

          // Migrate to the latest stable schema
          _ <- Resource.fromFuture(
            new FlywayMigrations(db.url).migrate(enableAppendOnlySchema = false)
          )

          // Write some data into the database
          dao <- daoOwner(100, db.url).acquire()
          _ <- Resource.fromFuture(dao.initializeLedger(TestLedgerId))
          _ <- Resource.fromFuture(dao.initializeParticipantId(TestParticipantId))

          // Attempt to migrate the non-empty database to the append-only schema - this must fail
          error <- Resource.fromFuture(
            new FlywayMigrations(db.url).migrate(enableAppendOnlySchema = true).failed
          )
        } yield error
      }

      for {
        error <- resource.asFuture
        _ <- resource.release()
      } yield {
        // safety_check is the name of a table
        // An insert is attempted into that table which fails if the database is not empty
        assert(error.getMessage.contains("safety_check"))
      }
    }

  }

  private[this] val TestLedgerId: LedgerId =
    LedgerId("test-ledger")

  private[this] val TestParticipantId: ParticipantId =
    ParticipantId(Ref.ParticipantId.assertFromString("test-participant"))

  private[this] def daoOwner(
      eventsPageSize: Int,
      jdbcUrl: String,
  )(implicit
      loggingContext: LoggingContext
  ): ResourceOwner[LedgerDao] =
    JdbcLedgerDao.writeOwner(
      serverRole = ServerRole.Testing(getClass),
      jdbcUrl = jdbcUrl,
      connectionPoolSize = 16,
      eventsPageSize = eventsPageSize,
      servicesExecutionContext = executionContext,
      metrics = new Metrics(new MetricRegistry),
      lfValueTranslationCache = LfValueTranslationCache.Cache.none,
      jdbcAsyncCommitMode = DbType.AsynchronousCommit,
      enricher = Some(new ValueEnricher(new Engine())),
      inMemoryCompletionsCache = false,
    )
}
