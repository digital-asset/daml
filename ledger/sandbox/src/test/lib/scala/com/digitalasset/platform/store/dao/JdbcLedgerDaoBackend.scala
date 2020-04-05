// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.participant.state.v1.Offset
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.logging.LoggingContext.newLoggingContext
import com.daml.platform.configuration.ServerRole
import com.daml.platform.store.{DbType, FlywayMigrations}
import com.daml.resources.Resource
import org.scalatest.Suite

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext}

private[dao] trait JdbcLedgerDaoBackend extends AkkaBeforeAndAfterAll { this: Suite =>

  protected def dbType: DbType
  protected def jdbcUrl: String

  protected final var ledgerDao: LedgerDao = _

  // `dbDispatcher` and `ledgerDao` depend on the `postgresFixture` which is in turn initialized `beforeAll`
  private var resource: Resource[LedgerDao] = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    implicit val executionContext: ExecutionContext = system.dispatcher
    resource = newLoggingContext { implicit logCtx =>
      for {
        _ <- Resource.fromFuture(new FlywayMigrations(jdbcUrl).migrate())
        dao <- JdbcLedgerDao
          .writeOwner(
            serverRole = ServerRole.Testing(getClass),
            jdbcUrl = jdbcUrl,
            eventsPageSize = 100,
            metrics = new MetricRegistry,
          )
          .acquire()
        _ <- Resource.fromFuture(dao.initializeLedger(LedgerId("test-ledger"), Offset.begin))
      } yield dao
    }
    ledgerDao = Await.result(resource.asFuture, 10.seconds)
  }

  override protected def afterAll(): Unit = {
    Await.result(resource.release(), 10.seconds)
    super.afterAll()
  }

}
