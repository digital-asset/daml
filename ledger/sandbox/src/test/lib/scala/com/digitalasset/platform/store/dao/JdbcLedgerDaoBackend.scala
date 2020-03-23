// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.store.dao

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.participant.state.v1.Offset
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.digitalasset.logging.LoggingContext.newLoggingContext
import com.digitalasset.platform.configuration.ServerRole
import com.digitalasset.platform.store.{DbType, FlywayMigrations}
import com.digitalasset.resources.Resource
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
          .writeOwner(ServerRole.Testing(getClass), jdbcUrl, new MetricRegistry)
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
