// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.api.domain.{LedgerId, ParticipantId}
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext
import com.daml.logging.LoggingContext.newLoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.configuration.ServerRole
import com.daml.platform.store.dao.JdbcLedgerDaoBackend.{TestLedgerId, TestParticipantId}
import com.daml.platform.store.dao.events.LfValueTranslation
import com.daml.platform.store.{DbType, FlywayMigrations}
import org.scalatest.AsyncTestSuite

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

object JdbcLedgerDaoBackend {

  private val TestLedgerId: LedgerId =
    LedgerId("test-ledger")

  private val TestParticipantId: ParticipantId =
    ParticipantId(Ref.ParticipantId.assertFromString("test-participant"))

}

private[dao] trait JdbcLedgerDaoBackend extends AkkaBeforeAndAfterAll {
  this: AsyncTestSuite =>

  protected def dbType: DbType

  protected def jdbcUrl: String

  protected def daoOwner(eventsPageSize: Int)(
      implicit loggingContext: LoggingContext
  ): ResourceOwner[LedgerDao] =
    JdbcLedgerDao.writeOwner(
      serverRole = ServerRole.Testing(getClass),
      jdbcUrl = jdbcUrl,
      eventsPageSize = eventsPageSize,
      metrics = new Metrics(new MetricRegistry),
      lfValueTranslationCache = LfValueTranslation.Cache.none,
      jdbcAsyncCommits = true,
    )

  protected final var ledgerDao: LedgerDao = _

  // `dbDispatcher` and `ledgerDao` depend on the `postgresFixture` which is in turn initialized `beforeAll`
  private var resource: Resource[LedgerDao] = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    // We use the dispatcher here because the default Scalatest execution context is too slow.
    implicit val resourceContext: ResourceContext = ResourceContext(system.dispatcher)
    resource = newLoggingContext { implicit loggingContext =>
      for {
        _ <- Resource.fromFuture(new FlywayMigrations(jdbcUrl).migrate())
        dao <- daoOwner(100).acquire()
        _ <- Resource.fromFuture(dao.initializeLedger(TestLedgerId))
        _ <- Resource.fromFuture(dao.initializeParticipantId(TestParticipantId))
      } yield dao
    }
    ledgerDao = Await.result(resource.asFuture, 10.seconds)
  }

  override protected def afterAll(): Unit = {
    Await.result(resource.release(), 10.seconds)
    super.afterAll()
  }
}
