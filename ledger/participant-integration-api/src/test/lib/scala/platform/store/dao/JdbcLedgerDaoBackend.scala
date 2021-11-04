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
import com.daml.platform.server.api.validation.ErrorFactories
import com.daml.platform.store.appendonlydao.LedgerDao
import com.daml.platform.store.cache.MutableLedgerEndCache
import com.daml.platform.store.dao.JdbcLedgerDaoBackend.{TestLedgerId, TestParticipantId}
import com.daml.platform.store.{DbType, FlywayMigrations, LfValueTranslationCache}
import org.mockito.MockitoSugar
import org.scalatest.AsyncTestSuite

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

object JdbcLedgerDaoBackend {

  private val TestLedgerId: LedgerId =
    LedgerId("test-ledger")

  private val TestParticipantIdRef =
    Ref.ParticipantId.assertFromString("test-participant")

  private val TestParticipantId: ParticipantId =
    ParticipantId(TestParticipantIdRef)

}

private[dao] trait JdbcLedgerDaoBackend extends AkkaBeforeAndAfterAll {
  this: AsyncTestSuite =>

  protected def dbType: DbType

  protected def jdbcUrl: String

  protected def daoOwner(
      eventsPageSize: Int,
      eventsProcessingParallelism: Int,
      errorFactories: ErrorFactories,
  )(implicit
      loggingContext: LoggingContext
  ): ResourceOwner[LedgerDao] = {
    com.daml.platform.store.appendonlydao.JdbcLedgerDao.writeOwner(
      serverRole = ServerRole.Testing(getClass),
      jdbcUrl = jdbcUrl,
      // this was the previous default.
      // keeping it hardcoded here to keep tests working as before extracting the parameter
      connectionPoolSize = 16,
      connectionTimeout = 250.millis,
      eventsPageSize = eventsPageSize,
      eventsProcessingParallelism = eventsProcessingParallelism,
      servicesExecutionContext = executionContext,
      metrics = new Metrics(new MetricRegistry),
      lfValueTranslationCache = LfValueTranslationCache.Cache.none,
      enricher = Some(new ValueEnricher(new Engine())),
      participantId = JdbcLedgerDaoBackend.TestParticipantIdRef,
      ledgerEndCache = ledgerEndCache,
      errorFactories = errorFactories,
    )
  }

  protected final var ledgerDao: LedgerDao = _
  protected var ledgerEndCache: MutableLedgerEndCache = _

  // `dbDispatcher` and `ledgerDao` depend on the `postgresFixture` which is in turn initialized `beforeAll`
  private var resource: Resource[LedgerDao] = _
  private val errorFactories_mock = MockitoSugar.mock[ErrorFactories]

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    // We use the dispatcher here because the default Scalatest execution context is too slow.
    implicit val resourceContext: ResourceContext = ResourceContext(system.dispatcher)
    ledgerEndCache = MutableLedgerEndCache()
    resource = newLoggingContext { implicit loggingContext =>
      for {
        _ <- Resource.fromFuture(
          new FlywayMigrations(jdbcUrl).migrate()
        )
        dao <- daoOwner(100, 4, errorFactories_mock).acquire()
        _ <- Resource.fromFuture(dao.initialize(TestLedgerId, TestParticipantId))
        initialLedgerEnd <- Resource.fromFuture(dao.lookupLedgerEndOffsetAndSequentialId())
        _ = ledgerEndCache.set(initialLedgerEnd)
      } yield dao
    }
    ledgerDao = Await.result(resource.asFuture, 30.seconds)
  }

  override protected def afterAll(): Unit = {
    Await.result(resource.release(), 10.seconds)
    MockitoSugar.verifyZeroInteractions(errorFactories_mock)
    super.afterAll()
  }
}
