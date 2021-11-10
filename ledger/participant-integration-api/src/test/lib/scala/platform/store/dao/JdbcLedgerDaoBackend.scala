// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import com.codahale.metrics.MetricRegistry
import com.daml.error.ErrorCodesVersionSwitcher
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
import com.daml.platform.store.appendonlydao.{
  DbDispatcher,
  JdbcLedgerDao,
  LedgerDao,
  SequentialWriteDao,
}
import com.daml.platform.store.appendonlydao.events.CompressionStrategy
import com.daml.platform.store.backend.StorageBackendFactory
import com.daml.platform.store.cache.MutableLedgerEndCache
import com.daml.platform.store.dao.JdbcLedgerDaoBackend.{TestLedgerId, TestParticipantId}
import com.daml.platform.store.interning.StringInterningView
import com.daml.platform.store.{DbType, FlywayMigrations, LfValueTranslationCache}
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
    val metrics = new Metrics(new MetricRegistry)
    val dbType = DbType.jdbcType(jdbcUrl)
    val storageBackendFactory = StorageBackendFactory.of(dbType)
    DbDispatcher
      .owner(
        dataSource = storageBackendFactory.createDataSourceStorageBackend.createDataSource(jdbcUrl),
        serverRole = ServerRole.Testing(getClass),
        connectionPoolSize = dbType.maxSupportedWriteConnections(16),
        connectionTimeout = 250.millis,
        metrics = metrics,
      )
      .map { dbDispatcher =>
        val stringInterningStorageBackend =
          storageBackendFactory.createStringInterningStorageBackend
        val stringInterningView = new StringInterningView(
          loadPrefixedEntries = (fromExclusive, toInclusive) =>
            implicit loggingContext =>
              dbDispatcher.executeSql(metrics.daml.index.db.loadStringInterningEntries) {
                stringInterningStorageBackend.loadStringInterningEntries(fromExclusive, toInclusive)
              }
        )
        JdbcLedgerDao.write(
          dbDispatcher = dbDispatcher,
          sequentialWriteDao = SequentialWriteDao(
            participantId = JdbcLedgerDaoBackend.TestParticipantIdRef,
            lfValueTranslationCache = LfValueTranslationCache.Cache.none,
            metrics = metrics,
            compressionStrategy = CompressionStrategy.none(metrics),
            ledgerEndCache = ledgerEndCache,
            stringInterningView = stringInterningView,
            ingestionStorageBackend = storageBackendFactory.createIngestionStorageBackend,
            parameterStorageBackend = storageBackendFactory.createParameterStorageBackend,
          ),
          eventsPageSize = eventsPageSize,
          eventsProcessingParallelism = eventsProcessingParallelism,
          servicesExecutionContext = executionContext,
          metrics = metrics,
          lfValueTranslationCache = LfValueTranslationCache.Cache.none,
          enricher = Some(new ValueEnricher(new Engine())),
          participantId = JdbcLedgerDaoBackend.TestParticipantIdRef,
          storageBackendFactory = storageBackendFactory,
          ledgerEndCache = ledgerEndCache,
          errorFactories = errorFactories,
        )
      }
  }

  protected final var ledgerDao: LedgerDao = _
  protected var ledgerEndCache: MutableLedgerEndCache = _

  // `dbDispatcher` and `ledgerDao` depend on the `postgresFixture` which is in turn initialized `beforeAll`
  private var resource: Resource[LedgerDao] = _
  private val errorFactories = ErrorFactories(new ErrorCodesVersionSwitcher(false))

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
        dao <- daoOwner(100, 4, errorFactories).acquire()
        _ <- Resource.fromFuture(dao.initialize(TestLedgerId, TestParticipantId))
        initialLedgerEnd <- Resource.fromFuture(dao.lookupLedgerEnd())
        _ = ledgerEndCache.set(initialLedgerEnd.lastOffset -> initialLedgerEnd.lastEventSeqId)
      } yield dao
    }
    ledgerDao = Await.result(resource.asFuture, 30.seconds)
  }

  override protected def afterAll(): Unit = {
    Await.result(resource.release(), 10.seconds)
    super.afterAll()
  }
}
