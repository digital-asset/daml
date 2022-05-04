// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import com.daml.platform.store.dao.{JdbcLedgerDao, LedgerDao, SequentialWriteDao}
import com.daml.platform.store.dao.events.CompressionStrategy
import com.daml.platform.store.backend.StorageBackendFactory
import com.daml.platform.store.cache.MutableLedgerEndCache
import com.daml.platform.store.dao.JdbcLedgerDaoBackend.{TestLedgerId, TestParticipantId}
import com.daml.platform.store.interning.StringInterningView
import com.daml.platform.store.{DbSupport, DbType, LfValueTranslationCache}
import org.scalatest.AsyncTestSuite

import scala.concurrent.{Await, Future}
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
      acsIdPageSize: Int,
      acsIdFetchingParallelism: Int,
      acsContractFetchingParallelism: Int,
      acsGlobalParallelism: Int,
  )(implicit
      loggingContext: LoggingContext
  ): ResourceOwner[LedgerDao] = {
    val metrics = new Metrics(new MetricRegistry)
    val dbType = DbType.jdbcType(jdbcUrl)
    val storageBackendFactory = StorageBackendFactory.of(dbType)
    DbSupport
      .migratedOwner(
        jdbcUrl = jdbcUrl,
        serverRole = ServerRole.Testing(getClass),
        connectionPoolSize = dbType.maxSupportedWriteConnections(16),
        connectionTimeout = 250.millis,
        metrics = metrics,
      )
      .map { dbSupport =>
        JdbcLedgerDao.write(
          dbSupport = dbSupport,
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
          acsIdPageSize = acsIdPageSize,
          acsIdFetchingParallelism = acsIdFetchingParallelism,
          acsContractFetchingParallelism = acsContractFetchingParallelism,
          acsGlobalParallelism = acsGlobalParallelism,
          servicesExecutionContext = executionContext,
          metrics = metrics,
          lfValueTranslationCache = LfValueTranslationCache.Cache.none,
          enricher = Some(new ValueEnricher(new Engine())),
          participantId = JdbcLedgerDaoBackend.TestParticipantIdRef,
          ledgerEndCache = ledgerEndCache,
          stringInterning = stringInterningView,
          materializer = materializer,
        )
      }
  }

  protected final var ledgerDao: LedgerDao = _
  protected var ledgerEndCache: MutableLedgerEndCache = _
  protected var stringInterningView: StringInterningView = _

  // `dbDispatcher` and `ledgerDao` depend on the `postgresFixture` which is in turn initialized `beforeAll`
  private var resource: Resource[LedgerDao] = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    // We use the dispatcher here because the default Scalatest execution context is too slow.
    implicit val resourceContext: ResourceContext = ResourceContext(system.dispatcher)
    ledgerEndCache = MutableLedgerEndCache()
    stringInterningView = new StringInterningView((_, _) => _ => Future.successful(Nil))
    resource = newLoggingContext { implicit loggingContext =>
      for {
        dao <- daoOwner(
          eventsPageSize = 4,
          eventsProcessingParallelism = 4,
          acsIdPageSize = 4,
          acsIdFetchingParallelism = 2,
          acsContractFetchingParallelism = 2,
          acsGlobalParallelism = 10,
        ).acquire()
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
