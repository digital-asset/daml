// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.api.domain.{LedgerId, ParticipantId}
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.lf.data.Ref
import com.daml.lf.engine.Engine
import com.daml.logging.LoggingContext
import com.daml.logging.LoggingContext.newLoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.configuration.{
  AcsStreamsConfig,
  ServerRole,
  TransactionFlatStreamsConfig,
  TransactionTreeStreamsConfig,
}
import com.daml.platform.store.DbSupport.{ConnectionPoolConfig, DbConfig}
import com.daml.platform.store.backend.StorageBackendFactory
import com.daml.platform.store.cache.MutableLedgerEndCache
import com.daml.platform.store.dao.JdbcLedgerDaoBackend.{TestLedgerId, TestParticipantId}
import com.daml.platform.store.dao.events.CompressionStrategy
import com.daml.platform.store.interning.StringInterningView
import com.daml.platform.store.{DbSupport, DbType}
import io.opentelemetry.api.GlobalOpenTelemetry
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
      acsIdPageSize: Int,
      acsIdFetchingParallelism: Int,
      acsContractFetchingParallelism: Int,
  )(implicit
      loggingContext: LoggingContext
  ): ResourceOwner[LedgerDao] = {
    val metrics = Metrics(new MetricRegistry, GlobalOpenTelemetry.getMeter("test"))
    val dbType = DbType.jdbcType(jdbcUrl)
    val storageBackendFactory = StorageBackendFactory.of(dbType)
    DbSupport
      .migratedOwner(
        serverRole = ServerRole.Testing(getClass),
        metrics = metrics,
        dbConfig = DbConfig(
          jdbcUrl,
          connectionPool = ConnectionPoolConfig(
            connectionPoolSize = dbType.maxSupportedWriteConnections(16),
            connectionTimeout = 250.millis,
          ),
        ),
      )
      .map { dbSupport =>
        JdbcLedgerDao.write(
          dbSupport = dbSupport,
          sequentialWriteDao = SequentialWriteDao(
            participantId = JdbcLedgerDaoBackend.TestParticipantIdRef,
            metrics = metrics,
            compressionStrategy = CompressionStrategy.none(metrics),
            ledgerEndCache = ledgerEndCache,
            stringInterningView = stringInterningView,
            ingestionStorageBackend = storageBackendFactory.createIngestionStorageBackend,
            parameterStorageBackend = storageBackendFactory.createParameterStorageBackend,
          ),
          eventsProcessingParallelism = eventsProcessingParallelism,
          servicesExecutionContext = executionContext,
          metrics = metrics,
          engine = Some(new Engine()),
          participantId = JdbcLedgerDaoBackend.TestParticipantIdRef,
          ledgerEndCache = ledgerEndCache,
          stringInterning = stringInterningView,
          completionsPageSize = 1000,
          acsStreamsConfig = AcsStreamsConfig(
            maxPayloadsPerPayloadsPage = eventsPageSize,
            maxIdsPerIdPage = acsIdPageSize,
            maxPagesPerIdPagesBuffer = 1,
            maxWorkingMemoryInBytesForIdPages = 100 * 1024 * 1024,
            maxParallelIdCreateQueries = acsIdFetchingParallelism,
            maxParallelPayloadCreateQueries = acsContractFetchingParallelism,
          ),
          transactionFlatStreamsConfig = TransactionFlatStreamsConfig.default,
          transactionTreeStreamsConfig = TransactionTreeStreamsConfig.default,
          globalMaxEventIdQueries = 20,
          globalMaxEventPayloadQueries = 10,
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
    stringInterningView = new StringInterningView()
    resource = newLoggingContext { implicit loggingContext =>
      for {
        dao <- daoOwner(
          eventsPageSize = 4,
          eventsProcessingParallelism = 4,
          acsIdPageSize = 4,
          acsIdFetchingParallelism = 2,
          acsContractFetchingParallelism = 2,
        ).acquire()
        _ <- Resource.fromFuture(dao.initialize(TestLedgerId, TestParticipantId))
        initialLedgerEnd <- Resource.fromFuture(dao.lookupLedgerEnd())
        _ = ledgerEndCache.set(initialLedgerEnd.lastOffset -> initialLedgerEnd.lastEventSeqId)
      } yield dao
    }
    ledgerDao = Await.result(resource.asFuture, 180.seconds)
  }

  override protected def afterAll(): Unit = {
    Await.result(resource.release(), 10.seconds)
    super.afterAll()
  }
}
