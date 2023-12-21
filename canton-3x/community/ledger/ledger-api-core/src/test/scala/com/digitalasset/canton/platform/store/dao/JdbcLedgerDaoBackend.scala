// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.api.testing.utils.PekkoBeforeAndAfterAll
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.lf.VersionRange
import com.daml.lf.data.Ref
import com.daml.lf.engine.{Engine, EngineConfig}
import com.daml.lf.language.LanguageVersion
import com.daml.metrics.api.dropwizard.DropwizardMetricsFactory
import com.daml.resources.PureResource
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.ledger.api.domain.{LedgerId, ParticipantId}
import com.digitalasset.canton.logging.LoggingContextWithTrace.withNewLoggingContext
import com.digitalasset.canton.logging.SuppressingLogger
import com.digitalasset.canton.metrics.MetricHandle.NoOpMetricsFactory
import com.digitalasset.canton.metrics.Metrics
import com.digitalasset.canton.platform.config.{
  ActiveContractsServiceStreamsConfig,
  ServerRole,
  TransactionFlatStreamsConfig,
  TransactionTreeStreamsConfig,
}
import com.digitalasset.canton.platform.store.DbSupport.{ConnectionPoolConfig, DbConfig}
import com.digitalasset.canton.platform.store.backend.StorageBackendFactory
import com.digitalasset.canton.platform.store.cache.MutableLedgerEndCache
import com.digitalasset.canton.platform.store.dao.JdbcLedgerDaoBackend.{
  TestLedgerId,
  TestParticipantId,
}
import com.digitalasset.canton.platform.store.dao.events.{CompressionStrategy, ContractLoader}
import com.digitalasset.canton.platform.store.interning.StringInterningView
import com.digitalasset.canton.platform.store.{DbSupport, DbType, FlywayMigrations}
import com.digitalasset.canton.tracing.TraceContext
import io.opentelemetry.api.OpenTelemetry
import org.scalatest.Suite

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext}

object JdbcLedgerDaoBackend {

  private val TestLedgerId: LedgerId =
    LedgerId("test-ledger")

  private val TestParticipantIdRef =
    Ref.ParticipantId.assertFromString("test-participant")

  private val TestParticipantId: ParticipantId =
    ParticipantId(TestParticipantIdRef)

}

private[dao] trait JdbcLedgerDaoBackend extends PekkoBeforeAndAfterAll with BaseTest {
  self: Suite =>

  // AsyncFlatSpec is with serial execution context
  private implicit val ec: ExecutionContext = system.dispatcher

  protected def dbType: DbType

  protected def jdbcUrl: String

  protected def daoOwner(
      eventsPageSize: Int,
      eventsProcessingParallelism: Int,
      acsIdPageSize: Int,
      acsIdFetchingParallelism: Int,
      acsContractFetchingParallelism: Int,
  ): ResourceOwner[LedgerDao] = {
    val loggerFactory: SuppressingLogger = SuppressingLogger(getClass)
    implicit val traceContext: TraceContext = TraceContext.empty
    val metrics = {
      val registry = new MetricRegistry
      new Metrics(
        new DropwizardMetricsFactory(registry),
        NoOpMetricsFactory,
        registry,
        reportExecutionContextMetrics = true,
      )
    }
    val dbType = DbType.jdbcType(jdbcUrl)
    val storageBackendFactory = StorageBackendFactory.of(dbType, loggerFactory)
    val dbConfig = DbConfig(
      jdbcUrl,
      connectionPool = ConnectionPoolConfig(
        connectionPoolSize = 16,
        connectionTimeout = 250.millis,
      ),
    )
    for {
      _ <- new ResourceOwner[Unit] {
        override def acquire()(implicit context: ResourceContext): Resource[Unit] =
          PureResource(
            new FlywayMigrations(dbConfig.jdbcUrl, loggerFactory = loggerFactory).migrate()
          )
      }
      dbSupport <- DbSupport.owner(
        serverRole = ServerRole.Testing(getClass),
        metrics = metrics,
        dbConfig = dbConfig,
        loggerFactory = loggerFactory,
      )
      contractLoader <- ContractLoader.create(
        contractStorageBackend = dbSupport.storageBackendFactory.createContractStorageBackend(
          ledgerEndCache,
          stringInterningView,
        ),
        dbDispatcher = dbSupport.dbDispatcher,
        metrics = metrics,
        // not making these configuration is only needed in canton. here we populating with sensible defaults
        maxQueueSize = 10000,
        maxBatchSize = 50,
        parallelism = 5,
        multiDomainEnabled = false,
        loggerFactory = loggerFactory,
      )
    } yield JdbcLedgerDao.write(
      dbSupport = dbSupport,
      sequentialWriteDao = SequentialWriteDao(
        participantId = JdbcLedgerDaoBackend.TestParticipantIdRef,
        metrics = metrics,
        compressionStrategy = CompressionStrategy.none(metrics),
        ledgerEndCache = ledgerEndCache,
        stringInterningView = stringInterningView,
        ingestionStorageBackend = storageBackendFactory.createIngestionStorageBackend,
        parameterStorageBackend = storageBackendFactory.createParameterStorageBackend,
        loggerFactory = loggerFactory,
      ),
      servicesExecutionContext = ec,
      metrics = metrics,
      // TODO(#14706): revert to new Engine() once the default engine config supports only 2.x
      engine = Some(
        new Engine(
          EngineConfig(allowedLanguageVersions =
            VersionRange(LanguageVersion.v2_1, LanguageVersion.v2_1)
          )
        )
      ),
      participantId = JdbcLedgerDaoBackend.TestParticipantIdRef,
      ledgerEndCache = ledgerEndCache,
      stringInterning = stringInterningView,
      completionsPageSize = 1000,
      activeContractsServiceStreamsConfig = ActiveContractsServiceStreamsConfig(
        maxPayloadsPerPayloadsPage = eventsPageSize,
        maxIdsPerIdPage = acsIdPageSize,
        maxPagesPerIdPagesBuffer = 1,
        maxWorkingMemoryInBytesForIdPages = 100 * 1024 * 1024,
        maxParallelIdCreateQueries = acsIdFetchingParallelism,
        maxParallelPayloadCreateQueries = acsContractFetchingParallelism,
        contractProcessingParallelism = eventsProcessingParallelism,
      ),
      transactionFlatStreamsConfig = TransactionFlatStreamsConfig.default,
      transactionTreeStreamsConfig = TransactionTreeStreamsConfig.default,
      globalMaxEventIdQueries = 20,
      globalMaxEventPayloadQueries = 10,
      tracer = OpenTelemetry.noop().getTracer("test"),
      loggerFactory = loggerFactory,
      contractLoader = contractLoader,
    )
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
    stringInterningView = new StringInterningView(loggerFactory)
    resource = withNewLoggingContext() { implicit loggingContext =>
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
    }(TraceContext.empty)
    ledgerDao = Await.result(resource.asFuture, 180.seconds)
  }

  override protected def afterAll(): Unit = {
    Await.result(resource.release(), 10.seconds)
    super.afterAll()
  }
}
