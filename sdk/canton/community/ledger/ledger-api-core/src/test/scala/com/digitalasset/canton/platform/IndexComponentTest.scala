// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform

import com.daml.ledger.api.testing.utils.PekkoBeforeAndAfterAll
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.participant.state.Update
import com.digitalasset.canton.ledger.participant.state.index.IndexService
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.IndexComponentTest.TestServices
import com.digitalasset.canton.platform.apiserver.execution.CommandProgressTracker
import com.digitalasset.canton.platform.config.{IndexServiceConfig, ServerRole}
import com.digitalasset.canton.platform.index.IndexServiceOwner
import com.digitalasset.canton.platform.indexer.ha.HaConfig
import com.digitalasset.canton.platform.indexer.parallel.NoOpReassignmentOffsetPersistence
import com.digitalasset.canton.platform.indexer.{IndexerConfig, JdbcIndexer}
import com.digitalasset.canton.platform.store.DbSupport.{ConnectionPoolConfig, DbConfig}
import com.digitalasset.canton.platform.store.cache.MutableLedgerEndCache
import com.digitalasset.canton.platform.store.dao.events.{ContractLoader, LfValueTranslation}
import com.digitalasset.canton.platform.store.interning.StringInterningView
import com.digitalasset.canton.platform.store.packagemeta.PackageMetadata
import com.digitalasset.canton.platform.store.{DbSupport, FlywayMigrations}
import com.digitalasset.canton.time.WallClock
import com.digitalasset.canton.tracing.NoReportingTracerProvider
import com.digitalasset.canton.util.PekkoUtil.{FutureQueue, IndexingFutureQueue}
import com.digitalasset.canton.{BaseTest, HasExecutorService}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.engine.{Engine, EngineConfig}
import com.digitalasset.daml.lf.language.{LanguageMajorVersion, LanguageVersion}
import com.digitalasset.daml.lf.transaction.test.{NodeIdTransactionBuilder, TestNodeBuilder}
import org.scalatest.Suite

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}

trait IndexComponentTest extends PekkoBeforeAndAfterAll with BaseTest with HasExecutorService {
  self: Suite =>

  private val clock = new WallClock(ProcessingTimeout(), loggerFactory)

  implicit val ec: ExecutionContext = system.dispatcher

  protected implicit val loggingContextWithTrace: LoggingContextWithTrace =
    LoggingContextWithTrace.ForTesting

  // if we would need multi-db, polimorphism can come here, look for JdbcLedgerDaoBackend
  private val jdbcUrl = s"jdbc:h2:mem:${getClass.getSimpleName.toLowerCase};db_close_delay=-1"

  private val testServicesRef: AtomicReference[TestServices] = new AtomicReference()

  private def testServices: TestServices =
    Option(testServicesRef.get())
      .getOrElse(throw new Exception("TestServices not initialized. Not accessing from a test?"))

  private def ledgerEndOffset = testServices.index.currentLedgerEnd().futureValue

  protected def ingestUpdates(updates: Update*): Offset = {
    val ledgerEndLongBefore = ledgerEndOffset.map(_.positive).getOrElse(0L)
    updates.foreach(update => testServices.indexer.offer(update).futureValue)
    val expectedOffset = Offset.tryFromLong(updates.size + ledgerEndLongBefore)
    eventually() {
      ledgerEndOffset shouldBe Some(expectedOffset)
      expectedOffset
    }
  }

  protected def index: IndexService = testServices.index

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    // We use the dispatcher here because the default Scalatest execution context is too slow.
    implicit val resourceContext: ResourceContext = ResourceContext(system.dispatcher)

    val indexerConfig = IndexerConfig()

    val engine = new Engine(
      EngineConfig(LanguageVersion.StableVersions(LanguageMajorVersion.V2))
    )
    val mutableLedgerEndCache = MutableLedgerEndCache()
    val stringInterningView = new StringInterningView(loggerFactory)
    val participantId = Ref.ParticipantId.assertFromString("index-component-test-participant-id")

    val indexResourceOwner =
      for {
        (inMemoryState, updaterFlow) <- LedgerApiServer.createInMemoryStateAndUpdater(
          participantId = participantId,
          commandProgressTracker = CommandProgressTracker.NoOp,
          indexServiceConfig = IndexServiceConfig(),
          maxCommandsInFlight = 1, // not used
          metrics = LedgerApiServerMetrics.ForTesting,
          executionContext = ec,
          tracer = NoReportingTracerProvider.tracer,
          loggerFactory = loggerFactory,
        )(mutableLedgerEndCache, stringInterningView)
        _ <- ResourceOwner.forFuture(() => new FlywayMigrations(jdbcUrl, loggerFactory).migrate())
        dbSupport <- DbSupport
          .owner(
            serverRole = ServerRole.ApiServer,
            metrics = LedgerApiServerMetrics.ForTesting,
            dbConfig = DbConfig(
              jdbcUrl = jdbcUrl,
              connectionPool = ConnectionPoolConfig(
                connectionPoolSize = 10,
                connectionTimeout = 250.millis,
              ),
            ),
            loggerFactory = loggerFactory,
          )
        indexerF <- new JdbcIndexer.Factory(
          participantId = participantId,
          participantDataSourceConfig = DbSupport.ParticipantDataSourceConfig(jdbcUrl),
          config = indexerConfig,
          excludedPackageIds = Set.empty,
          metrics = LedgerApiServerMetrics.ForTesting,
          inMemoryState = inMemoryState,
          apiUpdaterFlow = updaterFlow,
          executionContext = ec,
          tracer = NoReportingTracerProvider.tracer,
          loggerFactory = loggerFactory,
          dataSourceProperties = IndexerConfig.createDataSourcePropertiesForTesting(
            indexerConfig.ingestionParallelism.unwrap
          ),
          highAvailability = HaConfig(),
          indexSericeDbDispatcher = Some(dbSupport.dbDispatcher),
          clock = clock,
          reassignmentOffsetPersistence = NoOpReassignmentOffsetPersistence,
          postProcessor = (_, _) => Future.unit,
          sequentialPostProcessor = _ => (),
        ).initialized()
        indexerFutureQueueConsumer <- ResourceOwner.forFuture(() => indexerF(false)(_ => ()))
        indexer <- ResourceOwner.forReleasable(() =>
          new IndexingFutureQueue(indexerFutureQueueConsumer)
        ) { indexer =>
          indexer.shutdown()
          indexer.done.map(_ => ())
        }
        contractLoader <- ContractLoader.create(
          contractStorageBackend = dbSupport.storageBackendFactory.createContractStorageBackend(
            inMemoryState.stringInterningView
          ),
          dbDispatcher = dbSupport.dbDispatcher,
          metrics = LedgerApiServerMetrics.ForTesting,
          maxQueueSize = 10000,
          maxBatchSize = 50,
          parallelism = 5,
          loggerFactory = loggerFactory,
        )
        indexService <- new IndexServiceOwner(
          dbSupport = dbSupport,
          config = IndexServiceConfig(),
          experimentalEnableTopologyEvents = true,
          participantId = Ref.ParticipantId.assertFromString(IndexComponentTest.TestParticipantId),
          metrics = LedgerApiServerMetrics.ForTesting,
          inMemoryState = inMemoryState,
          tracer = NoReportingTracerProvider.tracer,
          loggerFactory = loggerFactory,
          incompleteOffsets = (_, _, _) => FutureUnlessShutdown.pure(Vector.empty),
          contractLoader = contractLoader,
          getPackageMetadataSnapshot = _ => PackageMetadata(),
          lfValueTranslation = new LfValueTranslation(
            metrics = LedgerApiServerMetrics.ForTesting,
            engineO = Some(engine),
            // Not used
            loadPackage = (_, _) => Future(None),
            loggerFactory = loggerFactory,
          ),
          queryExecutionContext = executorService,
          commandExecutionContext = executorService,
        )
      } yield indexService -> indexer

    val indexResource = indexResourceOwner.acquire()
    val (index, indexer) = indexResource.asFuture.futureValue

    testServicesRef.set(
      TestServices(
        indexResource = indexResource,
        index = index,
        indexer = indexer,
      )
    )
  }

  override def afterAll(): Unit = {
    testServices.indexResource.release().futureValue
    super.afterAll()
  }

  protected object TxBuilder {
    def apply(): NodeIdTransactionBuilder & TestNodeBuilder = new NodeIdTransactionBuilder
      with TestNodeBuilder
  }
}

object IndexComponentTest {

  val TestParticipantId = "index-component-test-participant-id"

  val maxUpdateCount = 1000000

  final case class TestServices(
      indexResource: Resource[Any],
      index: IndexService,
      indexer: FutureQueue[Update],
  )
}
