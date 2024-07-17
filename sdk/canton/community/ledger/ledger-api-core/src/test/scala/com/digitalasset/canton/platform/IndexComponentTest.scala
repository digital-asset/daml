// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform

import com.daml.ledger.api.testing.utils.PekkoBeforeAndAfterAll
import com.daml.ledger.resources.{Resource, ResourceContext}
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.health.HealthStatus
import com.digitalasset.canton.ledger.participant.state.index.IndexService
import com.digitalasset.canton.ledger.participant.state.{
  InternalStateServiceProviderImpl,
  ReadService,
  Update,
}
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.IndexComponentTest.{TestReadService, TestServices}
import com.digitalasset.canton.platform.apiserver.execution.CommandProgressTracker
import com.digitalasset.canton.platform.config.{IndexServiceConfig, ServerRole}
import com.digitalasset.canton.platform.index.IndexServiceOwner
import com.digitalasset.canton.platform.indexer.ha.HaConfig
import com.digitalasset.canton.platform.indexer.parallel.NoOpReassignmentOffsetPersistence
import com.digitalasset.canton.platform.indexer.{
  IndexerConfig,
  IndexerServiceOwner,
  IndexerStartupMode,
}
import com.digitalasset.canton.platform.store.DbSupport
import com.digitalasset.canton.platform.store.DbSupport.{
  ConnectionPoolConfig,
  DbConfig,
  ParticipantDataSourceConfig,
}
import com.digitalasset.canton.platform.store.cache.MutableLedgerEndCache
import com.digitalasset.canton.platform.store.dao.events.{ContractLoader, LfValueTranslation}
import com.digitalasset.canton.platform.store.interning.StringInterningView
import com.digitalasset.canton.platform.store.packagemeta.PackageMetadata
import com.digitalasset.canton.time.WallClock
import com.digitalasset.canton.tracing.{NoReportingTracerProvider, TraceContext, Traced}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.engine.{Engine, EngineConfig}
import com.digitalasset.daml.lf.language.{LanguageMajorVersion, LanguageVersion}
import com.digitalasset.daml.lf.transaction.test.{NodeIdTransactionBuilder, TestNodeBuilder}
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.stream.{BoundedSourceQueue, Materializer, QueueOfferResult}
import org.scalatest.Suite

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future, blocking}

trait IndexComponentTest extends PekkoBeforeAndAfterAll with BaseTest {
  self: Suite =>

  private val clock = new WallClock(ProcessingTimeout(), loggerFactory)

  // AsyncFlatSpec is with serial execution context
  private implicit val ec: ExecutionContext = system.dispatcher

  protected implicit val loggingContextWithTrace: LoggingContextWithTrace =
    LoggingContextWithTrace.ForTesting

  // if we would need multi-db, polimorphism can come here, look for JdbcLedgerDaoBackend
  private val jdbcUrl = s"jdbc:h2:mem:${getClass.getSimpleName.toLowerCase};db_close_delay=-1"

  private val testServicesRef: AtomicReference[TestServices] = new AtomicReference()

  private def testServices: TestServices =
    Option(testServicesRef.get())
      .getOrElse(throw new Exception("TestServices not initialized. Not accessing from a test?"))

  protected def ingestUpdates(updates: Traced[Update]*): Offset = {
    val lastOffset = testServices.testReadService.push(updates.toVector)
    Iterator
      .continually(
        org.apache.pekko.pattern.after(20.millis)(testServices.index.currentLedgerEnd()).futureValue
      )
      .dropWhile(absoluteOffset =>
        Offset.fromHexString(Ref.HexString.assertFromString(absoluteOffset.value)) < lastOffset
      )
      .next()
    lastOffset
  }

  protected def index: IndexService = testServices.index

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    // We use the dispatcher here because the default Scalatest execution context is too slow.
    implicit val resourceContext: ResourceContext = ResourceContext(system.dispatcher)

    val testReadService = new TestReadService()
    val indexerConfig = IndexerConfig()

    val engine = new Engine(
      EngineConfig(LanguageVersion.StableVersions(LanguageMajorVersion.V2))
    )
    val mutableLedgerEndCache = MutableLedgerEndCache()
    val stringInterningView = new StringInterningView(loggerFactory)

    val indexResourceOwner =
      for {
        (inMemoryState, updaterFlow) <- LedgerApiServer.createInMemoryStateAndUpdater(
          commandProgressTracker = CommandProgressTracker.NoOp,
          indexServiceConfig = IndexServiceConfig(),
          maxCommandsInFlight = 1, // not used
          metrics = LedgerApiServerMetrics.ForTesting,
          executionContext = ec,
          tracer = NoReportingTracerProvider.tracer,
          loggerFactory = loggerFactory,
        )(mutableLedgerEndCache, stringInterningView)
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
        _indexerHealth <- new IndexerServiceOwner(
          participantId = Ref.ParticipantId.assertFromString("index-component-test-participant-id"),
          participantDataSourceConfig = ParticipantDataSourceConfig(jdbcUrl),
          readService = testReadService,
          config = indexerConfig,
          metrics = LedgerApiServerMetrics.ForTesting,
          inMemoryState = inMemoryState,
          inMemoryStateUpdaterFlow = updaterFlow,
          executionContext = ec,
          tracer = NoReportingTracerProvider.tracer,
          loggerFactory = loggerFactory,
          startupMode = IndexerStartupMode.MigrateAndStart,
          dataSourceProperties = IndexerConfig.createDataSourcePropertiesForTesting(
            indexerConfig.ingestionParallelism.unwrap
          ),
          highAvailability = HaConfig(),
          indexServiceDbDispatcher = Some(dbSupport.dbDispatcher),
          excludedPackageIds = Set.empty,
          clock = clock,
          reassignmentOffsetPersistence = NoOpReassignmentOffsetPersistence,
        )
        contractLoader <- ContractLoader.create(
          contractStorageBackend = dbSupport.storageBackendFactory.createContractStorageBackend(
            inMemoryState.ledgerEndCache,
            inMemoryState.stringInterningView,
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
          participantId = Ref.ParticipantId.assertFromString(IndexComponentTest.TestParticipantId),
          metrics = LedgerApiServerMetrics.ForTesting,
          servicesExecutionContext = ec,
          engine = new Engine(
            EngineConfig(LanguageVersion.StableVersions(LanguageMajorVersion.V2))
          ),
          inMemoryState = inMemoryState,
          tracer = NoReportingTracerProvider.tracer,
          loggerFactory = loggerFactory,
          incompleteOffsets = (_, _, _) => Future.successful(Vector.empty),
          contractLoader = contractLoader,
          getPackageMetadataSnapshot = _ => PackageMetadata(),
          lfValueTranslation = new LfValueTranslation(
            metrics = LedgerApiServerMetrics.ForTesting,
            engineO = Some(engine),
            // Not used
            loadPackage = (_packageId, _loggingContext) => Future(None),
            loggerFactory = loggerFactory,
          ),
        )
      } yield indexService

    val indexResource = indexResourceOwner.acquire()

    testServicesRef.set(
      TestServices(
        indexResource = indexResource,
        index = Await.result(indexResource.asFuture, 180.seconds),
        testReadService = testReadService,
      )
    )
  }

  override protected def afterAll(): Unit = {
    Await.result(testServices.indexResource.release(), 10.seconds)
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

  class TestReadService(implicit val materializer: Materializer)
      extends ReadService
      with InternalStateServiceProviderImpl {
    private var currentEnd: Int = 0
    private var queue: Vector[(Offset, Traced[Update])] = Vector.empty
    private var subscription: BoundedSourceQueue[(Offset, Traced[Update])] = _

    override def stateUpdates(beginAfter: Option[Offset])(implicit
        traceContext: TraceContext
    ): Source[(Offset, Traced[Update]), NotUsed] = blocking(synchronized {
      val (boundedSourceQueue, source) = Source
        .queue[(Offset, Traced[Update])](maxUpdateCount)
        .preMaterialize()
      subscription = boundedSourceQueue
      pushToSubscription(queue.dropWhile { case (offset, _) =>
        beginAfter.exists(_ > offset)
      })
      source
    })

    override def currentHealth(): HealthStatus = HealthStatus.healthy

    def push(updates: Vector[Traced[Update]]): Offset = blocking(synchronized {
      val offsetUpdates = updates
        .map { case update =>
          (nextOffset, update)
        }
      queue = queue ++ offsetUpdates
      pushToSubscription(offsetUpdates)
      ledgerEnd
    })

    def ledgerEnd: Offset = toOffset(currentEnd)

    private def toOffset(i: Int): Offset = {
      val bb = ByteBuffer.allocate(4)
      bb.putInt(i)
      Offset.fromByteArray(bb.array())
    }

    private def nextOffset: Offset = {
      currentEnd += 1
      toOffset(currentEnd)
    }

    private def pushToSubscription(updates: Vector[(Offset, Traced[Update])]): Unit =
      if (subscription != null) updates.map(subscription.offer).foreach {
        case QueueOfferResult.Enqueued => ()
        case notExpected => throw new Exception(s"Cannot fill queue: $notExpected")
      }
  }

  final case class TestServices(
      indexResource: Resource[IndexService],
      index: IndexService,
      testReadService: TestReadService,
  )

}
