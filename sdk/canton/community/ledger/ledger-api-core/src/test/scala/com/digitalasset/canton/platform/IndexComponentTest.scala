// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform

import com.daml.ledger.api.testing.utils.PekkoBeforeAndAfterAll
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.digitalasset.canton.config.{BatchingConfig, CachingConfigs, ProcessingTimeout}
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.ledger.participant.state.Update.TransactionAccepted.RepresentativePackageId.SameAsContractPackageId
import com.digitalasset.canton.ledger.participant.state.Update.{
  ContractInfo,
  OnPRReassignmentAccepted,
  RepairReassignmentAccepted,
  RepairTransactionAccepted,
  SequencedReassignmentAccepted,
  SequencedTransactionAccepted,
}
import com.digitalasset.canton.ledger.participant.state.index.IndexService
import com.digitalasset.canton.ledger.participant.state.{Reassignment, Update}
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, HasCloseContext}
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.metrics.{CommonMockMetrics, LedgerApiServerMetrics}
import com.digitalasset.canton.participant.ledger.api.LedgerApiJdbcUrl
import com.digitalasset.canton.participant.store.ContractStore
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
import com.digitalasset.canton.platform.store.{
  DbSupport,
  FlywayMigrations,
  LedgerApiContractStore,
  LedgerApiContractStoreImpl,
  PruningOffsetService,
}
import com.digitalasset.canton.protocol.{ContractInstance, LfContractId}
import com.digitalasset.canton.resource.DbStorageSingle
import com.digitalasset.canton.store.db.DbStorageSetup.DbBasicConfig
import com.digitalasset.canton.store.packagemeta.PackageMetadata
import com.digitalasset.canton.time.{SimClock, WallClock}
import com.digitalasset.canton.tracing.{NoReportingTracerProvider, TraceContext}
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.util.PekkoUtil.{FutureQueue, IndexingFutureQueue}
import com.digitalasset.canton.{BaseTest, HasExecutorService}
import com.digitalasset.daml.lf.data.{Bytes, Ref}
import com.digitalasset.daml.lf.engine.{Engine, EngineConfig}
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.transaction.test.{NodeIdTransactionBuilder, TestNodeBuilder}
import org.scalatest.Suite

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}

trait IndexComponentTest
    extends PekkoBeforeAndAfterAll
    with BaseTest
    with HasExecutorService
    with HasCloseContext
    with FlagCloseable {
  self: Suite =>

  private val clock = new WallClock(ProcessingTimeout(), loggerFactory)

  implicit val ec: ExecutionContext = system.dispatcher

  protected implicit val loggingContextWithTrace: LoggingContextWithTrace =
    LoggingContextWithTrace.ForTesting

  private val dbName: String = getClass.getSimpleName.toLowerCase

  protected val dbConfig: com.digitalasset.canton.config.DbConfig =
    DbBasicConfig(username = "", password = "", dbName = dbName, host = "", port = 0).toH2DbConfig

  private def jdbcUrl: String = LedgerApiJdbcUrl.fromDbConfig(dbConfig).value.url

  protected val indexerConfig: IndexerConfig = IndexerConfig()

  protected val indexServiceConfig: IndexServiceConfig = IndexServiceConfig()

  protected val indexReadConnectionPoolSize: Int = 10

  private val testServicesRef: AtomicReference[TestServices] = new AtomicReference()

  private def testServices: TestServices =
    Option(testServicesRef.get())
      .getOrElse(throw new Exception("TestServices not initialized. Not accessing from a test?"))

  private def ledgerEndOffset = testServices.index.currentLedgerEnd().futureValue

  protected def ingestUpdates(updates: (Update, Vector[ContractInstance])*): Offset = {
    val ledgerEndLongBefore = ledgerEndOffset.map(_.positive).getOrElse(0L)
    // contracts should be stored in participant contract store before ingesting the updates to get the internal contract ids mapping
    MonadUtil
      .sequentialTraverse_(updates) { case (update, contracts) =>
        storeContracts(update, contracts).flatMap(testServices.indexer.offer)
      }
      .futureValue
    val expectedOffset = Offset.tryFromLong(updates.size + ledgerEndLongBefore)
    eventually() {
      ledgerEndOffset shouldBe Some(expectedOffset)
      expectedOffset
    }
  }

  protected def ingestUpdateAsync(update: Update): Future[Unit] =
    testServices.indexer.offer(update).map(_ => ())

  protected def storeContracts(
      update: Update,
      contracts: Vector[ContractInstance],
  ): Future[Update] =
    // this mimics protocol processing that stores contracts and retrieves their internal contract ids afterward
    testServices.participantContractStore
      .storeContracts(contracts)
      .flatMap(_ =>
        testServices.participantContractStore
          .lookupBatchedNonCachedInternalIds(
            contracts.map(_.contractId)
          )
      )
      .map { internalContractIds =>
        update match {
          case txAccepted: SequencedTransactionAccepted =>
            txAccepted.copy(contractInfos =
              injectInternalContractIds(
                txAccepted.contractInfos,
                internalContractIds,
              )
            )
          case txAccepted: RepairTransactionAccepted =>
            txAccepted.copy(contractInfos =
              injectInternalContractIds(
                txAccepted.contractInfos,
                internalContractIds,
              )
            )
          case reassignment: SequencedReassignmentAccepted =>
            reassignment.copy(
              reassignment =
                injectInternalContractIds(reassignment.reassignment, internalContractIds)
            )
          case reassignment: RepairReassignmentAccepted =>
            reassignment.copy(
              reassignment =
                injectInternalContractIds(reassignment.reassignment, internalContractIds)
            )
          case reassignment: OnPRReassignmentAccepted =>
            reassignment.copy(
              reassignment =
                injectInternalContractIds(reassignment.reassignment, internalContractIds)
            )
          case other => other
        }
      }

  private def injectInternalContractIds(
      contractInfo: Map[LfContractId, ContractInfo],
      internalContractIds: Map[LfContractId, Long],
  ): Map[LfContractId, ContractInfo] =
    internalContractIds.foldLeft(contractInfo) { case (acc, (coid, internalContractId)) =>
      acc.updated(
        coid,
        acc.get(coid) match {
          case Some(contractInfo) => contractInfo.copy(internalContractId = internalContractId)
          case None =>
            ContractInfo(
              internalContractId = internalContractId,
              contractAuthenticationData = Bytes.Empty,
              representativePackageId = SameAsContractPackageId,
            )
        },
      )
    }

  private def injectInternalContractIds(
      reassignmentBatch: Reassignment.Batch,
      internalContractIds: Map[LfContractId, Long],
  ): Reassignment.Batch = Reassignment.Batch(
    reassignmentBatch.reassignments.map {
      case assign: Reassignment.Assign =>
        assign.copy(internalContractId = internalContractIds.get(assign.createNode.coid).value)
      case unassign: Reassignment.Unassign => unassign: Reassignment
    }
  )

  protected def index: IndexService = testServices.index

  protected def sequentialPostProcessor: Update => Unit = _ => ()

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    // We use the dispatcher here because the default Scalatest execution context is too slow.
    implicit val resourceContext: ResourceContext = ResourceContext(system.dispatcher)

    val engine = new Engine(
      EngineConfig(LanguageVersion.stableLfVersionsRange)
    )
    val mutableLedgerEndCache = MutableLedgerEndCache()
    val stringInterningView = new StringInterningView(loggerFactory)
    val participantId = Ref.ParticipantId.assertFromString("index-component-test-participant-id")

    val pruningOffsetService = new PruningOffsetService {
      override def pruningOffset(implicit
          traceContext: TraceContext
      ): Future[Option[Offset]] = Future.successful(None)
    }

    val indexResourceOwner =
      for {
        dbStorage <- ResourceOwner
          .forCloseable(() =>
            DbStorageSingle
              .tryCreate(
                config = dbConfig,
                connectionPoolForParticipant = false,
                logQueryCost = None,
                clock = new SimClock(CantonTimestamp.Epoch, loggerFactory),
                scheduler = None,
                metrics = CommonMockMetrics.dbStorage,
                timeouts = timeouts,
                loggerFactory = loggerFactory,
              )
          )
        contractStore <-
          ResourceOwner
            .forCloseable(() =>
              ContractStore.create(
                storage = dbStorage,
                processingTimeouts = timeouts,
                cachingConfigs = CachingConfigs(),
                batchingConfig = BatchingConfig(),
                loggerFactory = loggerFactory,
              )
            )
        participantContractStore = LedgerApiContractStoreImpl(
          contractStore,
          loggerFactory,
          LedgerApiServerMetrics.ForTesting,
        )
        (inMemoryState, updaterFlow) <- LedgerApiServerInternals.createInMemoryStateAndUpdater(
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
                connectionPoolSize = indexReadConnectionPoolSize,
                connectionTimeout = 250.millis,
              ),
            ),
            loggerFactory = loggerFactory,
          )
        indexerF <- new JdbcIndexer.Factory(
          participantId = participantId,
          participantDataSourceConfig = DbSupport.ParticipantDataSourceConfig(jdbcUrl),
          config = indexerConfig,
          metrics = LedgerApiServerMetrics.ForTesting,
          inMemoryState = inMemoryState,
          apiUpdaterFlow = updaterFlow,
          executionContext = ec,
          tracer = NoReportingTracerProvider.tracer,
          loggerFactory = loggerFactory,
          dataSourceProperties = IndexerConfig.createDataSourcePropertiesForTesting(indexerConfig),
          highAvailability = HaConfig(),
          indexSericeDbDispatcher = None,
          clock = clock,
          reassignmentOffsetPersistence = NoOpReassignmentOffsetPersistence,
          postProcessor = (_, _) => Future.unit,
          sequentialPostProcessor = sequentialPostProcessor,
          contractStore = participantContractStore,
        ).initialized()
        indexerFutureQueueConsumer <- ResourceOwner.forFuture(() => indexerF(false)(_ => ()))
        indexer <- ResourceOwner.forReleasable(() =>
          new IndexingFutureQueue(indexerFutureQueueConsumer)
        ) { indexer =>
          indexer.shutdown()
          indexer.done.map(_ => ())
        }
        contractLoader <- ContractLoader.create(
          participantContractStore = participantContractStore,
          contractStorageBackend = dbSupport.storageBackendFactory.createContractStorageBackend(
            inMemoryState.stringInterningView,
            inMemoryState.ledgerEndCache,
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
          config = indexServiceConfig,
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
          getPackagePreference = (
              _: PackageName,
              _: Set[PackageId],
              _: String,
              _: LoggingContextWithTrace,
          ) => FutureUnlessShutdown.pure(Left("not used")),
          participantContractStore = participantContractStore,
          pruningOffsetService = pruningOffsetService,
        )
      } yield (indexService, indexer, participantContractStore)

    val indexResource = indexResourceOwner.acquire()
    val (index, indexer, participantContractStore) = indexResource.asFuture.futureValue

    testServicesRef.set(
      TestServices(
        indexResource = indexResource,
        index = index,
        indexer = indexer,
        participantContractStore = participantContractStore,
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

  final case class TestServices(
      indexResource: Resource[Any],
      index: IndexService,
      indexer: FutureQueue[Update],
      participantContractStore: LedgerApiContractStore,
  )
}
