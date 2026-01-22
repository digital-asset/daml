// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.canton.sequencing

import cats.data.EitherT
import cats.syntax.either.*
import com.daml.metrics.api.MetricsContext
import com.daml.tracing.NoOpTelemetry
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.*
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.SynchronizerCryptoClient
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.environment.CantonNodeParameters
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonServerBuilder
import com.digitalasset.canton.resource.{Storage, StorageSingleSetup}
import com.digitalasset.canton.sequencer.admin.v30
import com.digitalasset.canton.sequencer.api.v30.SequencerAuthenticationServiceGrpc
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.synchronizer.block.BlockFormat.{AcknowledgeTag, SendTag}
import com.digitalasset.canton.synchronizer.block.{
  BlockFormat,
  RawLedgerBlock,
  SequencerDriverHealthStatus,
}
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.sequencer.Sequencer.SignedSubmissionRequest
import com.digitalasset.canton.synchronizer.sequencer.block.BlockOrderer
import com.digitalasset.canton.synchronizer.sequencer.block.BlockSequencerFactory.OrderingTimeFixMode
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.admin.{
  BftOrderingSequencerAdminService,
  GrpcBftOrderingSequencerPruningAdminService,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.canton.topology.{
  CantonOrderingTopologyProvider,
  SequencerNodeId,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.P2PGrpcNetworking.P2PEndpoint
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.PekkoP2PGrpcNetworking.PekkoP2PGrpcNetworkManager
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.authentication.ServerAuthenticatingServerInterceptor
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.standalone.P2PGrpcStandaloneBftOrderingService
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.pekko.PekkoModuleSystem.{
  PekkoEnv,
  PekkoFutureUnlessShutdown,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.pekko.{
  CloseableActorSystem,
  PekkoModuleSystem,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.standalone.topology.FixedFileBasedOrderingTopologyProvider
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig.{
  DefaultAuthenticationTokenManagerConfig,
  P2PConnectionManagementConfig,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftOrderingModuleSystemInitializer.BftOrderingStores
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.integration.canton.topology.OrderingTopologyProvider
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.data.AvailabilityStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.OutputMetadataStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.{
  OutputModule,
  PekkoBlockSubscription,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.p2p.P2PNetworkOutModule
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.p2p.data.P2PEndpointsStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.pruning.BftOrdererPruningScheduler
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.pruning.data.BftOrdererPruningSchedulerStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.{
  BftBlockOrdererConfig,
  BftOrderingModuleSystemInitializer,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.Module.SystemInitializer
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  BlockNumber,
  EpochLength,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.OrderingRequest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.snapshot.SequencerSnapshotAdditionalInfo
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.{
  Mempool,
  Output,
  SequencerNode,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{
  ModuleRef,
  P2PConnectionEventListener,
}
import com.digitalasset.canton.synchronizer.sequencer.errors.SequencerError
import com.digitalasset.canton.synchronizer.sequencer.{AuthenticationServices, SequencerSnapshot}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.standalone.v1.{
  SendRequest,
  SendResponse,
  StandaloneBftOrderingServiceGrpc,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.{
  BftOrderingMessage,
  BftOrderingServiceGrpc,
}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.{Member, PhysicalSynchronizerId, SequencerId}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.{PekkoUtil, SingleUseCell}
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString
import io.grpc.stub.StreamObserver
import io.grpc.{ServerInterceptors, ServerServiceDefinition}
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.stream.scaladsl.{Keep, Sink, Source}
import org.apache.pekko.stream.{KillSwitch, KillSwitches, Materializer}

import java.security.SecureRandom
import java.time.Instant
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.{ExecutionContextExecutor, Future, Promise}
import scala.util.Random

final class BftBlockOrderer(
    config: BftBlockOrdererConfig,
    sharedLocalStorage: Storage,
    cryptoApi: SynchronizerCryptoClient,
    sequencerId: SequencerId,
    clock: Clock,
    authenticationServicesO: Option[
      AuthenticationServices
    ], // Owned and managed by the sequencer runtime, absent in some tests
    nodeParameters: CantonNodeParameters,
    sequencerSubscriptionInitialHeight: Long,
    override val orderingTimeFixMode: OrderingTimeFixMode,
    sequencerSnapshotInfo: Option[SequencerSnapshot.ImplementationSpecificInfo],
    exitOnFatalFailures: Boolean,
    metrics: BftOrderingMetrics,
    override val loggerFactory: NamedLoggerFactory,
    queryCostMonitoring: Option[QueryCostMonitoringConfig],
    executionContext: ExecutionContextExecutor,
)(implicit materializer: Materializer, tracer: Tracer)
    extends BlockOrderer
    with NamedLogging
    with FlagCloseableAsync
    with HasCloseContext {

  import BftBlockOrderer.*

  implicit val ec: ExecutionContextExecutor =
    config.dedicatedExecutionContextDivisor.fold(executionContext) { divisor =>
      Threading.newExecutionContext(
        "bft-orderer-dedicated-ec",
        noTracingLogger,
        PositiveInt.tryCreate(
          Threading.detectNumberOfThreads(noTracingLogger).value / divisor
        ),
      )
    }

  require(
    sequencerSubscriptionInitialHeight >= BlockNumber.First,
    s"The sequencer subscription initial height must be non-negative, but was $sequencerSubscriptionInitialHeight",
  )

  private val longRunningExecutor = Executors.newCachedThreadPool()

  private val psId: PhysicalSynchronizerId = cryptoApi.psid

  private implicit val protocolVersion: ProtocolVersion = psId.protocolVersion

  private val isAuthenticationEnabled =
    config.initialNetwork.exists(_.endpointAuthentication.enabled)

  private val thisNode =
    config.standalone.fold(SequencerNodeId.toBftNodeId(sequencerId)) { standaloneConfig =>
      BftNodeId(standaloneConfig.thisSequencerId)
    }

  // The initial metrics factory, which also pre-initializes histograms (as required by OpenTelemetry), is built
  //  very early in the Canton bootstrap process, before unique IDs for synchronizer nodes are even available,
  //  so it doesn't include the sequencer ID in the labels, rather just the node name AKA "instance name".
  //
  //  The instance name, though, coming from the Canton config, is operator-chosen and is, in general, not unique and
  //  even uncorrelated with the sequencer ID, while the BFT ordering system must refer to nodes uniquely and, thus,
  //  refers to them only by their sequencer IDs.
  //
  //  Since we want to always be able to correlate the sequencer IDs included as additional metrics context, e.g. in
  //  consensus voting metrics, with the label used by each sequencer to identify itself as the metrics reporting
  //  sequencer, we use the sequencer ID for that, rather than the instance name.
  //
  //  Hence, we add to the metrics context this node's sequencer ID as the reporting sequencer.
  //  Also, we do it as soon as the BFT block orderer is created, so that all BFT ordering sequencers include it in all
  //  emitted metrics.
  private implicit val metricsContext: MetricsContext =
    MetricsContext(metrics.global.labels.ReportingSequencer -> thisNode)

  // Initialize the non-compliant behavior meter so that a value appears even if all behavior is compliant.
  metrics.security.noncompliant.behavior.mark(0)

  metrics.performance.enabled = config.enablePerformanceMetrics

  override val timeouts: ProcessingTimeout = nodeParameters.processingTimeouts

  private val standaloneServiceRef = new SingleUseCell[P2PGrpcStandaloneBftOrderingService]

  override def firstBlockHeight: Long = sequencerSubscriptionInitialHeight

  checkConfigSecurity()

  private val p2pServerGrpcExecutor =
    Threading.newExecutionContext(
      loggerFactory.threadName + "-dp2p-server-grpc-executor-context",
      noTracingLogger,
    )

  // Standalone mode doesn't support authentication
  private val maybeAuthenticationServices =
    Option
      .when(
        config.initialNetwork
          .exists(_.endpointAuthentication.enabled && config.standalone.isEmpty)
      )(
        authenticationServicesO
      )
      .flatten

  private val authenticationTokenManagerConfig =
    config.initialNetwork
      .map(_.endpointAuthentication.authToken)
      .getOrElse(DefaultAuthenticationTokenManagerConfig)

  private val maybeServerAuthenticatingFilter =
    maybeAuthenticationServices.map { authenticationServices =>
      new ServerAuthenticatingServerInterceptor(
        psId,
        sequencerId,
        authenticationServices.syncCryptoForAuthentication.crypto,
        Seq(protocolVersion),
        authenticationTokenManagerConfig,
        timeouts,
        loggerFactory,
      )
    }

  private val p2pGrpcServerManager =
    new P2PGrpcServerManager(
      config.initialNetwork.map(_.serverEndpoint).map(createServer),
      timeouts,
      loggerFactory,
    )

  private val localStorage = {
    implicit val traceContext: TraceContext = TraceContext.empty
    config.storage match {
      case Some(storageConfig) =>
        logger.info("Using a dedicated storage configuration for BFT ordering tables")
        StorageSingleSetup.tryCreateAndMigrateStorage(
          storageConfig,
          queryCostMonitoring,
          clock,
          timeouts,
          loggerFactory,
        )
      case _ =>
        logger.info("Re-using the existing sequencer storage for BFT ordering tables as well")
        sharedLocalStorage
    }
  }

  private val p2pGrpcConnectionState = new P2PGrpcConnectionState(thisNode, loggerFactory)

  private val p2pEndpointsStore = setupP2PEndpointsStore(localStorage)
  private val availabilityStore =
    AvailabilityStore(config.batchAggregator, localStorage, timeouts, loggerFactory)
  private val epochStore = EpochStore(config.batchAggregator, localStorage, timeouts, loggerFactory)
  private val outputStore = OutputMetadataStore(localStorage, timeouts, loggerFactory)
  private val pruningSchedulerStore =
    BftOrdererPruningSchedulerStore(localStorage, timeouts, loggerFactory)

  private val sequencerSnapshotAdditionalInfo = sequencerSnapshotInfo.map { snapshot =>
    implicit val traceContext: TraceContext = TraceContext.empty
    SequencerSnapshotAdditionalInfo
      .fromProto(protocolVersion, snapshot.info)
      .fold(
        error =>
          sys.error(
            s"BFT ordering bootstrap failed: can't deserialize additional info from sequencer snapshot " +
              s"due to: ${error.message}"
          ),
        additionalInfo => {
          logger.info(s"Received sequencer snapshot additional info: $additionalInfo")
          additionalInfo
        },
      )
  }

  private val isOrdererHealthy = new AtomicBoolean(true)

  private val outputPreviousStoredBlock = new OutputModule.PreviousStoredBlock

  private val PekkoModuleSystem.PekkoModuleSystemInitResult(
    actorSystem,
    initResult,
  ) = createModuleSystem()

  private val mempoolRef = initResult.inputModuleRef
  private val p2pNetworkInModuleRef = initResult.p2pNetworkInModuleRef
  private val p2pNetworkOutAdminModuleRef = initResult.p2pNetworkOutAdminModuleRef
  private val consensusAdminModuleRef = initResult.consensusAdminModuleRef
  private val outputModuleRef = initResult.outputModuleRef
  private val p2pNetworkManager = initResult.p2pNetworkManager

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  @volatile private var warnedAboutStandaloneSend = false

  // Start the gRPC server only now because it needs the modules to be available before serving requests,
  //  else creating a peer receiver could end up with a `null` input module.
  p2pGrpcServerManager.startServer()

  private def createModuleSystem(): PekkoModuleSystem.PekkoModuleSystemInitResult[Mempool.Message] =
    PekkoModuleSystem.tryCreate(
      "bftOrderingPekkoModuleSystem",
      createSystemInitializer(),
      createNetworkManager,
      exitOnFatalFailures,
      isOrdererHealthy,
      metrics,
      loggerFactory,
    )

  private lazy val blockSubscription =
    new PekkoBlockSubscription[PekkoEnv](
      BlockNumber(sequencerSubscriptionInitialHeight),
      timeouts,
      loggerFactory,
    )(
      abort = sys.error
    )

  private val standaloneSubscriptionKillSwitchF = Option.when(config.standalone.isDefined) {
    implicit val traceContext: TraceContext = TraceContext.empty
    PekkoUtil
      .runSupervised(
        blockSubscription
          .subscription()
          .map(b =>
            // The server is started earlier if standalone mode is enabled
            standaloneServiceRef.get.foreach(_.push(b.value))
          )
          .toMat(Sink.ignore)(Keep.both),
        errorLogMessagePrefix = "Failed to handle state changes",
      )
  }

  private def setupP2PEndpointsStore(storage: Storage): P2PEndpointsStore[PekkoEnv] = {
    val store = P2PEndpointsStore(storage, timeouts, loggerFactory)
    config.initialNetwork.foreach { network =>
      implicit val traceContext: TraceContext = TraceContext.empty
      val overwrite = network.overwriteStoredEndpoints
      awaitFuture(
        for {
          size <-
            if (overwrite) store.clearAllEndpoints().map(_ => 0)
            else store.listEndpoints.map(_.size)
          _ <-
            if (size == 0)
              PekkoFutureUnlessShutdown.sequence(
                network.peerEndpoints
                  .map(e => store.addEndpoint(P2PEndpoint.fromEndpointConfig(e)))
                  .map(_.map(_ => ()))
              )
            else PekkoFutureUnlessShutdown.pure(())
        } yield (),
        "init endpoints",
      )
      if (overwrite) {
        logger.info("BFT P2P endpoints from configuration written to the store (overwriting mode)")
      } else {
        logger.info(
          "Using initial BFT network endpoints already present in the store (populated with the configuration " +
            "if the store is empty) instead of the configuration"
        )
      }
    }
    store
  }

  private def createSystemInitializer(): SystemInitializer[
    PekkoEnv,
    PekkoP2PGrpcNetworkManager,
    BftOrderingMessage,
    Mempool.Message,
  ] = {
    val stores =
      BftOrderingStores(
        p2pEndpointsStore,
        availabilityStore,
        epochStore,
        epochStoreReader = epochStore,
        outputStore,
        pruningSchedulerStore,
      )
    val topologyProvider =
      config.standalone.fold[OrderingTopologyProvider[PekkoEnv]](
        new CantonOrderingTopologyProvider(cryptoApi, loggerFactory, metrics)
      ) { standaloneConfig =>
        new FixedFileBasedOrderingTopologyProvider(
          standaloneConfig,
          cryptoApi.pureCrypto,
          metrics,
        )
      }

    new BftOrderingModuleSystemInitializer(
      thisNode,
      config,
      BlockNumber(sequencerSubscriptionInitialHeight),
      // TODO(#18910) test with multiple epoch lengths >= 1 (incl. 1)
      // TODO(#19289) support dynamically configurable epoch length
      EpochLength(config.epochLength),
      stores,
      topologyProvider,
      blockSubscription,
      sequencerSnapshotAdditionalInfo,
      bootstrapMembership =>
        new P2PNetworkOutModule.State(p2pGrpcConnectionState, bootstrapMembership),
      clock,
      new Random(new SecureRandom()),
      metrics,
      loggerFactory,
      timeouts,
      outputPreviousStoredBlock = outputPreviousStoredBlock,
    )
  }

  private val pruningScheduler: BftOrdererPruningScheduler = {
    val scheduler = new BftOrdererPruningScheduler(
      pruningSchedulerStore,
      initResult.pruningModuleRef,
      loggerFactory,
      timeouts,
    )
    implicit val traceContext: TraceContext = TraceContext.empty
    timeouts.default.await(s"${getClass.getSimpleName} starting pruning scheduler")(
      scheduler.start()
    )
    scheduler
  }

  private def createNetworkManager(
      connectionEventListener: P2PConnectionEventListener,
      p2pNetworkIn: ModuleRef[BftOrderingMessage],
  ) =
    new PekkoP2PGrpcNetworkManager(
      createConnectionManager(connectionEventListener, p2pNetworkIn),
      timeouts,
      loggerFactory,
      metrics,
    )

  private def createConnectionManager(
      p2pConnectionEventListener: P2PConnectionEventListener,
      p2pNetworkIn: ModuleRef[BftOrderingMessage],
  ) = {
    val maybeGrpcNetworkingAuthenticationInitialState =
      maybeAuthenticationServices.map { authenticationServices =>
        P2PGrpcNetworking.AuthenticationInitialState(
          psId,
          sequencerId,
          authenticationServices,
          authenticationTokenManagerConfig,
          getServerToClientAuthenticationEndpoint(config),
          clock,
        )
      }
    new P2PGrpcConnectionManager(
      thisNode,
      config.initialNetwork
        .map(_.connectionManagementConfig)
        .getOrElse(P2PConnectionManagementConfig()),
      p2pGrpcConnectionState,
      maybeGrpcNetworkingAuthenticationInitialState,
      getServerToClientAuthenticationEndpoint(config),
      p2pConnectionEventListener,
      p2pNetworkIn,
      metrics,
      longRunningExecutor,
      timeouts,
      loggerFactory,
    )
  }

  // Called by the Scala gRPC service binding when we receive a request to establish the P2P gRPC streaming channel;
  //  it either returns a new receiver for the gRPC stream or throws, which fails the stream establishment and
  //  is propagated to the peer as an error.
  private def createPeerReceiverForIncomingConnection(
      peerSender: StreamObserver[BftOrderingMessage]
  )(implicit traceContext: TraceContext): UnlessShutdown[StreamObserver[BftOrderingMessage]] =
    p2pNetworkManager.connectionManager.createServerSidePeerReceiver(
      p2pNetworkInModuleRef,
      peerSender,
    )

  private def createServer(
      serverConfig: ServerConfig
  ): UnlessShutdown[LifeCycle.CloseableServer] = {
    implicit val traceContext: TraceContext = TraceContext.empty
    synchronizeWithClosingSync("start-P2P-server") {
      import scala.jdk.CollectionConverters.*
      val activeServerBuilder =
        CantonServerBuilder
          .forConfig(
            config = serverConfig,
            adminTokenDispenser = None,
            executor = p2pServerGrpcExecutor,
            loggerFactory = loggerFactory,
            apiLoggingConfig = nodeParameters.loggingConfig.api,
            tracing = nodeParameters.tracing,
            grpcMetrics = metrics.grpcMetrics,
            NoOpTelemetry,
          )
          .addService(
            ServerInterceptors.intercept(
              BftOrderingServiceGrpc.bindService(
                new P2PGrpcBftOrderingService(
                  createPeerReceiverForIncomingConnection,
                  loggerFactory,
                ),
                executionContext,
              ),
              List( // Filters are applied in reverse order
                maybeServerAuthenticatingFilter,
                maybeAuthenticationServices.map(_.authenticationServerInterceptor),
              ).flatten.asJava,
            )
          )
      config.standalone.foreach { _ =>
        val standaloneService =
          new P2PGrpcStandaloneBftOrderingService(orderSendRequest, loggerFactory)
        standaloneServiceRef.putIfAbsent(standaloneService).discard
        activeServerBuilder
          .addService(
            StandaloneBftOrderingServiceGrpc.bindService(
              standaloneService,
              executionContext,
            )
          )
          .discard
      }
      // Also offer the authentication service on BFT P2P endpoints, so that the BFT orderers don't have to also know the sequencer API endpoints
      maybeAuthenticationServices.fold(
        logger.info("P2P authentication disabled")
      ) { authenticationServices =>
        logger.info("P2P authentication enabled")
        activeServerBuilder
          .addService(
            SequencerAuthenticationServiceGrpc.bindService(
              authenticationServices.sequencerAuthenticationService,
              executionContext,
            )
          )
          .discard
      }
      logger
        .info(s"successfully bound P2P endpoint ${serverConfig.address}:${serverConfig.port}")
      LifeCycle.toCloseableServer(activeServerBuilder.build, logger, "P2PServer")
    }
  }

  override def send(
      signedSubmissionRequest: SignedSubmissionRequest
  )(implicit traceContext: TraceContext): EitherT[Future, SequencerDeliverError, Unit] =
    config.standalone.fold {
      logger.debug(
        "sending submission " +
          s"with message ID ${signedSubmissionRequest.content.messageId} " +
          s"from ${signedSubmissionRequest.content.sender} " +
          s"to ${signedSubmissionRequest.content.batch.allRecipients} "
      )
      sendToMempool(
        SendTag,
        signedSubmissionRequest.content.messageId.unwrap,
        signedSubmissionRequest.content.sender,
        signedSubmissionRequest.toByteString,
      )
    } { _ =>
      if (!warnedAboutStandaloneSend) {
        logger.warn(
          "BFT standalone mode enabled: ignoring send requests and not sending anything to the mempool"
        )
        warnedAboutStandaloneSend = true
      }
      EitherT.rightT(())
    }

  override def acknowledge(signedAcknowledgeRequest: SignedContent[AcknowledgeRequest])(implicit
      traceContext: TraceContext
  ): Future[Unit] = {
    val request = signedAcknowledgeRequest.content
    logger.debug(s"member ${request.member} acknowledging timestamp ${request.timestamp}")
    sendToMempool(
      AcknowledgeTag,
      "ACK-" + request.timestamp,
      signedAcknowledgeRequest.content.member,
      signedAcknowledgeRequest.toByteString,
    ).value.map(_ => ())
  }

  override def health(implicit traceContext: TraceContext): Future[SequencerDriverHealthStatus] = {
    val isStorageInactive = !localStorage.isActive
    val isOrdererUnhealthy = !isOrdererHealthy.get

    val description =
      if (isStorageInactive) Some("BFT orderer can't connect to database")
      else if (isOrdererUnhealthy)
        Some("BFT orderer encountered a problem, check the logs for errors")
      else None

    Future.successful(
      SequencerDriverHealthStatus(
        isActive = description.isEmpty,
        description,
      )
    )
  }

  override def subscribe(
  )(implicit traceContext: TraceContext): Source[Traced[RawLedgerBlock], KillSwitch] =
    config.standalone.fold(
      blockSubscription
        .subscription()
        .map(tracedBlock => tracedBlock.map(BlockFormat.blockOrdererBlockToRawLedgerBlock(logger)))
    ) { _ =>
      logger.info("BFT standalone mode enabled: not subscribing to any blocks")
      Source
        .empty[Traced[RawLedgerBlock]]
        .viaMat(KillSwitches.single)(
          Keep.right
        ) // In non-standalone mode, the block subscription is not used
    }

  override def sequencingTime(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[CantonTimestamp]] =
    FutureUnlessShutdown.pure(outputPreviousStoredBlock.getBlockNumberAndBftTime.map(_._2))

  override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = {
    logger.debug("Beginning async BFT block orderer shutdown")(TraceContext.empty)

    // Shutdown the P2P network client portion and module system
    SyncCloseable(
      "p2pNetworkManager.close()",
      p2pNetworkManager.close(),
    ) +:
      // Shutdown the server-authenticating server-side filter early (as it's also a client of the auth service),
      //  if authentication is enabled.
      (maybeServerAuthenticatingFilter.map(_.closeAsync()).getOrElse(Seq.empty) ++
        Seq[AsyncOrSyncCloseable](
          SyncCloseable("blockSubscription.close()", blockSubscription.close()),
          SyncCloseable("epochStore.close()", epochStore.close()),
          SyncCloseable("outputStore.close()", outputStore.close()),
          SyncCloseable("availabilityStore.close()", availabilityStore.close()),
          SyncCloseable("p2pEndpointsStore.close()", p2pEndpointsStore.close()),
          SyncCloseable("pruningScheduler.close()", pruningScheduler.close()),
          SyncCloseable("pruningSchedulerStore.close()", pruningSchedulerStore.close()),
          SyncCloseable("shutdownPekkoActorSystem()", shutdownPekkoActorSystem()),
        ) ++
        // Shutdown the dedicated local storage if present
        Option
          .when(localStorage != sharedLocalStorage)(
            SyncCloseable("dedicatedLocalStorage.close()", localStorage.close())
          )
          .toList ++
        // Shutdown the P2P server + connection manager and associated executor
        Seq[AsyncOrSyncCloseable](
          SyncCloseable(
            "p2pGrpcServerManager.close()",
            p2pGrpcServerManager.close(),
          ),
          SyncCloseable("p2pServerGrpcExecutor.shutdown()", p2pServerGrpcExecutor.shutdown()),
        ) ++
        // The kill switch ensures that we don't process the remaining contents of the queue buffer
        standaloneSubscriptionKillSwitchF
          .map(ks =>
            SyncCloseable(
              "standaloneSubscriptionKillSwitch.shutdown()",
              ks._1.shutdown(),
            )
          )
          .toList ++
        standaloneServiceRef.get.toList
          .map(s => SyncCloseable("standaloneServiceRef.close()", s.close())) ++
        Seq(
          SyncCloseable(
            "longRunningExecutor.shutdown()",
            longRunningExecutor.shutdown(),
          )
        ))
  }

  override def adminServices: Seq[ServerServiceDefinition] =
    Seq(
      v30.SequencerBftAdministrationServiceGrpc.bindService(
        new BftOrderingSequencerAdminService(
          mempoolRef,
          p2pNetworkOutAdminModuleRef,
          consensusAdminModuleRef,
          loggerFactory,
        ),
        executionContext,
      ),
      v30.SequencerBftPruningAdministrationServiceGrpc.bindService(
        new GrpcBftOrderingSequencerPruningAdminService(
          initResult.pruningModuleRef,
          pruningScheduler,
          loggerFactory,
        )(
          executionContext,
          metricsContext,
          TraceContext.empty,
        ),
        executionContext,
      ),
    )

  override def sequencerSnapshotAdditionalInfo(
      timestamp: CantonTimestamp
  ): EitherT[Future, SequencerError, Option[v30.BftSequencerSnapshotAdditionalInfo]] = {
    val replyPromise = Promise[SequencerNode.SnapshotMessage]()
    val replyRef = new ModuleRef[SequencerNode.SnapshotMessage] {
      override def asyncSend(msg: SequencerNode.SnapshotMessage)(implicit
          traceContext: TraceContext,
          metricsContext: MetricsContext,
      ): Unit =
        replyPromise.success(msg)
    }
    outputModuleRef.asyncSendNoTrace(
      Output.SequencerSnapshotMessage.GetAdditionalInfo(timestamp, replyRef)
    )
    EitherT(replyPromise.future.map {
      case SequencerNode.SnapshotMessage.AdditionalInfo(info) =>
        Right(Some(info))
      case SequencerNode.SnapshotMessage.AdditionalInfoRetrievalError(errorMessage) =>
        Left(SequencerError.SnapshotNotFound.CouldNotRetrieveSnapshot(errorMessage))
    })
  }

  private def shutdownPekkoActorSystem(): Unit = {
    logger.info(
      s"shutting down the actor system"
    )(TraceContext.empty)
    LifeCycle.close(
      new CloseableActorSystem(
        actorSystem,
        logger,
        timeouts.shutdownProcessing,
      )
    )(logger)
  }

  private def sendToMempool(
      tag: String,
      messageId: String,
      sender: Member,
      payload: ByteString,
  )(implicit traceContext: TraceContext): EitherT[Future, SequencerDeliverError, Unit] =
    sendToMempoolGeneric(tag, messageId, payload, Some(sender))

  private def orderSendRequest(
      request: SendRequest
  )(implicit traceContext: TraceContext): Future[SendResponse] =
    sendToMempoolGeneric(request.tag, "standalone", request.payload)
      .fold(e => SendResponse(Some(e.cause)), _ => SendResponse(None))

  private def sendToMempoolGeneric(
      tag: String,
      messageId: String,
      payload: ByteString,
      sender: Option[Member] = None,
  )(implicit traceContext: TraceContext): EitherT[Future, SequencerDeliverError, Unit] = {
    val replyPromise = Promise[SequencerNode.Message]()
    val replyRef = new ModuleRef[SequencerNode.Message] {
      override def asyncSend(msg: SequencerNode.Message)(implicit
          traceContext: TraceContext,
          metricsContext: MetricsContext,
      ): Unit =
        replyPromise.success(msg)
    }
    mempoolRef.asyncSend(
      Mempool.OrderRequest(
        Traced(
          OrderingRequest(
            tag,
            messageId,
            payload,
            orderingStartInstant = Some(Instant.now),
          )
        ),
        Some(replyRef),
        sender,
      )
    )
    EitherT(replyPromise.future.map {
      case SequencerNode.RequestAccepted => Either.unit
      case SequencerNode.RequestRejected(reason) =>
        Left(SequencerErrors.Overloaded(reason)) // Currently the only case
      case _ => Left(SequencerErrors.Internal("wrong response"))
    })
  }

  private def checkConfigSecurity(): Unit = {
    if (
      !(isAuthenticationEnabled || config.initialNetwork
        .map(_.peerEndpoints)
        .forall(_.forall(_.tlsConfig.forall(_.enabled))))
    )
      logger.warn(
        "Insecure setup: at least one of P2P endpoint authentication or mTLS must be set up for P2P endpoints to be verifiably associated to sequencer nodes"
      )(TraceContext.empty)

    if (config.initialNetwork.forall(_.serverEndpoint.tls.isEmpty))
      logger.info(
        "TLS is not enabled for the P2P server endpoint; make sure that at least TLS termination is correctly set up " +
          "to ensure channel confidentiality and integrity"
      )(TraceContext.empty)
  }

  private def awaitFuture[T](f: PekkoFutureUnlessShutdown[T], description: String)(implicit
      traceContext: TraceContext
  ): T = {
    logger.debug(description)
    timeouts.default
      .await(s"${getClass.getSimpleName} $description")(
        f.futureUnlessShutdown().failOnShutdownToAbortException(description)
      )(ErrorLoggingContext.fromTracedLogger(logger))
  }
}

object BftBlockOrderer {

  @VisibleForTesting
  private[sequencing] def getServerToClientAuthenticationEndpoint(
      config: BftBlockOrdererConfig
  ): Option[P2PEndpoint] =
    config.initialNetwork.map { initialNetwork =>
      P2PEndpoint.fromEndpointConfig(
        initialNetwork.serverEndpoint.serverToClientAuthenticationEndpointConfig
      )
    }
}
