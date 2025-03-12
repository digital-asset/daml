// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver

import cats.data.EitherT
import cats.syntax.either.*
import com.daml.metrics.api.MetricsContext
import com.daml.tracing.NoOpTelemetry
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.environment.CantonNodeParameters
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonServerBuilder
import com.digitalasset.canton.resource.{Storage, StorageSetup}
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
import com.digitalasset.canton.synchronizer.sequencer.Sequencer.{
  SignedOrderingRequest,
  SignedOrderingRequestOps,
}
import com.digitalasset.canton.synchronizer.sequencer.block.BlockOrderer
import com.digitalasset.canton.synchronizer.sequencer.block.BlockSequencerFactory.OrderingTimeFixMode
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.admin.BftOrderingSequencerAdminService
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftOrderingModuleSystemInitializer.BftOrderingStores
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver.BftBlockOrdererConfig.DefaultAuthenticationTokenManagerConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.data.AvailabilityStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.IssConsensusModule
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.PekkoBlockSubscription
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.OutputMetadataStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking.GrpcNetworking
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking.GrpcNetworking.P2PEndpoint
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking.authentication.ServerAuthenticatingServerFilter
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking.data.P2PEndpointsStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.OrderingTopologyProvider
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.{
  BftOrderingModuleSystemInitializer,
  CloseableActorSystem,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.Module.{
  SystemInitializationResult,
  SystemInitializer,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.ModuleRef
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BlockNumber
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.OrderingRequest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.snapshot.SequencerSnapshotAdditionalInfo
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.{
  Mempool,
  Output,
  SequencerNode,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.pekko.PekkoModuleSystem
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.pekko.PekkoModuleSystem.{
  PekkoEnv,
  PekkoFutureUnlessShutdown,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.p2p.grpc.{
  GrpcBftOrderingService,
  PekkoGrpcP2PNetworking,
}
import com.digitalasset.canton.synchronizer.sequencer.errors.SequencerError
import com.digitalasset.canton.synchronizer.sequencer.{AuthenticationServices, SequencerSnapshot}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.{
  BftOrderingServiceGrpc,
  BftOrderingServiceReceiveRequest,
  BftOrderingServiceReceiveResponse,
}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.{Member, SequencerId, SynchronizerId}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString
import io.grpc.stub.StreamObserver
import io.grpc.{ServerInterceptors, ServerServiceDefinition}
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.stream.{KillSwitch, Materializer}

import java.security.SecureRandom
import java.time.Instant
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Random

final class BftBlockOrderer(
    config: BftBlockOrdererConfig,
    sharedLocalStorage: Storage,
    synchronizerId: SynchronizerId,
    sequencerId: SequencerId,
    protocolVersion: ProtocolVersion,
    clock: Clock,
    orderingTopologyProvider: OrderingTopologyProvider[PekkoEnv],
    authenticationServicesO: Option[
      AuthenticationServices
    ], // Owned and managed by the sequencer runtime, absent in some tests
    nodeParameters: CantonNodeParameters,
    sequencerSubscriptionInitialHeight: Long,
    override val orderingTimeFixMode: OrderingTimeFixMode,
    sequencerSnapshotInfo: Option[SequencerSnapshot.ImplementationSpecificInfo],
    metrics: BftOrderingMetrics,
    namedLoggerFactory: NamedLoggerFactory,
    dedicatedStorageSetup: StorageSetup,
    queryCostMonitoring: Option[QueryCostMonitoringConfig] = None,
)(implicit executionContext: ExecutionContext, materializer: Materializer)
    extends BlockOrderer
    with NamedLogging
    with FlagCloseableAsync {

  require(
    sequencerSubscriptionInitialHeight >= BlockNumber.First,
    s"The sequencer subscription initial height must be non-negative, but was $sequencerSubscriptionInitialHeight",
  )

  private val thisNode = SequencerNodeId.toBftNodeId(sequencerId)

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

  override val timeouts: ProcessingTimeout = nodeParameters.processingTimeouts

  override def firstBlockHeight: Long = sequencerSubscriptionInitialHeight

  override protected val loggerFactory: NamedLoggerFactory =
    namedLoggerFactory
      .append(
        "bftOrderingEndpoint",
        config.initialNetwork.map(_.serverEndpoint.toString).getOrElse("unknown"),
      )

  private val closeable = FlagCloseable(loggerFactory.getTracedLogger(getClass), timeouts)
  implicit private val closeContext: CloseContext = CloseContext(closeable)

  private val p2pServerGrpcExecutor =
    Threading.newExecutionContext(
      loggerFactory.threadName + "-dp2p-server-grpc-executor-context",
      noTracingLogger,
    )

  private val maybeAuthenticationServices =
    Option
      .when(config.initialNetwork.exists(_.endpointAuthentication.enabled))(
        authenticationServicesO
      )
      .flatten

  private val authenticationTokenManagerConfig =
    config.initialNetwork
      .map(_.endpointAuthentication.authToken)
      .getOrElse(DefaultAuthenticationTokenManagerConfig)

  private val maybeServerAuthenticatingFilter =
    maybeAuthenticationServices.map { authenticationServices =>
      new ServerAuthenticatingServerFilter(
        synchronizerId,
        sequencerId,
        authenticationServices.syncCryptoForAuthentication.crypto,
        Seq(protocolVersion),
        authenticationTokenManagerConfig,
        timeouts,
        loggerFactory,
      )
    }

  private val p2pGrpcNetworking = {
    val maybeGrpcNetworkingAuthenticationInitialState =
      maybeAuthenticationServices.map { authenticationServices =>
        GrpcNetworking.AuthenticationInitialState(
          protocolVersion,
          synchronizerId,
          sequencerId,
          authenticationServices,
          authenticationTokenManagerConfig,
          serverEndpoint = config.initialNetwork.map { initialNetwork =>
            P2PEndpoint.fromEndpointConfig(initialNetwork.serverEndpoint.clientConfig)
          },
          clock,
        )
      }
    new GrpcNetworking(
      config.initialNetwork.map(_.serverEndpoint).map(createServer),
      maybeGrpcNetworkingAuthenticationInitialState,
      timeouts,
      loggerFactory,
    )
  }

  private val localStorage = {
    implicit val traceContext: TraceContext = TraceContext.empty
    config.storage match {
      case Some(storageConfig) =>
        logger.info("Using a dedicated storage configuration for BFT ordering tables")
        dedicatedStorageSetup.tryCreateAndMigrateStorage(
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

  private val p2pEndpointsStore = setupP2pEndpointsStore(localStorage)
  private val availabilityStore = AvailabilityStore(localStorage, timeouts, loggerFactory)
  private val epochStore = EpochStore(localStorage, timeouts, loggerFactory)
  private val outputStore = OutputMetadataStore(localStorage, timeouts, loggerFactory)

  private val sequencerSnapshotAdditionalInfo = sequencerSnapshotInfo.map { snapshot =>
    implicit val traceContext: TraceContext = TraceContext.empty
    SequencerSnapshotAdditionalInfo
      .fromProto(snapshot.info)
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

  private val PekkoModuleSystem.PekkoModuleSystemInitResult(
    actorSystem,
    SystemInitializationResult(
      mempoolRef,
      p2pNetworkInModuleRef,
      p2pNetworkOutAdminModuleRef,
      consensusAdminModuleRef,
      outputModuleRef,
    ),
  ) = createModuleSystem()

  // Start the gRPC server only now because they need the modules to be available before serving requests,
  //  else `tryCreateServerEndpoint` could end up with a `null` input module. However, we still need
  //  to create the networking component first because the modules depend on it.
  p2pGrpcNetworking.serverRole.startServer()

  private def createModuleSystem() =
    PekkoModuleSystem.tryCreate(
      "bftOrderingPekkoModuleSystem",
      createSystemInitializer(),
      createClientNetworkManager(),
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

  private def setupP2pEndpointsStore(storage: Storage): P2PEndpointsStore[PekkoEnv] = {
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
          "Using initial BFT network endpoints already present in the store instead of the configuration"
        )
      }
    }
    store
  }

  private def createSystemInitializer(): SystemInitializer[
    PekkoEnv,
    BftOrderingServiceReceiveRequest,
    Mempool.Message,
  ] = {
    val stores =
      BftOrderingStores(
        p2pEndpointsStore,
        availabilityStore,
        epochStore,
        epochStoreReader = epochStore,
        outputStore,
      )
    new BftOrderingModuleSystemInitializer(
      protocolVersion,
      thisNode,
      config,
      BlockNumber(sequencerSubscriptionInitialHeight),
      // TODO(#18910) test with multiple epoch lengths >= 1 (incl. 1)
      // TODO(#19289) support dynamically configurable epoch length
      IssConsensusModule.DefaultEpochLength,
      stores,
      orderingTopologyProvider,
      blockSubscription,
      sequencerSnapshotAdditionalInfo,
      clock,
      new Random(new SecureRandom()),
      metrics,
      loggerFactory,
      timeouts,
    )
  }

  private def createClientNetworkManager() =
    new PekkoGrpcP2PNetworking.PekkoClientP2PNetworkManager(
      p2pGrpcNetworking.clientRole.getServerHandleOrStartConnection,
      p2pGrpcNetworking.clientRole.closeConnection,
      timeouts,
      loggerFactory,
    )

  private def tryCreateServerEndpoint(
      clientEndpoint: StreamObserver[BftOrderingServiceReceiveResponse]
  ): StreamObserver[BftOrderingServiceReceiveRequest] = {
    p2pGrpcNetworking.serverRole.addClientHandle(clientEndpoint)
    PekkoGrpcP2PNetworking.tryCreateServerHandle(
      SequencerNodeId.toBftNodeId(sequencerId),
      p2pNetworkInModuleRef,
      clientEndpoint,
      p2pGrpcNetworking.serverRole.cleanupClientHandle,
      loggerFactory,
    )
  }

  private def createServer(
      serverConfig: ServerConfig
  ): UnlessShutdown[LifeCycle.CloseableServer] = {
    implicit val traceContext: TraceContext = TraceContext.empty
    performUnlessClosing("start-P2P-server") {

      import scala.jdk.CollectionConverters.*
      val activeServerBuilder =
        CantonServerBuilder
          .forConfig(
            config = serverConfig,
            adminToken = None,
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
                new GrpcBftOrderingService(
                  tryCreateServerEndpoint,
                  loggerFactory,
                ),
                executionContext,
              ),
              List( // Filters are applied in reverse order
                maybeServerAuthenticatingFilter,
                maybeAuthenticationServices.map(
                  _.authenticationServerInterceptor
                ),
              ).flatten.asJava,
            )
          )
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
      signedOrderingRequest: SignedOrderingRequest
  )(implicit traceContext: TraceContext): EitherT[Future, SequencerDeliverError, Unit] = {
    logger.debug(s"sending submission")
    sendToMempool(
      SendTag,
      signedOrderingRequest.submissionRequest.sender,
      signedOrderingRequest.toByteString,
    )
  }

  override def acknowledge(signedAcknowledgeRequest: SignedContent[AcknowledgeRequest])(implicit
      traceContext: TraceContext
  ): Future[Unit] = {
    val request = signedAcknowledgeRequest.content
    logger.debug(s"member ${request.member} acknowledging timestamp ${request.timestamp}")
    sendToMempool(
      AcknowledgeTag,
      signedAcknowledgeRequest.content.member,
      signedAcknowledgeRequest.toByteString,
    ).value.map(_ => ())
  }

  override def health(implicit traceContext: TraceContext): Future[SequencerDriverHealthStatus] = {
    val isStorageActive = sharedLocalStorage.isActive
    val description = if (isStorageActive) None else Some("BFT orderer can't connect to database")
    Future.successful(
      SequencerDriverHealthStatus(
        isActive = isStorageActive,
        description,
      )
    )
  }

  override def subscribe(
  )(implicit traceContext: TraceContext): Source[RawLedgerBlock, KillSwitch] =
    blockSubscription.subscription().map(BlockFormat.blockOrdererBlockToRawLedgerBlock(logger))

  override protected def closeAsync(): Seq[AsyncOrSyncCloseable] =
    maybeServerAuthenticatingFilter.map(_.closeAsync()).getOrElse(Seq.empty) ++
      Seq[AsyncOrSyncCloseable](
        SyncCloseable("p2pGrpcNetworking.close()", p2pGrpcNetworking.close()),
        SyncCloseable("p2pServerGrpcExecutor.shutdown()", p2pServerGrpcExecutor.shutdown()),
        SyncCloseable("blockSubscription.close()", blockSubscription.close()),
        SyncCloseable("epochStore.close()", epochStore.close()),
        SyncCloseable("outputStore.close()", outputStore.close()),
        SyncCloseable("availabilityStore.close()", availabilityStore.close()),
        SyncCloseable("p2pEndpointsStore.close()", p2pEndpointsStore.close()),
        SyncCloseable("shutdownPekkoActorSystem()", shutdownPekkoActorSystem()),
      ) ++
      Seq[Option[AsyncOrSyncCloseable]](
        Option.when(localStorage != sharedLocalStorage)(
          SyncCloseable("dedicatedLocalStorage.close()", localStorage.close())
        )
      ).flatten ++
      Seq[AsyncOrSyncCloseable](
        SyncCloseable("closeable.close()", closeable.close())
      )

  override def adminServices: Seq[ServerServiceDefinition] =
    Seq(
      v30.SequencerBftAdministrationServiceGrpc.bindService(
        new BftOrderingSequencerAdminService(
          p2pNetworkOutAdminModuleRef,
          consensusAdminModuleRef,
          loggerFactory,
        ),
        executionContext,
      )
    )

  override def sequencerSnapshotAdditionalInfo(
      timestamp: CantonTimestamp
  ): EitherT[Future, SequencerError, Option[v30.BftSequencerSnapshotAdditionalInfo]] = {
    val replyPromise = Promise[SequencerNode.SnapshotMessage]()
    val replyRef = new ModuleRef[SequencerNode.SnapshotMessage] {
      override def asyncSendTraced(msg: SequencerNode.SnapshotMessage)(implicit
          traceContext: TraceContext
      ): Unit =
        replyPromise.success(msg)
    }
    outputModuleRef.asyncSend(
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
      sender: Member,
      payload: ByteString,
  )(implicit traceContext: TraceContext): EitherT[Future, SequencerDeliverError, Unit] = {
    val replyPromise = Promise[SequencerNode.Message]()
    val replyRef = new ModuleRef[SequencerNode.Message] {
      override def asyncSendTraced(msg: SequencerNode.Message)(implicit
          traceContext: TraceContext
      ): Unit =
        replyPromise.success(msg)
    }
    mempoolRef.asyncSendTraced(
      Mempool.OrderRequest(
        Traced(
          OrderingRequest(
            tag,
            payload,
            orderingStartInstant = Some(Instant.now),
          )
        ),
        Some(replyRef),
        Some(sender),
      )
    )
    EitherT(replyPromise.future.map {
      case SequencerNode.RequestAccepted => Either.unit
      case SequencerNode.RequestRejected(reason) =>
        Left(SequencerErrors.Overloaded(reason)) // Currently the only case
      case _ => Left(SequencerErrors.Internal("wrong response"))
    })
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
