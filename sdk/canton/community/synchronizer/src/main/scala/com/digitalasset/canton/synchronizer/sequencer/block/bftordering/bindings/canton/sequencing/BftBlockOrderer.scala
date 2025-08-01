// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.canton.sequencing

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
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.admin.{
  BftOrderingSequencerAdminService,
  BftOrderingSequencerPruningAdminService,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.canton.topology.SequencerNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.P2PGrpcNetworking.P2PEndpoint
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.PekkoP2PGrpcNetworking.PekkoP2PGrpcNetworkRefFactory
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.authentication.ServerAuthenticatingServerInterceptor
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.{
  P2PGrpcBftOrderingService,
  P2PGrpcClientConnectionManager,
  P2PGrpcNetworking,
  P2PGrpcServerConnectionManager,
  P2PGrpcStreamingServerSideReceiver,
  PekkoP2PGrpcNetworking,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.pekko.PekkoModuleSystem.{
  PekkoEnv,
  PekkoFutureUnlessShutdown,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.pekko.{
  CloseableActorSystem,
  PekkoModuleSystem,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig.DefaultAuthenticationTokenManagerConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftOrderingModuleSystemInitializer.BftOrderingStores
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.integration.canton.topology.OrderingTopologyProvider
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.data.AvailabilityStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.PekkoBlockSubscription
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.OutputMetadataStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.p2p.data.P2PEndpointsStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.{
  BftBlockOrdererConfig,
  BftOrderingModuleSystemInitializer,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.Module.SystemInitializer
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
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
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.{
  BftOrderingServiceGrpc,
  BftOrderingServiceReceiveRequest,
  BftOrderingServiceReceiveResponse,
}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.{Member, PhysicalSynchronizerId, SequencerId}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString
import io.grpc.stub.StreamObserver
import io.grpc.{ServerInterceptors, ServerServiceDefinition}
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.stream.{KillSwitch, Materializer}

import java.security.SecureRandom
import java.time.Instant
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Random, Success, Try}

final class BftBlockOrderer(
    config: BftBlockOrdererConfig,
    sharedLocalStorage: Storage,
    psid: PhysicalSynchronizerId,
    sequencerId: SequencerId,
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
    with FlagCloseableAsync
    with HasCloseContext {

  import BftBlockOrderer.*

  require(
    sequencerSubscriptionInitialHeight >= BlockNumber.First,
    s"The sequencer subscription initial height must be non-negative, but was $sequencerSubscriptionInitialHeight",
  )

  private implicit val protocolVersion: ProtocolVersion = psid.protocolVersion

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

  metrics.performance.enabled = config.enablePerformanceMetrics

  override val timeouts: ProcessingTimeout = nodeParameters.processingTimeouts

  override def firstBlockHeight: Long = sequencerSubscriptionInitialHeight

  private val loggedP2PEndpoint = config.initialNetwork
    .map(endpointConfig =>
      P2PEndpoint
        .fromEndpointConfig(
          endpointConfig.serverEndpoint.serverToClientAuthenticationEndpointConfig
        )
        .id
        .logString
    )
    .getOrElse("unknown")

  override protected val loggerFactory: NamedLoggerFactory =
    namedLoggerFactory
      .append(
        "p2pEndpoint",
        loggedP2PEndpoint,
      )

  checkConfigSecurity()

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
      new ServerAuthenticatingServerInterceptor(
        psid,
        sequencerId,
        authenticationServices.syncCryptoForAuthentication.crypto,
        Seq(protocolVersion),
        authenticationTokenManagerConfig,
        timeouts,
        loggerFactory,
      )
    }

  private val p2pGrpcServerConnectionManager =
    new P2PGrpcServerConnectionManager(
      config.initialNetwork.map(_.serverEndpoint).map(createServer),
      timeouts,
      loggerFactory,
    )

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

  private val PekkoModuleSystem.PekkoModuleSystemInitResult(
    actorSystem,
    initResult,
  ) = createModuleSystem()

  private val mempoolRef = initResult.inputModuleRef
  private val p2pNetworkInModuleRef = initResult.p2pNetworkInModuleRef
  private val p2pNetworkOutAdminModuleRef = initResult.p2pNetworkOutAdminModuleRef
  private val consensusAdminModuleRef = initResult.consensusAdminModuleRef
  private val outputModuleRef = initResult.outputModuleRef
  private val p2pNetworkRefFactory = initResult.p2pNetworkRefFactory

  // Start the gRPC server only now because it needs the modules to be available before serving requests,
  //  else creating a peer receiver could end up with a `null` input module.
  p2pGrpcServerConnectionManager.startServer()

  private def createModuleSystem(): PekkoModuleSystem.PekkoModuleSystemInitResult[Mempool.Message] =
    PekkoModuleSystem.tryCreate(
      "bftOrderingPekkoModuleSystem",
      createSystemInitializer(),
      createClientNetworkManager,
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
    PekkoP2PGrpcNetworkRefFactory,
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
      thisNode,
      config,
      BlockNumber(sequencerSubscriptionInitialHeight),
      // TODO(#18910) test with multiple epoch lengths >= 1 (incl. 1)
      // TODO(#19289) support dynamically configurable epoch length
      EpochLength(config.epochLength),
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

  private def createClientNetworkManager(P2PConnectionEventListener: P2PConnectionEventListener) =
    new PekkoP2PGrpcNetworking.PekkoP2PGrpcNetworkRefFactory(
      createP2PGrpcClientConnectionManager(P2PConnectionEventListener),
      timeouts,
      loggerFactory,
      metrics,
    )

  private def createP2PGrpcClientConnectionManager(
      p2pConnectionEventListener: P2PConnectionEventListener
  ) = {
    val maybeGrpcNetworkingAuthenticationInitialState =
      maybeAuthenticationServices.map { authenticationServices =>
        P2PGrpcNetworking.AuthenticationInitialState(
          psid,
          sequencerId,
          authenticationServices,
          authenticationTokenManagerConfig,
          getServerToClientAuthenticationEndpoint(config),
          clock,
        )
      }
    new P2PGrpcClientConnectionManager(
      thisNode,
      maybeGrpcNetworkingAuthenticationInitialState,
      p2pConnectionEventListener,
      timeouts,
      loggerFactory,
    )
  }

  // Called by the Scala gRPC service binding when we receive a request to establish the P2P gRPC streaming channel;
  //  it either returns a new receiver for the gRPC stream or throws, which fails the stream establishment and
  //  is propagated to the peer as an error.
  private def tryCreateServerSidePeerReceiver(
      peerSender: StreamObserver[BftOrderingServiceReceiveResponse]
  ): P2PGrpcStreamingServerSideReceiver = {
    implicit val traceContext: TraceContext = TraceContext.empty
    if (!isClosing) {
      p2pGrpcServerConnectionManager.addPeerSender(peerSender)
      logger.debug("Creating a peer sender for an incoming connection")
      Try(
        PekkoP2PGrpcNetworking.tryCreateServerSidePeerReceiver(
          SequencerNodeId.toBftNodeId(sequencerId),
          p2pNetworkInModuleRef,
          peerSender,
          p2pGrpcServerConnectionManager.cleanupPeerSender,
          getMessageSendInstant =
            (msg: BftOrderingServiceReceiveRequest) => msg.sentAt.map(_.asJavaInstant),
          loggerFactory,
          metrics,
        )
      ) match {
        case Failure(exception) =>
          logger.debug("Failed creating a peer sender for an incoming connection", exception)
          throw exception
        case Success(value) =>
          logger.debug("Successfully created a peer sender for an incoming connection")
          value
      }
    } else {
      logger.debug(
        "BFT block orderer not accepting new peer connections due to shutdown in progress"
      )
      throw new IllegalStateException("BFT block orderer is closed")
    }
  }

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
                new P2PGrpcBftOrderingService(tryCreateServerSidePeerReceiver, loggerFactory),
                executionContext,
              ),
              List( // Filters are applied in reverse order
                maybeServerAuthenticatingFilter,
                maybeAuthenticationServices.map(_.authenticationServerInterceptor),
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
    val isStorageActive = localStorage.isActive
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

  override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = {
    logger.debug("Starting BFT block orderer shutdown as requested")(TraceContext.empty)
    maybeServerAuthenticatingFilter.map(_.closeAsync()).getOrElse(Seq.empty) ++
      Seq[AsyncOrSyncCloseable](
        SyncCloseable(
          "p2pNetworkRefFactory.close()",
          p2pNetworkRefFactory.close(),
        ),
        SyncCloseable(
          "p2pGrpcServerConnectionManager.close()",
          p2pGrpcServerConnectionManager.close(),
        ),
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
      ).flatten
  }

  override def adminServices: Seq[ServerServiceDefinition] =
    Seq(
      v30.SequencerBftAdministrationServiceGrpc.bindService(
        new BftOrderingSequencerAdminService(
          p2pNetworkOutAdminModuleRef,
          consensusAdminModuleRef,
          loggerFactory,
        ),
        executionContext,
      ),
      v30.SequencerBftPruningAdministrationServiceGrpc.bindService(
        new BftOrderingSequencerPruningAdminService(initResult.pruningModuleRef, loggerFactory)(
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
      sender: Member,
      payload: ByteString,
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

  private def checkConfigSecurity(): Unit =
    if (config.initialNetwork.forall(_.serverEndpoint.tls.isEmpty))
      logger.info(
        "TLS is not enabled for the P2P server endpoint; make sure that at least TLS termination is correctly set up " +
          "to ensure channel confidentiality and integrity"
      )(TraceContext.empty)

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
