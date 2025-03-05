// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver

import cats.data.EitherT
import cats.syntax.either.*
import com.daml.jwt.JwtTimestampLeeway
import com.daml.metrics.api.MetricsContext
import com.daml.tracing.NoOpTelemetry
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, Port}
import com.digitalasset.canton.config.manual.CantonConfigValidatorDerivation
import com.digitalasset.canton.config.{
  AuthServiceConfig,
  BasicKeepAliveServerConfig,
  CantonConfigValidator,
  CantonConfigValidatorInstances,
  ClientConfig,
  FullClientConfig,
  KeepAliveClientConfig,
  PemFileOrString,
  ProcessingTimeout,
  QueryCostMonitoringConfig,
  ServerConfig,
  StorageConfig,
  TlsClientConfig,
  TlsServerConfig,
  UniformCantonConfigValidation,
}
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
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver.BftBlockOrderer.Config
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.AvailabilityModuleConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.data.AvailabilityStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.IssConsensusModule
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.IssSegmentModule.BlockCompletionTimeout
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.PekkoBlockSubscription
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.OutputMetadataStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.time.BftTime
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking.GrpcNetworking
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking.GrpcNetworking.P2PEndpoint
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
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.NumberIdentifiers.BlockNumber
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
import io.netty.handler.ssl.SslContext
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.stream.{KillSwitch, Materializer}

import java.security.SecureRandom
import java.time.Instant
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Random

final class BftBlockOrderer(
    config: Config,
    sharedLocalStorage: Storage,
    synchronizerId: SynchronizerId,
    sequencerId: SequencerId,
    protocolVersion: ProtocolVersion,
    clock: Clock,
    orderingTopologyProvider: OrderingTopologyProvider[PekkoEnv],
    authenticationServices: Option[
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

  import BftBlockOrderer.*

  require(
    sequencerSubscriptionInitialHeight >= BlockNumber.First,
    s"The sequencer subscription initial height must be non-negative, but was $sequencerSubscriptionInitialHeight",
  )

  // The initial metrics factory, which also pre-initializes histograms (as required by OpenTelemetry), is built
  //  very early in the Canton bootstrap process, before unique IDs for synchronizer nodes are even available,
  //  so it doesn't include the sequencer ID in the labels, rather just the node name AKA "instance name".
  //
  //  The instance name, though, coming from the Canton config, is operator-chosen and is, in general, not unique and
  //  even uncorrelated with the sequencer ID, while the BFT ordering system must refer to peers uniquely and, thus,
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
    MetricsContext(metrics.global.labels.ReportingSequencer -> sequencerId.toProtoPrimitive)

  // Initialize the non-compliant behavior meter so that a value appears even if all behavior is compliant.
  metrics.security.noncompliant.behavior.mark(0)

  override val timeouts: ProcessingTimeout = nodeParameters.processingTimeouts

  override def firstBlockHeight: Long = sequencerSubscriptionInitialHeight

  override protected val loggerFactory: NamedLoggerFactory =
    namedLoggerFactory
      .append(
        "bftOrderingPeerEndpoint",
        config.initialNetwork.map(_.serverEndpoint.toString).getOrElse("unknown"),
      )

  private val closeable = FlagCloseable(loggerFactory.getTracedLogger(getClass), timeouts)
  implicit private val closeContext: CloseContext = CloseContext(closeable)

  private val p2pServerGrpcExecutor =
    Threading.newExecutionContext(
      loggerFactory.threadName + "-dp2p-server-grpc-executor-context",
      noTracingLogger,
    )

  private val p2pGrpcNetworking =
    new GrpcNetworking(
      servers = config.initialNetwork.toList.map { case P2PNetworkConfig(serverEndpoint, _, _) =>
        createServer(serverEndpoint)
      },
      authenticationServices.map { as =>
        GrpcNetworking.Authentication(
          protocolVersion,
          synchronizerId,
          sequencerId,
          as.syncCryptoForAuthentication,
          clock,
        )
      },
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

  // Start the gRPC servers only now because they need the modules to be available before serving requests,
  //  else tryCreateServerEndpoint could end up with a null input module. However, we still need
  //  to create the networking component first because the modules depend on it.
  p2pGrpcNetworking.serverRole.startServers()

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
      sequencerId,
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
      sequencerId,
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
              authenticationServices.map(_.authenticationInterceptor).toList.asJava,
            )
          )
      // Also offer the authentication service on BFT P2P endpoints, so that the BFT orderers don't have to also know the sequencer API endpoints
      authenticationServices.fold(logger.info("P2P authentication disabled")) { as =>
        logger.info("P2P authentication enabled")
        activeServerBuilder
          .addService(
            SequencerAuthenticationServiceGrpc.bindService(
              as.sequencerAuthenticationService,
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

object BftBlockOrderer {

  val DefaultMaxRequestPayloadBytes: Int = 1 * 1024 * 1024
  val DefaultMaxMempoolQueueSize: Int = 10 * 1024
  val DefaultMaxRequestsInBatch: Short = 16
  val DefaultMinRequestsInBatch: Short = 3
  val DefaultMaxBatchCreationInterval: FiniteDuration = 100.milliseconds
  val DefaultMaxBatchesPerProposal: Short = 16
  val DefaultOutputFetchTimeout: FiniteDuration = 1.second

  /** @param maxRequestsInBatch
    *   A maximum number of requests in a batch. Needs to be the same across the network for the BFT
    *   time assumptions to hold. It is validated in runtime.
    * @param maxBatchesPerBlockProposal
    *   A maximum number of batches per block proposal (pre-prepare). Needs to be the same across
    *   the network for the BFT time assumptions to hold. It is validated in runtime.
    */
  final case class Config(
      maxRequestPayloadBytes: Int = DefaultMaxRequestPayloadBytes,
      maxMempoolQueueSize: Int = DefaultMaxMempoolQueueSize,
      // TODO(#24184) make a dynamic sequencing parameter
      maxRequestsInBatch: Short = DefaultMaxRequestsInBatch,
      minRequestsInBatch: Short = DefaultMinRequestsInBatch,
      maxBatchCreationInterval: FiniteDuration = DefaultMaxBatchCreationInterval,
      // TODO(#24184) make a dynamic sequencing parameter
      maxBatchesPerBlockProposal: Short = DefaultMaxBatchesPerProposal,
      outputFetchTimeout: FiniteDuration = DefaultOutputFetchTimeout,
      initialNetwork: Option[P2PNetworkConfig] = None,
      storage: Option[StorageConfig] = None,
  ) extends UniformCantonConfigValidation {
    // The below parameters are not yet dynamically configurable.
    private val EmptyBlockCreationIntervalMultiplayer = 3L
    require(
      BlockCompletionTimeout > AvailabilityModuleConfig.EmptyBlockCreationInterval * EmptyBlockCreationIntervalMultiplayer,
      s"The block completion timeout should be sufficiently larger (currently $EmptyBlockCreationIntervalMultiplayer times) " +
        "than the empty block creation interval to avoid unnecessary view changes.",
    )

    private val maxRequestsPerBlock = maxBatchesPerBlockProposal * maxRequestsInBatch
    require(
      maxRequestsPerBlock < BftTime.MaxRequestsPerBlock,
      s"Maximum block size too big: $maxRequestsInBatch maximum requests per batch and " +
        s"$maxBatchesPerBlockProposal maximum batches per block proposal means " +
        s"$maxRequestsPerBlock maximum requests per block, " +
        s"but the maximum number allowed of requests per block is ${BftTime.MaxRequestsPerBlock}",
    )
  }
  object Config {
    implicit val configCantonConfigValidator: CantonConfigValidator[Config] =
      CantonConfigValidatorDerivation[Config]
  }

  final case class P2PNetworkConfig(
      serverEndpoint: P2PServerConfig,
      peerEndpoints: Seq[P2PEndpointConfig],
      overwriteStoredEndpoints: Boolean = false,
  ) extends UniformCantonConfigValidation
  object P2PNetworkConfig {
    implicit val bftNetworkCanonConfigValidator: CantonConfigValidator[P2PNetworkConfig] =
      CantonConfigValidatorDerivation[P2PNetworkConfig]
  }

  final case class P2PServerConfig(
      override val address: String,
      override val internalPort: Option[Port] = None,
      tls: Option[TlsServerConfig] = None,
      override val maxInboundMessageSize: NonNegativeInt = ServerConfig.defaultMaxInboundMessageSize,
  ) extends ServerConfig
      with UniformCantonConfigValidation {

    override val jwtTimestampLeeway: Option[JwtTimestampLeeway] = None
    override val keepAliveServer: Option[BasicKeepAliveServerConfig] = None
    override val authServices: Seq[AuthServiceConfig] = Seq.empty
    override val adminToken: Option[String] = None

    override def sslContext: Option[SslContext] = tls.map(CantonServerBuilder.sslContext(_))

    override def serverCertChainFile: Option[PemFileOrString] = tls.map(_.certChainFile)

    def clientConfig: FullClientConfig =
      FullClientConfig(
        address,
        port,
        tls = tls.map(_.clientConfig),
        keepAliveClient = None,
      )
  }
  object P2PServerConfig {
    implicit val adminServerConfigCantonConfigValidator: CantonConfigValidator[P2PServerConfig] = {
      import CantonConfigValidatorInstances.*
      CantonConfigValidatorDerivation[P2PServerConfig]
    }
  }

  final case class P2PEndpointConfig(
      override val address: String,
      override val port: Port,
      override val tlsConfig: Option[TlsClientConfig] = Some(
        TlsClientConfig(trustCollectionFile = None, clientCert = None, enabled = true)
      ),
  ) extends ClientConfig
      with UniformCantonConfigValidation {
    override val keepAliveClient: Option[KeepAliveClientConfig] = None
  }
  object P2PEndpointConfig {
    implicit val p2pEndpointConfigCantonConfigValidator
        : CantonConfigValidator[P2PEndpointConfig] = {
      import CantonConfigValidatorInstances.*
      CantonConfigValidatorDerivation[P2PEndpointConfig]
    }
  }

  final case class EndpointId(
      address: String,
      port: Port,
      tls: Boolean,
  )
}
