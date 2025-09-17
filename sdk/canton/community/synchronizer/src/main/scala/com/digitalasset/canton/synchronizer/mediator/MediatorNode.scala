// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.mediator

import cats.data.{EitherT, OptionT}
import cats.syntax.either.*
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.admin.mediator.v30.MediatorStatusServiceGrpc.MediatorStatusService
import com.digitalasset.canton.auth.CantonAdminTokenDispenser
import com.digitalasset.canton.common.sequencer.grpc.SequencerInfoLoader
import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.config.*
import com.digitalasset.canton.config.manual.CantonConfigValidatorDerivation
import com.digitalasset.canton.connection.GrpcApiInfoService
import com.digitalasset.canton.connection.v30.ApiInfoServiceGrpc
import com.digitalasset.canton.crypto.{
  Crypto,
  CryptoHandshakeValidator,
  SynchronizerCrypto,
  SynchronizerCryptoClient,
}
import com.digitalasset.canton.environment.*
import com.digitalasset.canton.health.*
import com.digitalasset.canton.health.admin.data.{WaitingForExternalInput, WaitingForInitialization}
import com.digitalasset.canton.lifecycle.UnlessShutdown.Outcome
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, HasCloseContext, LifeCycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.mediator.admin.v30.MediatorInitializationServiceGrpc
import com.digitalasset.canton.networking.grpc.{CantonGrpcUtil, CantonMutableHandlerRegistry}
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.sequencing.client.{
  RecordingConfig,
  ReplayConfig,
  RequestSigner,
  SequencerClient,
  SequencerClientConfig,
  SequencerClientFactory,
}
import com.digitalasset.canton.sequencing.{
  GrpcSequencerConnectionXPoolFactory,
  SequencerConnectionXPool,
  SequencerConnections,
}
import com.digitalasset.canton.store.*
import com.digitalasset.canton.synchronizer.Synchronizer
import com.digitalasset.canton.synchronizer.mediator.admin.data.MediatorNodeStatus
import com.digitalasset.canton.synchronizer.mediator.admin.gprc.{
  InitializeMediatorRequest,
  InitializeMediatorResponse,
}
import com.digitalasset.canton.synchronizer.mediator.service.{
  GrpcMediatorInitializationService,
  GrpcMediatorStatusService,
}
import com.digitalasset.canton.synchronizer.mediator.store.{
  MediatorSynchronizerConfiguration,
  MediatorSynchronizerConfigurationStore,
}
import com.digitalasset.canton.synchronizer.metrics.MediatorMetrics
import com.digitalasset.canton.synchronizer.service.GrpcSequencerConnectionService
import com.digitalasset.canton.time.{Clock, HasUptime, SynchronizerTimeTracker}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.admin.grpc.PSIdLookup
import com.digitalasset.canton.topology.client.SynchronizerTopologyClient
import com.digitalasset.canton.topology.processing.{
  InitialTopologySnapshotValidator,
  TopologyTransactionProcessor,
}
import com.digitalasset.canton.topology.store.TopologyStoreId.SynchronizerStore
import com.digitalasset.canton.topology.store.{TopologyStore, TopologyStoreId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.{MonadUtil, SingleUseCell}
import com.digitalasset.canton.version.{
  ProtocolVersion,
  ProtocolVersionCompatibility,
  ReleaseVersion,
}
import com.google.common.annotations.VisibleForTesting
import io.grpc.ServerServiceDefinition
import monocle.Lens
import monocle.macros.syntax.lens.*
import org.apache.pekko.actor.ActorSystem

import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.Future
import scala.util.Success

/** Various parameters for non-standard mediator settings
  *
  * @param dontWarnOnDeprecatedPV
  *   if true, then this mediator will not emit a warning when connecting to a sequencer using a
  *   deprecated protocol version.
  */
final case class MediatorNodeParameterConfig(
    // TODO(i15561): Revert back to `false` once there is a stable Daml 3 protocol version
    override val alphaVersionSupport: Boolean = true,
    override val betaVersionSupport: Boolean = false,
    override val dontWarnOnDeprecatedPV: Boolean = false,
    override val batching: BatchingConfig = BatchingConfig(),
    override val caching: CachingConfigs = CachingConfigs(),
    override val watchdog: Option[WatchdogConfig] = None,
) extends ProtocolConfig
    with LocalNodeParametersConfig
    with UniformCantonConfigValidation

object MediatorNodeParameterConfig {
  implicit val mediatorNodeParameterConfigCantonConfigValidator
      : CantonConfigValidator[MediatorNodeParameterConfig] =
    CantonConfigValidatorDerivation[MediatorNodeParameterConfig]
}

final case class MediatorNodeParameters(
    general: CantonNodeParameters.General,
    protocol: CantonNodeParameters.Protocol,
) extends CantonNodeParameters
    with HasGeneralCantonNodeParameters
    with HasProtocolCantonNodeParameters

final case class RemoteMediatorConfig(
    adminApi: FullClientConfig,
    token: Option[String] = None,
) extends NodeConfig
    with UniformCantonConfigValidation {
  override def clientAdminApi: ClientConfig = adminApi
}
object RemoteMediatorConfig {
  implicit val remoteMediatorConfigCantonConfigValidator
      : CantonConfigValidator[RemoteMediatorConfig] =
    CantonConfigValidatorDerivation[RemoteMediatorConfig]
}

/** Mediator Node configuration that defaults to auto-init
  */
final case class MediatorNodeConfig(
    override val adminApi: AdminServerConfig = AdminServerConfig(),
    override val storage: StorageConfig = StorageConfig.Memory(),
    override val crypto: CryptoConfig = CryptoConfig(),
    replication: Option[ReplicationConfig] = None,
    override val init: InitConfig = InitConfig(),
    timeTracker: SynchronizerTimeTrackerConfig = SynchronizerTimeTrackerConfig(),
    override val sequencerClient: SequencerClientConfig = SequencerClientConfig(),
    caching: CachingConfigs = CachingConfigs(),
    override val parameters: MediatorNodeParameterConfig = MediatorNodeParameterConfig(),
    mediator: MediatorConfig = MediatorConfig(),
    override val monitoring: NodeMonitoringConfig = NodeMonitoringConfig(),
    override val topology: TopologyConfig = TopologyConfig(),
) extends LocalNodeConfig
    with ConfigDefaults[DefaultPorts, MediatorNodeConfig]
    with UniformCantonConfigValidation {

  override def nodeTypeName: String = "mediator"

  override def clientAdminApi: ClientConfig = adminApi.clientConfig

  def toRemoteConfig: RemoteMediatorConfig = RemoteMediatorConfig(adminApi.clientConfig)

  def replicationEnabled: Boolean = replication.exists(_.isEnabled)

  override def withDefaults(
      ports: DefaultPorts,
      edition: CantonEdition,
  ): MediatorNodeConfig =
    this
      .focus(_.adminApi.internalPort)
      .modify(ports.mediatorAdminApiPort.setDefaultPort)
      .focus(_.replication)
      .modify(ReplicationConfig.withDefaultO(storage, _, edition))
}

object MediatorNodeConfig {
  implicit val mediatorNodeConfigCantonConfigValidator: CantonConfigValidator[MediatorNodeConfig] =
    CantonConfigValidatorDerivation[MediatorNodeConfig]
}

class MediatorNodeBootstrap(
    arguments: CantonNodeBootstrapCommonArguments[
      MediatorNodeConfig,
      MediatorNodeParameters,
      MediatorMetrics,
    ],
    protected val replicaManager: MediatorReplicaManager,
)(
    implicit executionContext: ExecutionContextIdlenessExecutorService,
    implicit val executionSequencerFactory: ExecutionSequencerFactory,
    scheduler: ScheduledExecutorService,
    actorSystem: ActorSystem,
) extends CantonNodeBootstrapImpl[
      MediatorNode,
      MediatorNodeConfig,
      MediatorNodeParameters,
      MediatorMetrics,
    ](arguments) {

  override protected def member(uid: UniqueIdentifier): Member = MediatorId(uid)
  override def metrics: BaseMetrics = arguments.metrics

  override protected def adminTokenConfig: AdminTokenConfig = config.adminApi.adminTokenConfig

  private val synchronizerTopologyManager = new SingleUseCell[SynchronizerTopologyManager]()

  override protected def sequencedTopologyStores: Seq[TopologyStore[SynchronizerStore]] =
    synchronizerTopologyManager.get.map(_.store).toList

  override protected def sequencedTopologyManagers: Seq[SynchronizerTopologyManager] =
    synchronizerTopologyManager.get.toList

  override protected def lookupTopologyClient(
      storeId: TopologyStoreId
  ): Option[SynchronizerTopologyClient] =
    storeId match {
      case SynchronizerStore(synchronizerId) =>
        replicaManager.mediatorRuntime
          .map(_.mediator.topologyClient)
          .filter(_.psid == synchronizerId)
      case _ => None
    }

  override protected lazy val lookupActivePSId: PSIdLookup =
    synchronizerId =>
      synchronizerTopologyManager.get
        .map(_.psid)
        .filter(_.logical == synchronizerId)

  private lazy val deferredSequencerClientHealth =
    MutableHealthComponent(loggerFactory, SequencerClient.healthName, timeouts)

  override protected def mkNodeHealthService(
      storage: Storage
  ): (DependenciesHealthService, LivenessHealthService) = {
    val readiness =
      DependenciesHealthService(
        "mediator",
        logger,
        timeouts,
        Seq(storage),
        softDependencies = Seq(deferredSequencerClientHealth),
      )

    val liveness = LivenessHealthService(
      logger,
      timeouts,
      fatalDependencies = Seq(deferredSequencerClientHealth),
    )
    (readiness, liveness)
  }

  override protected def bindNodeStatusService(): ServerServiceDefinition =
    MediatorStatusService.bindService(
      new GrpcMediatorStatusService(getNodeStatus, loggerFactory),
      executionContext,
    )

  private class WaitForMediatorToSynchronizerInit(
      storage: Storage,
      crypto: Crypto,
      adminServerRegistry: CantonMutableHandlerRegistry,
      adminTokenDispenser: CantonAdminTokenDispenser,
      mediatorId: MediatorId,
      authorizedTopologyManager: AuthorizedTopologyManager,
      healthService: DependenciesHealthService,
  ) extends BootstrapStageWithStorage[
        MediatorNode,
        StartupNode,
        (StaticSynchronizerParameters, PhysicalSynchronizerId),
      ](
        "wait-for-mediator-to-synchronizer-init",
        bootstrapStageCallback,
        storage,
        false, // this stage does not have auto-init
      )
      with GrpcMediatorInitializationService.Callback {

    override def getAdminToken: Option[String] = Some(adminTokenDispenser.getCurrentToken.secret)

    adminServerRegistry
      .addServiceU(
        MediatorInitializationServiceGrpc
          .bindService(
            new GrpcMediatorInitializationService(this, loggerFactory),
            executionContext,
          )
      )
    adminServerRegistry.addServiceU(
      ApiInfoServiceGrpc.bindService(
        new GrpcApiInfoService(CantonGrpcUtil.ApiName.AdminApi),
        executionContext,
      )
    )

    private val synchronizerConfigurationStore =
      MediatorSynchronizerConfigurationStore(storage, timeouts, loggerFactory)
    addCloseable(synchronizerConfigurationStore)
    addCloseable(deferredSequencerClientHealth)

    override protected def stageCompleted(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Option[(StaticSynchronizerParameters, PhysicalSynchronizerId)]] =
      OptionT(synchronizerConfigurationStore.fetchConfiguration()).map {
        mediatorSynchronizerConfiguration =>
          (
            mediatorSynchronizerConfiguration.synchronizerParameters,
            mediatorSynchronizerConfiguration.synchronizerId,
          )

      }.value

    override protected def buildNextStage(
        result: (
            StaticSynchronizerParameters,
            PhysicalSynchronizerId,
        )
    ): EitherT[FutureUnlessShutdown, String, StartupNode] = {
      val (staticSynchronizerParameters, psid) = result
      val synchronizerTopologyStore =
        TopologyStore(
          SynchronizerStore(psid),
          storage,
          staticSynchronizerParameters.protocolVersion,
          timeouts,
          loggerFactory,
        )
      addCloseable(synchronizerTopologyStore)

      EitherT.rightT(
        new StartupNode(
          storage,
          SynchronizerCrypto(crypto, staticSynchronizerParameters),
          adminServerRegistry,
          adminTokenDispenser,
          mediatorId,
          staticSynchronizerParameters,
          authorizedTopologyManager,
          synchronizerConfigurationStore,
          synchronizerTopologyStore,
          healthService,
        )
      )
    }

    override def waitingFor: Option[WaitingForExternalInput] = Some(
      WaitingForInitialization
    )

    override protected def autoCompleteStage(): EitherT[FutureUnlessShutdown, String, Option[
      (StaticSynchronizerParameters, PhysicalSynchronizerId)
    ]] =
      EitherT.rightT(None) // this stage doesn't have auto-init

    override def initialize(request: InitializeMediatorRequest)(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, String, InitializeMediatorResponse] =
      if (isInitialized) {
        logger.info(
          "Received a request to initialize an already initialized mediator. Skipping initialization!"
        )
        EitherT.pure(InitializeMediatorResponse())
      } else {
        val synchronizerAlias = SynchronizerAlias.tryCreate("synchronizer")
        val sequencerInfoLoader = createSequencerInfoLoader()
        completeWithExternalUS {
          logger.info(
            s"Assigning mediator to ${request.synchronizerId} via sequencers ${request.sequencerConnections}"
          )
          for {
            sequencerAggregatedInfo <- sequencerInfoLoader
              .loadAndAggregateSequencerEndpoints(
                synchronizerAlias,
                Some(request.synchronizerId),
                request.sequencerConnections,
                request.sequencerConnectionValidation,
              )
              .leftMap(error => s"Error loading sequencer endpoint information: $error")

            _ <- CryptoHandshakeValidator
              .validate(sequencerAggregatedInfo.staticSynchronizerParameters, cryptoConfig)
              .toEitherT[FutureUnlessShutdown]

            configToStore = MediatorSynchronizerConfiguration(
              request.synchronizerId,
              sequencerAggregatedInfo.staticSynchronizerParameters,
              request.sequencerConnections,
            )
            _ <- EitherT.right(synchronizerConfigurationStore.saveConfiguration(configToStore))
          } yield (
            sequencerAggregatedInfo.staticSynchronizerParameters,
            request.synchronizerId,
          )
        }.map(_ => InitializeMediatorResponse())
      }

  }

  private class StartupNode(
      storage: Storage,
      crypto: SynchronizerCrypto,
      adminServerRegistry: CantonMutableHandlerRegistry,
      adminTokenDispenser: CantonAdminTokenDispenser,
      mediatorId: MediatorId,
      staticSynchronizerParameters: StaticSynchronizerParameters,
      authorizedTopologyManager: AuthorizedTopologyManager,
      synchronizerConfigurationStore: MediatorSynchronizerConfigurationStore,
      synchronizerTopologyStore: TopologyStore[SynchronizerStore],
      healthService: DependenciesHealthService,
  ) extends BootstrapStage[MediatorNode, RunningNode[MediatorNode]](
        description = "Startup mediator node",
        bootstrapStageCallback,
      )
      with HasCloseContext {

    private val synchronizerLoggerFactory =
      loggerFactory.append("synchronizerId", synchronizerTopologyStore.storeId.psid.toString)

    override protected def attempt()(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, String, Option[RunningNode[MediatorNode]]] = {

      def createSynchronizerOutboxFactory(
          synchronizerTopologyManager: SynchronizerTopologyManager
      ) =
        new SynchronizerOutboxFactory(
          memberId = mediatorId,
          authorizedTopologyManager = authorizedTopologyManager,
          synchronizerTopologyManager = synchronizerTopologyManager,
          crypto = crypto,
          topologyConfig = config.topology,
          timeouts = timeouts,
          loggerFactory = synchronizerLoggerFactory,
          futureSupervisor = arguments.futureSupervisor,
        )

      def createSynchronizerTopologyManager(): Either[String, SynchronizerTopologyManager] = {
        val outboxQueue = new SynchronizerOutboxQueue(loggerFactory)

        val topologyManager = new SynchronizerTopologyManager(
          nodeId = mediatorId.uid,
          clock = clock,
          crypto = crypto,
          staticSynchronizerParameters = staticSynchronizerParameters,
          store = synchronizerTopologyStore,
          outboxQueue = outboxQueue,
          exitOnFatalFailures = parameters.exitOnFatalFailures,
          timeouts = timeouts,
          futureSupervisor = futureSupervisor,
          loggerFactory = loggerFactory,
        )

        if (synchronizerTopologyManager.putIfAbsent(topologyManager).nonEmpty)
          Left("synchronizerTopologyManager shouldn't have been set before")
        else
          topologyManager.asRight

      }

      synchronizeWithClosing("starting up mediator node") {
        for {
          // safety check. normally we shouldn't even be able to get to this point without a configuration,
          // because the previous bootstrap stage only completes if it finds a configuration.
          _ <- fetchSynchronizerConfigOrFail(synchronizerConfigurationStore)

          synchronizerTopologyManager <- EitherT.fromEither[FutureUnlessShutdown](
            createSynchronizerTopologyManager()
          )
          synchronizerOutboxFactory = createSynchronizerOutboxFactory(synchronizerTopologyManager)

          indexedStringStore = IndexedStringStore.create(
            storage,
            parameters.cachingConfigs.indexedStrings,
            timeouts,
            synchronizerLoggerFactory,
          )
          _ = addCloseable(indexedStringStore)

          _ <- EitherT.right[String](
            replicaManager.setup(
              adminServerRegistry,
              () =>
                mkMediatorRuntime(
                  mediatorId,
                  synchronizerTopologyStore.storeId.psid,
                  indexedStringStore,
                  synchronizerConfigurationStore,
                  storage,
                  crypto,
                  adminServerRegistry,
                  staticSynchronizerParameters,
                  synchronizerTopologyStore,
                  topologyManagerStatus = TopologyManagerStatus
                    .combined(authorizedTopologyManager, synchronizerTopologyManager),
                  synchronizerOutboxFactory,
                ),
              storage.isActive,
            )
          )
        } yield {
          val node = new MediatorNode(
            arguments.config,
            mediatorId,
            synchronizerTopologyStore.storeId.psid,
            replicaManager,
            storage,
            clock,
            adminTokenDispenser,
            synchronizerLoggerFactory,
            healthData = healthService.dependencies.map(_.toComponentStatus),
          )
          addCloseable(node)
          Some(new RunningNode(bootstrapStageCallback, node))
        }
      }
    }
  }

  private def createSequencerInfoLoader() =
    new SequencerInfoLoader(
      timeouts = timeouts,
      traceContextPropagation = parameters.tracing.propagation,
      clientProtocolVersions =
        if (parameters.alphaVersionSupport) ProtocolVersion.supported
        else
          // TODO(#15561) Remove NonEmpty construct once stableAndSupported is NonEmpty again
          NonEmpty
            .from(ProtocolVersion.stable)
            .getOrElse(sys.error("no protocol version is considered stable in this release")),
      minimumProtocolVersion = Some(ProtocolVersion.minimum),
      dontWarnOnDeprecatedPV = parameters.dontWarnOnDeprecatedPV,
      loggerFactory = loggerFactory,
    )

  private def fetchSynchronizerConfigOrFail(
      configStore: MediatorSynchronizerConfigurationStore
  ): EitherT[FutureUnlessShutdown, String, MediatorSynchronizerConfiguration] =
    EitherT.fromOptionF(
      fopt = configStore.fetchConfiguration(),
      ifNone =
        s"Mediator synchronizer config has not been set. Must first be initialized by the synchronizer in order to start.",
    )

  private def mkMediatorRuntime(
      mediatorId: MediatorId,
      synchronizerId: PhysicalSynchronizerId,
      indexedStringStore: IndexedStringStore,
      synchronizerConfigurationStore: MediatorSynchronizerConfigurationStore,
      storage: Storage,
      crypto: SynchronizerCrypto,
      adminServerRegistry: CantonMutableHandlerRegistry,
      staticSynchronizerParameters: StaticSynchronizerParameters,
      synchronizerTopologyStore: TopologyStore[SynchronizerStore],
      topologyManagerStatus: TopologyManagerStatus,
      synchronizerOutboxFactory: SynchronizerOutboxFactory,
  ): EitherT[FutureUnlessShutdown, String, MediatorRuntime] = {
    val synchronizerLoggerFactory = loggerFactory.append("synchronizerId", synchronizerId.toString)
    val synchronizerAlias = SynchronizerAlias(
      synchronizerId.uid.toLengthLimitedString
    )
    val sequencerInfoLoader = createSequencerInfoLoader()
    def getSequencerConnectionFromStore: FutureUnlessShutdown[Option[SequencerConnections]] =
      synchronizerConfigurationStore.fetchConfiguration().map(_.map(_.sequencerConnections))

    val connectionPoolFactory = new GrpcSequencerConnectionXPoolFactory(
      clientProtocolVersions = ProtocolVersionCompatibility.supportedProtocols(parameters),
      minimumProtocolVersion = Some(ProtocolVersion.minimum),
      authConfig = parameters.sequencerClient.authToken,
      member = mediatorId,
      clock = clock,
      crypto = crypto.crypto,
      seedForRandomnessO = arguments.testingConfig.sequencerTransportSeed,
      futureSupervisor = futureSupervisor,
      timeouts = timeouts,
      loggerFactory = loggerFactory,
    )

    val useNewConnectionPool = parameters.sequencerClient.useNewConnectionPool

    val connectionPoolRef = new AtomicReference[Option[SequencerConnectionXPool]](None)

    val mediatorRuntimeET = for {
      physicalSynchronizerIdx <- EitherT
        .right(IndexedPhysicalSynchronizer.indexed(indexedStringStore)(synchronizerId))

      sequencedEventStore = SequencedEventStore(
        storage,
        physicalSynchronizerIdx,
        timeouts,
        synchronizerLoggerFactory,
      )
      sendTrackerStore = SendTrackerStore()
      sequencerCounterTrackerStore = SequencerCounterTrackerStore(
        storage,
        physicalSynchronizerIdx,
        timeouts,
        synchronizerLoggerFactory,
      )
      topologyProcessorAndClient <-
        EitherT
          .right(
            TopologyTransactionProcessor.createProcessorAndClientForSynchronizer(
              synchronizerTopologyStore,
              synchronizerPredecessor = None,
              crypto.pureCrypto,
              arguments.parameterConfig,
              arguments.clock,
              arguments.futureSupervisor,
              synchronizerLoggerFactory,
            )()
          )
      (topologyProcessor, topologyClient) = topologyProcessorAndClient
      _ = ips.add(topologyClient)

      // Session signing keys are used only if they are configured in Canton's configuration file.
      syncCryptoWithOptionalSessionKeys = SynchronizerCryptoClient.createWithOptionalSessionKeys(
        mediatorId,
        synchronizerId,
        topologyClient,
        staticSynchronizerParameters,
        crypto,
        cryptoConfig,
        parameters.batchingConfig.parallelism,
        parameters.cachingConfigs.publicKeyConversionCache,
        timeouts,
        futureSupervisor,
        synchronizerLoggerFactory,
      )
      sequencerClientFactory = SequencerClientFactory(
        synchronizerId,
        syncCryptoWithOptionalSessionKeys,
        crypto,
        parameters.sequencerClient,
        parameters.tracing.propagation,
        arguments.testingConfig,
        staticSynchronizerParameters,
        timeouts,
        clock,
        topologyClient,
        futureSupervisor,
        member =>
          MediatorNodeBootstrap.recordSequencerInteractions
            .get()
            .lift(member)
            .map(Synchronizer.setMemberRecordingPath(member)),
        member =>
          MediatorNodeBootstrap.replaySequencerConfig
            .get()
            .lift(member)
            .map(Synchronizer.defaultReplayPath(member)),
        arguments.metrics.sequencerClient,
        parameters.loggingConfig,
        parameters.exitOnFatalFailures,
        synchronizerLoggerFactory,
        ProtocolVersionCompatibility.supportedProtocols(parameters),
      )

      // we wait here until the sequencer becomes active. this allows to reconfigure the
      // sequencer client address
      connectionPoolAndInfo <-
        if (useNewConnectionPool)
          GrpcSequencerConnectionService.waitUntilSequencerConnectionIsValidWithPool(
            connectionPoolFactory,
            parameters.tracing,
            this,
            getSequencerConnectionFromStore,
          )
        else
          for {
            info <- GrpcSequencerConnectionService.waitUntilSequencerConnectionIsValid(
              sequencerInfoLoader,
              this,
              getSequencerConnectionFromStore,
            )
            dummyPool <- EitherT.fromEither[FutureUnlessShutdown](
              connectionPoolFactory
                .createFromOldConfig(
                  info.sequencerConnections,
                  expectedPSIdO = None,
                  parameters.tracing,
                )
                .leftMap(error => error.toString)
            )
          } yield (dummyPool, info)
      (connectionPool, info) = connectionPoolAndInfo
      _ = connectionPoolRef.set(Some(connectionPool))

      sequencerClient <- sequencerClientFactory
        .create(
          mediatorId,
          sequencedEventStore,
          sendTrackerStore,
          RequestSigner(
            syncCryptoWithOptionalSessionKeys,
            staticSynchronizerParameters.protocolVersion,
            loggerFactory,
          ),
          info.sequencerConnections,
          synchronizerPredecessor = None,
          info.expectedSequencersO,
          connectionPool,
        )

      sequencerClientRef =
        GrpcSequencerConnectionService
          .setup[MediatorSynchronizerConfiguration](mediatorId, useNewConnectionPool)(
            adminServerRegistry,
            () => synchronizerConfigurationStore.fetchConfiguration(),
            config => synchronizerConfigurationStore.saveConfiguration(config),
            Lens[MediatorSynchronizerConfiguration, SequencerConnections](_.sequencerConnections)(
              connection => conf => conf.copy(sequencerConnections = connection)
            ),
            RequestSigner(
              syncCryptoWithOptionalSessionKeys,
              staticSynchronizerParameters.protocolVersion,
              loggerFactory,
            ),
            sequencerClientFactory,
            sequencerInfoLoader,
            synchronizerAlias,
            synchronizerId,
            sequencerClient,
            loggerFactory,
          )

      _ = sequencerClientRef.set(sequencerClient)
      _ = deferredSequencerClientHealth.set(sequencerClient.healthComponent)

      timeTracker = SynchronizerTimeTracker(
        config.timeTracker,
        clock,
        sequencerClient,
        timeouts,
        loggerFactory,
      )
      _ = topologyClient.setSynchronizerTimeTracker(timeTracker)

      // TODO(i12076): Request topology information from all sequencers and reconcile
      _ <- MonadUtil.unlessM(
        EitherT.right[String](synchronizerConfigurationStore.isTopologyInitialized())
      )(
        new StoreBasedSynchronizerTopologyInitializationCallback(
          mediatorId
        ).callback(
          new InitialTopologySnapshotValidator(
            crypto.pureCrypto,
            synchronizerTopologyStore,
            arguments.parameterConfig.processingTimeouts,
            synchronizerLoggerFactory,
          ),
          topologyClient,
          sequencerClient,
          staticSynchronizerParameters.protocolVersion,
        ).semiflatMap(_ => synchronizerConfigurationStore.setTopologyInitialized())
      )

      mediatorRuntime <- MediatorRuntimeFactory.create(
        mediatorId,
        storage,
        sequencerCounterTrackerStore,
        sequencedEventStore,
        sequencerClient,
        syncCryptoWithOptionalSessionKeys,
        topologyClient,
        topologyProcessor,
        topologyManagerStatus,
        synchronizerOutboxFactory,
        timeTracker,
        parameters,
        clock,
        arguments.metrics,
        config.mediator,
        synchronizerLoggerFactory,
      )
      _ <- mediatorRuntime.start()
    } yield mediatorRuntime

    mediatorRuntimeET.thereafterF {
      case Success(Outcome(Right(_))) => Future.unit
      // In case of error or exception, ensure the pool is closed
      case _ => Future.successful(connectionPoolRef.get.foreach(_.close()))
    }

  }

  override protected def customNodeStages(
      storage: Storage,
      crypto: Crypto,
      adminServerRegistry: CantonMutableHandlerRegistry,
      adminTokenDispenser: CantonAdminTokenDispenser,
      nodeId: UniqueIdentifier,
      authorizedTopologyManager: AuthorizedTopologyManager,
      healthServer: GrpcHealthReporter,
      healthService: DependenciesHealthService,
  ): BootstrapStageOrLeaf[MediatorNode] =
    new WaitForMediatorToSynchronizerInit(
      storage,
      crypto,
      adminServerRegistry,
      adminTokenDispenser,
      MediatorId(nodeId),
      authorizedTopologyManager,
      healthService,
    )

  override protected def onClosed(): Unit =
    super.onClosed()
}

object MediatorNodeBootstrap {
  val LoggerFactoryKeyName: String = "mediator"

  /** If the function maps `member` to `recordConfig`, the sequencer client for `member` will record
    * all sends requested and events received to the directory specified by the recording config. A
    * new recording starts whenever the synchronizer is restarted.
    */
  @VisibleForTesting
  val recordSequencerInteractions: AtomicReference[PartialFunction[Member, RecordingConfig]] =
    new AtomicReference(PartialFunction.empty)

  /** If the function maps `member` to `path`, the sequencer client for `member` will replay events
    * from `path` instead of pulling them from the sequencer. A new replay starts whenever the
    * synchronizer is restarted.
    */
  @VisibleForTesting
  val replaySequencerConfig: AtomicReference[PartialFunction[Member, ReplayConfig]] =
    new AtomicReference(PartialFunction.empty)
}

class MediatorNode(
    config: MediatorNodeConfig,
    mediatorId: MediatorId,
    psid: PhysicalSynchronizerId,
    protected[canton] val replicaManager: MediatorReplicaManager,
    storage: Storage,
    override val clock: Clock,
    override val adminTokenDispenser: CantonAdminTokenDispenser,
    override val loggerFactory: NamedLoggerFactory,
    healthData: => Seq[ComponentStatus],
) extends CantonNode
    with NamedLogging
    with HasUptime {

  override type Status = MediatorNodeStatus

  def isActive: Boolean = replicaManager.isActive

  def status: MediatorNodeStatus = {
    val ports = Map("admin" -> config.adminApi.port)

    MediatorNodeStatus(
      mediatorId.uid,
      psid,
      uptime(),
      ports,
      replicaManager.isActive,
      replicaManager.getTopologyQueueStatus,
      healthData,
      ReleaseVersion.current,
    )
  }

  override def close(): Unit =
    LifeCycle.close(
      replicaManager,
      storage,
    )(logger)
}
