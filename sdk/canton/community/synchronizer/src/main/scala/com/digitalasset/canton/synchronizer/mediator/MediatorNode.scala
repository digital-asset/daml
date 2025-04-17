// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.mediator

import cats.data.{EitherT, OptionT}
import cats.syntax.either.*
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.admin.mediator.v30.MediatorStatusServiceGrpc.MediatorStatusService
import com.digitalasset.canton.auth.CantonAdminToken
import com.digitalasset.canton.common.sequencer.grpc.SequencerInfoLoader
import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.config.*
import com.digitalasset.canton.config.manual.CantonConfigValidatorDerivation
import com.digitalasset.canton.connection.GrpcApiInfoService
import com.digitalasset.canton.connection.v30.ApiInfoServiceGrpc
import com.digitalasset.canton.crypto.{
  Crypto,
  CryptoHandshakeValidator,
  SynchronizerCryptoClient,
  SynchronizerCryptoPureApi,
}
import com.digitalasset.canton.environment.*
import com.digitalasset.canton.health.*
import com.digitalasset.canton.health.admin.data.{WaitingForExternalInput, WaitingForInitialization}
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, HasCloseContext, LifeCycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.mediator.admin.v30.MediatorInitializationServiceGrpc
import com.digitalasset.canton.networking.grpc.{CantonGrpcUtil, CantonMutableHandlerRegistry}
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.sequencing.SequencerConnections
import com.digitalasset.canton.sequencing.client.{
  RecordingConfig,
  ReplayConfig,
  RequestSigner,
  SequencerClient,
  SequencerClientConfig,
  SequencerClientFactory,
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
import com.digitalasset.canton.topology.client.SynchronizerTopologyClient
import com.digitalasset.canton.topology.processing.{
  InitialTopologySnapshotValidator,
  SequencedTime,
  TopologyTransactionProcessor,
}
import com.digitalasset.canton.topology.store.TopologyStoreId.SynchronizerStore
import com.digitalasset.canton.topology.store.{TopologyStore, TopologyStoreId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{EitherTUtil, SingleUseCell}
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

/** Various parameters for non-standard mediator settings
  *
  * @param dontWarnOnDeprecatedPV
  *   if true, then this mediator will not emit a warning when connecting to a sequencer using a
  *   deprecated protocol version.
  */
final case class MediatorNodeParameterConfig(
    override val sessionSigningKeys: SessionSigningKeysConfig = SessionSigningKeysConfig.disabled,
    override val alphaVersionSupport: Boolean = false,
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

  override protected def adminTokenConfig: Option[String] = config.adminApi.adminToken

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
          .filter(_.synchronizerId == synchronizerId)
      case _ => None
    }

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
      adminToken: CantonAdminToken,
      mediatorId: MediatorId,
      authorizedTopologyManager: AuthorizedTopologyManager,
      healthService: DependenciesHealthService,
  ) extends BootstrapStageWithStorage[
        MediatorNode,
        StartupNode,
        (StaticSynchronizerParameters, SynchronizerId),
      ](
        "wait-for-mediator-to-synchronizer-init",
        bootstrapStageCallback,
        storage,
        false, // this stage does not have auto-init
      )
      with GrpcMediatorInitializationService.Callback {

    override def getAdminToken: Option[String] = Some(adminToken.secret)

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
    ): FutureUnlessShutdown[Option[(StaticSynchronizerParameters, SynchronizerId)]] =
      OptionT(synchronizerConfigurationStore.fetchConfiguration).map {
        mediatorSynchronizerConfiguration =>
          (
            mediatorSynchronizerConfiguration.synchronizerParameters,
            mediatorSynchronizerConfiguration.synchronizerId,
          )

      }.value

    override protected def buildNextStage(
        result: (
            StaticSynchronizerParameters,
            SynchronizerId,
        )
    ): EitherT[FutureUnlessShutdown, String, StartupNode] = {
      val (staticSynchronizerParameters, synchronizerId) = result
      val synchronizerTopologyStore =
        TopologyStore(
          SynchronizerStore(synchronizerId),
          storage,
          staticSynchronizerParameters.protocolVersion,
          timeouts,
          loggerFactory,
        )
      addCloseable(synchronizerTopologyStore)

      EitherT.rightT(
        new StartupNode(
          storage,
          crypto,
          adminServerRegistry,
          adminToken,
          mediatorId,
          staticSynchronizerParameters,
          authorizedTopologyManager,
          synchronizerId,
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
      (StaticSynchronizerParameters, SynchronizerId)
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
          } yield (sequencerAggregatedInfo.staticSynchronizerParameters, request.synchronizerId)
        }.map(_ => InitializeMediatorResponse())
      }

  }

  private class StartupNode(
      storage: Storage,
      crypto: Crypto,
      adminServerRegistry: CantonMutableHandlerRegistry,
      adminToken: CantonAdminToken,
      mediatorId: MediatorId,
      staticSynchronizerParameters: StaticSynchronizerParameters,
      authorizedTopologyManager: AuthorizedTopologyManager,
      synchronizerId: SynchronizerId,
      synchronizerConfigurationStore: MediatorSynchronizerConfigurationStore,
      synchronizerTopologyStore: TopologyStore[SynchronizerStore],
      healthService: DependenciesHealthService,
  ) extends BootstrapStage[MediatorNode, RunningNode[MediatorNode]](
        description = "Startup mediator node",
        bootstrapStageCallback,
      )
      with HasCloseContext {

    private val synchronizerLoggerFactory =
      loggerFactory.append("synchronizerId", synchronizerId.toString)

    override protected def attempt()(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, String, Option[RunningNode[MediatorNode]]] = {

      def createSynchronizerOutboxFactory(
          synchronizerTopologyManager: SynchronizerTopologyManager
      ) =
        new SynchronizerOutboxFactory(
          synchronizerId = synchronizerId,
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

      val fetchConfig: () => FutureUnlessShutdown[Option[MediatorSynchronizerConfiguration]] = () =>
        synchronizerConfigurationStore.fetchConfiguration

      val saveConfig: MediatorSynchronizerConfiguration => FutureUnlessShutdown[Unit] =
        synchronizerConfigurationStore.saveConfiguration

      performUnlessClosingEitherUSF("starting up mediator node") {
        for {
          synchronizerConfig <- OptionT(fetchConfig()).toRight(
            s"Mediator synchronizer config has not been set. Must first be initialized by the synchronizer in order to start."
          )

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
                  synchronizerConfig,
                  indexedStringStore,
                  fetchConfig,
                  saveConfig,
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
            synchronizerId,
            replicaManager,
            storage,
            clock,
            adminToken,
            synchronizerLoggerFactory,
            healthData = healthService.dependencies.map(_.toComponentStatus),
            staticSynchronizerParameters.protocolVersion,
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
          ProtocolVersion.stable,
      minimumProtocolVersion = Some(ProtocolVersion.minimum),
      dontWarnOnDeprecatedPV = parameters.dontWarnOnDeprecatedPV,
      loggerFactory = loggerFactory,
    )

  private def mkMediatorRuntime(
      mediatorId: MediatorId,
      synchronizerConfig: MediatorSynchronizerConfiguration,
      indexedStringStore: IndexedStringStore,
      fetchConfig: () => FutureUnlessShutdown[Option[MediatorSynchronizerConfiguration]],
      saveConfig: MediatorSynchronizerConfiguration => FutureUnlessShutdown[Unit],
      storage: Storage,
      crypto: Crypto,
      adminServerRegistry: CantonMutableHandlerRegistry,
      staticSynchronizerParameters: StaticSynchronizerParameters,
      synchronizerTopologyStore: TopologyStore[SynchronizerStore],
      topologyManagerStatus: TopologyManagerStatus,
      synchronizerOutboxFactory: SynchronizerOutboxFactory,
  ): EitherT[FutureUnlessShutdown, String, MediatorRuntime] = {
    val synchronizerId = synchronizerConfig.synchronizerId
    val synchronizerLoggerFactory = loggerFactory.append("synchronizerId", synchronizerId.toString)
    val synchronizerAlias = SynchronizerAlias(
      synchronizerConfig.synchronizerId.uid.toLengthLimitedString
    )
    val sequencerInfoLoader = createSequencerInfoLoader()
    def getSequencerConnectionFromStore: FutureUnlessShutdown[Option[SequencerConnections]] =
      fetchConfig().map(_.map(_.sequencerConnections))

    for {
      indexedSynchronizerId <- EitherT
        .right(IndexedSynchronizer.indexed(indexedStringStore)(synchronizerId))
      sequencedEventStore = SequencedEventStore(
        storage,
        indexedSynchronizerId,
        synchronizerConfig.synchronizerParameters.protocolVersion,
        timeouts,
        synchronizerLoggerFactory,
      )
      sendTrackerStore = SendTrackerStore(storage)
      sequencerCounterTrackerStore = SequencerCounterTrackerStore(
        storage,
        indexedSynchronizerId,
        timeouts,
        synchronizerLoggerFactory,
      )
      topologyProcessorAndClient <-
        EitherT
          .right(
            TopologyTransactionProcessor.createProcessorAndClientForSynchronizer(
              synchronizerTopologyStore,
              synchronizerId,
              new SynchronizerCryptoPureApi(staticSynchronizerParameters, crypto.pureCrypto),
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
        new SynchronizerCryptoPureApi(staticSynchronizerParameters, crypto.pureCrypto),
        parameters.sessionSigningKeys,
        parameters.batchingConfig.parallelism,
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
        synchronizerConfig.synchronizerParameters,
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
      info <- GrpcSequencerConnectionService.waitUntilSequencerConnectionIsValid(
        sequencerInfoLoader,
        this,
        getSequencerConnectionFromStore,
      )

      sequencerClient <- sequencerClientFactory
        .create(
          mediatorId,
          sequencedEventStore,
          sendTrackerStore,
          RequestSigner(
            syncCryptoWithOptionalSessionKeys,
            synchronizerConfig.synchronizerParameters.protocolVersion,
            loggerFactory,
          ),
          info.sequencerConnections,
          info.expectedSequencers,
        )

      sequencerClientRef =
        GrpcSequencerConnectionService.setup[MediatorSynchronizerConfiguration](mediatorId)(
          adminServerRegistry,
          fetchConfig,
          saveConfig,
          Lens[MediatorSynchronizerConfiguration, SequencerConnections](_.sequencerConnections)(
            connection => conf => conf.copy(sequencerConnections = connection)
          ),
          RequestSigner(
            syncCryptoWithOptionalSessionKeys,
            synchronizerConfig.synchronizerParameters.protocolVersion,
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

      topologyStoreIsEmpty <- EitherT.right(
        synchronizerTopologyStore
          .maxTimestamp(SequencedTime.MaxValue, includeRejected = true)
          .map(_.isEmpty)
      )
      // TODO(i12076): Request topology information from all sequencers and reconcile
      _ <-
        if (topologyStoreIsEmpty) {
          new StoreBasedSynchronizerTopologyInitializationCallback(
            mediatorId
          ).callback(
            new InitialTopologySnapshotValidator(
              staticSynchronizerParameters.protocolVersion,
              new SynchronizerCryptoPureApi(staticSynchronizerParameters, crypto.pureCrypto),
              synchronizerTopologyStore,
              arguments.parameterConfig.processingTimeouts,
              synchronizerLoggerFactory,
            ),
            topologyClient,
            sequencerClient,
            synchronizerConfig.synchronizerParameters.protocolVersion,
          )
        } else EitherTUtil.unitUS

      mediatorRuntime <- MediatorRuntimeFactory.create(
        mediatorId,
        synchronizerId,
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
        synchronizerConfig.synchronizerParameters.protocolVersion,
        clock,
        arguments.metrics,
        config.mediator,
        synchronizerLoggerFactory,
      )
      _ <- mediatorRuntime.start()
    } yield mediatorRuntime
  }

  override protected def customNodeStages(
      storage: Storage,
      crypto: Crypto,
      adminServerRegistry: CantonMutableHandlerRegistry,
      adminToken: CantonAdminToken,
      nodeId: UniqueIdentifier,
      authorizedTopologyManager: AuthorizedTopologyManager,
      healthServer: GrpcHealthReporter,
      healthService: DependenciesHealthService,
  ): BootstrapStageOrLeaf[MediatorNode] =
    new WaitForMediatorToSynchronizerInit(
      storage,
      crypto,
      adminServerRegistry,
      adminToken,
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
    synchronizerId: SynchronizerId,
    protected[canton] val replicaManager: MediatorReplicaManager,
    storage: Storage,
    override val clock: Clock,
    override val adminToken: CantonAdminToken,
    override val loggerFactory: NamedLoggerFactory,
    healthData: => Seq[ComponentStatus],
    protocolVersion: ProtocolVersion,
) extends CantonNode
    with NamedLogging
    with HasUptime {

  override type Status = MediatorNodeStatus

  def isActive: Boolean = replicaManager.isActive

  def status: MediatorNodeStatus = {
    val ports = Map("admin" -> config.adminApi.port)

    MediatorNodeStatus(
      mediatorId.uid,
      synchronizerId,
      uptime(),
      ports,
      replicaManager.isActive,
      replicaManager.getTopologyQueueStatus,
      healthData,
      ReleaseVersion.current,
      protocolVersion,
    )
  }

  override def close(): Unit =
    LifeCycle.close(
      replicaManager,
      storage,
    )(logger)

}
