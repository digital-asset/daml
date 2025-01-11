// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.mediator

import cats.data.{EitherT, OptionT}
import cats.syntax.either.*
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.admin.mediator.v30.MediatorStatusServiceGrpc.MediatorStatusService
import com.digitalasset.canton.auth.CantonAdminToken
import com.digitalasset.canton.common.sequencer.grpc.SequencerInfoLoader
import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.config.*
import com.digitalasset.canton.connection.GrpcApiInfoService
import com.digitalasset.canton.connection.v30.ApiInfoServiceGrpc
import com.digitalasset.canton.crypto.{
  Crypto,
  CryptoHandshakeValidator,
  SynchronizerCryptoPureApi,
  SynchronizerSyncCryptoClient,
}
import com.digitalasset.canton.data.CantonTimestamp
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
  MediatorDomainConfiguration,
  MediatorDomainConfigurationStore,
}
import com.digitalasset.canton.synchronizer.metrics.MediatorMetrics
import com.digitalasset.canton.synchronizer.service.GrpcSequencerConnectionService
import com.digitalasset.canton.time.{Clock, HasUptime, SynchronizerTimeTracker}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.SynchronizerTopologyClient
import com.digitalasset.canton.topology.processing.{
  InitialTopologySnapshotValidator,
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
import io.grpc.ServerServiceDefinition
import monocle.Lens
import monocle.macros.syntax.lens.*
import org.apache.pekko.actor.ActorSystem

import java.util.concurrent.ScheduledExecutorService

abstract class MediatorNodeConfigCommon(
    val adminApi: AdminServerConfig,
    val storage: StorageConfig,
    val crypto: CryptoConfig,
    val init: InitConfig,
    val timeTracker: SynchronizerTimeTrackerConfig,
    val sequencerClient: SequencerClientConfig,
    val caching: CachingConfigs,
    val parameters: MediatorNodeParameterConfig,
    val mediator: MediatorConfig,
    val monitoring: NodeMonitoringConfig,
) extends LocalNodeConfig {

  override def clientAdminApi: ClientConfig = adminApi.clientConfig

  def toRemoteConfig: RemoteMediatorConfig = RemoteMediatorConfig(adminApi.clientConfig)

  def replicationEnabled: Boolean
}

/** Various parameters for non-standard mediator settings
  *
  * @param dontWarnOnDeprecatedPV if true, then this mediator will not emit a warning when connecting to a sequencer using a deprecated protocol version.
  */
final case class MediatorNodeParameterConfig(
    override val sessionSigningKeys: SessionSigningKeysConfig = SessionSigningKeysConfig.disabled,
    // TODO(i15561): Revert back to `false` once there is a stable Daml 3 protocol version
    override val alphaVersionSupport: Boolean = true,
    override val betaVersionSupport: Boolean = false,
    override val dontWarnOnDeprecatedPV: Boolean = false,
    override val batching: BatchingConfig = BatchingConfig(),
    override val caching: CachingConfigs = CachingConfigs(),
    override val watchdog: Option[WatchdogConfig] = None,
) extends ProtocolConfig
    with LocalNodeParametersConfig

final case class MediatorNodeParameters(
    general: CantonNodeParameters.General,
    protocol: CantonNodeParameters.Protocol,
) extends CantonNodeParameters
    with HasGeneralCantonNodeParameters
    with HasProtocolCantonNodeParameters

final case class RemoteMediatorConfig(
    adminApi: ClientConfig,
    token: Option[String] = None,
) extends NodeConfig {
  override def clientAdminApi: ClientConfig = adminApi
}

/** Community Mediator Node configuration that defaults to auto-init
  */
final case class CommunityMediatorNodeConfig(
    override val adminApi: AdminServerConfig = AdminServerConfig(),
    override val storage: StorageConfig = StorageConfig.Memory(),
    override val crypto: CommunityCryptoConfig = CommunityCryptoConfig(),
    override val init: InitConfig = InitConfig(identity = Some(InitConfigBase.Identity())),
    override val timeTracker: SynchronizerTimeTrackerConfig = SynchronizerTimeTrackerConfig(),
    override val sequencerClient: SequencerClientConfig = SequencerClientConfig(),
    override val caching: CachingConfigs = CachingConfigs(),
    override val parameters: MediatorNodeParameterConfig = MediatorNodeParameterConfig(),
    override val mediator: MediatorConfig = MediatorConfig(),
    override val monitoring: NodeMonitoringConfig = NodeMonitoringConfig(),
    override val topology: TopologyConfig = TopologyConfig(),
) extends MediatorNodeConfigCommon(
      adminApi,
      storage,
      crypto,
      init,
      timeTracker,
      sequencerClient,
      caching,
      parameters,
      mediator,
      monitoring,
    )
    with ConfigDefaults[DefaultPorts, CommunityMediatorNodeConfig] {

  override val nodeTypeName: String = "mediator"

  override def replicationEnabled: Boolean = false

  override def withDefaults(ports: DefaultPorts): CommunityMediatorNodeConfig =
    this
      .focus(_.adminApi.internalPort)
      .modify(ports.mediatorAdminApiPort.setDefaultPort)
}

class MediatorNodeBootstrap(
    arguments: CantonNodeBootstrapCommonArguments[
      MediatorNodeConfigCommon,
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
      MediatorNodeConfigCommon,
      MediatorNodeParameters,
      MediatorMetrics,
    ](arguments) {

  override protected def member(uid: UniqueIdentifier): Member = MediatorId(uid)

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
      case SynchronizerStore(synchronizerId, _) =>
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
        config.init.autoInit,
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
      MediatorDomainConfigurationStore(storage, timeouts, loggerFactory)
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

            configToStore = MediatorDomainConfiguration(
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
      synchronizerConfigurationStore: MediatorDomainConfigurationStore,
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

      val fetchConfig: () => FutureUnlessShutdown[Option[MediatorDomainConfiguration]] = () =>
        synchronizerConfigurationStore.fetchConfiguration

      val saveConfig: MediatorDomainConfiguration => FutureUnlessShutdown[Unit] =
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
            parameterConfig.cachingConfigs.indexedStrings,
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
        if (parameterConfig.alphaVersionSupport) ProtocolVersion.supported
        else
          // TODO(#15561) Remove NonEmpty construct once stableAndSupported is NonEmpty again
          NonEmpty
            .from(ProtocolVersion.stable)
            .getOrElse(sys.error("no protocol version is considered stable in this release")),
      minimumProtocolVersion = Some(ProtocolVersion.minimum),
      dontWarnOnDeprecatedPV = parameterConfig.dontWarnOnDeprecatedPV,
      loggerFactory = loggerFactory,
    )

  private def mkMediatorRuntime(
      mediatorId: MediatorId,
      synchronizerConfig: MediatorDomainConfiguration,
      indexedStringStore: IndexedStringStore,
      fetchConfig: () => FutureUnlessShutdown[Option[MediatorDomainConfiguration]],
      saveConfig: MediatorDomainConfiguration => FutureUnlessShutdown[Unit],
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
      syncCryptoApi = new SynchronizerSyncCryptoClient(
        mediatorId,
        synchronizerId,
        topologyClient,
        crypto,
        arguments.parameterConfig.sessionSigningKeys,
        staticSynchronizerParameters,
        timeouts,
        futureSupervisor,
        synchronizerLoggerFactory,
      )
      sequencerClientFactory = SequencerClientFactory(
        synchronizerId,
        syncCryptoApi,
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
          Synchronizer.recordSequencerInteractions
            .get()
            .lift(member)
            .map(Synchronizer.setMemberRecordingPath(member)),
        member =>
          Synchronizer.replaySequencerConfig
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
        futureSupervisor,
        getSequencerConnectionFromStore,
      )

      sequencerClient <- sequencerClientFactory
        .create(
          mediatorId,
          sequencedEventStore,
          sendTrackerStore,
          RequestSigner(
            syncCryptoApi,
            synchronizerConfig.synchronizerParameters.protocolVersion,
            loggerFactory,
          ),
          info.sequencerConnections,
          info.expectedSequencers,
        )

      sequencerClientRef =
        GrpcSequencerConnectionService.setup[MediatorDomainConfiguration](mediatorId)(
          adminServerRegistry,
          fetchConfig,
          saveConfig,
          Lens[MediatorDomainConfiguration, SequencerConnections](_.sequencerConnections)(
            connection => conf => conf.copy(sequencerConnections = connection)
          ),
          RequestSigner(
            syncCryptoApi,
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
        synchronizerConfig.synchronizerParameters.protocolVersion,
        timeouts,
        loggerFactory,
      )
      _ = topologyClient.setSynchronizerTimeTracker(timeTracker)

      topologyStoreIsEmpty <- EitherT.right(
        synchronizerTopologyStore
          .maxTimestamp(CantonTimestamp.MaxValue, includeRejected = true)
          .map(_.isEmpty)
      )
      // TODO(i12076): Request topology information from all sequencers and reconcile
      _ <-
        if (topologyStoreIsEmpty) {
          new StoreBasedSynchronizerTopologyInitializationCallback(
            mediatorId
          ).callback(
            new InitialTopologySnapshotValidator(
              synchronizerId,
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
        syncCryptoApi,
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
}

class MediatorNode(
    config: MediatorNodeConfigCommon,
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
