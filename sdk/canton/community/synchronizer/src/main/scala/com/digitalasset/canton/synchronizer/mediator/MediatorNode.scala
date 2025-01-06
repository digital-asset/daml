// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.mediator

import cats.data.{EitherT, OptionT}
import cats.syntax.either.*
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.admin.domain.v30.MediatorStatusServiceGrpc.MediatorStatusService
import com.digitalasset.canton.auth.CantonAdminToken
import com.digitalasset.canton.common.domain.grpc.SequencerInfoLoader
import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.config.*
import com.digitalasset.canton.connection.GrpcApiInfoService
import com.digitalasset.canton.connection.v30.ApiInfoServiceGrpc
import com.digitalasset.canton.crypto.{
  Crypto,
  CryptoHandshakeValidator,
  DomainCryptoPureApi,
  DomainSyncCryptoClient,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.environment.*
import com.digitalasset.canton.health.*
import com.digitalasset.canton.health.admin.data.{WaitingForExternalInput, WaitingForInitialization}
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, HasCloseContext, LifeCycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.mediator.admin.v30.MediatorInitializationServiceGrpc
import com.digitalasset.canton.networking.grpc.{CantonGrpcUtil, CantonMutableHandlerRegistry}
import com.digitalasset.canton.protocol.StaticDomainParameters
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
import com.digitalasset.canton.time.{Clock, DomainTimeTracker, HasUptime}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.DomainTopologyClient
import com.digitalasset.canton.topology.processing.{
  InitialTopologySnapshotValidator,
  TopologyTransactionProcessor,
}
import com.digitalasset.canton.topology.store.TopologyStoreId.DomainStore
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
    val timeTracker: DomainTimeTrackerConfig,
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
    override val adminApi: CommunityAdminServerConfig = CommunityAdminServerConfig(),
    override val storage: CommunityStorageConfig = CommunityStorageConfig.Memory(),
    override val crypto: CommunityCryptoConfig = CommunityCryptoConfig(),
    override val init: InitConfig = InitConfig(identity = Some(InitConfigBase.Identity())),
    override val timeTracker: DomainTimeTrackerConfig = DomainTimeTrackerConfig(),
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

  private val domainTopologyManager = new SingleUseCell[DomainTopologyManager]()

  override protected def sequencedTopologyStores: Seq[TopologyStore[DomainStore]] =
    domainTopologyManager.get.map(_.store).toList

  override protected def sequencedTopologyManagers: Seq[DomainTopologyManager] =
    domainTopologyManager.get.toList

  override protected def lookupTopologyClient(
      storeId: TopologyStoreId
  ): Option[DomainTopologyClient] =
    storeId match {
      case DomainStore(synchronizerId, _) =>
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

  private class WaitForMediatorToDomainInit(
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
        (StaticDomainParameters, SynchronizerId),
      ](
        "wait-for-mediator-to-domain-init",
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

    private val domainConfigurationStore =
      MediatorDomainConfigurationStore(storage, timeouts, loggerFactory)
    addCloseable(domainConfigurationStore)
    addCloseable(deferredSequencerClientHealth)

    override protected def stageCompleted(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Option[(StaticDomainParameters, SynchronizerId)]] =
      OptionT(domainConfigurationStore.fetchConfiguration).map { mediatorDomainConfiguration =>
        (mediatorDomainConfiguration.domainParameters, mediatorDomainConfiguration.synchronizerId)

      }.value

    override protected def buildNextStage(
        result: (
            StaticDomainParameters,
            SynchronizerId,
        )
    ): EitherT[FutureUnlessShutdown, String, StartupNode] = {
      val (staticDomainParameters, synchronizerId) = result
      val domainTopologyStore =
        TopologyStore(
          DomainStore(synchronizerId),
          storage,
          staticDomainParameters.protocolVersion,
          timeouts,
          loggerFactory,
        )
      addCloseable(domainTopologyStore)

      EitherT.rightT(
        new StartupNode(
          storage,
          crypto,
          adminServerRegistry,
          adminToken,
          mediatorId,
          staticDomainParameters,
          authorizedTopologyManager,
          synchronizerId,
          domainConfigurationStore,
          domainTopologyStore,
          healthService,
        )
      )
    }

    override def waitingFor: Option[WaitingForExternalInput] = Some(
      WaitingForInitialization
    )

    override protected def autoCompleteStage()
        : EitherT[FutureUnlessShutdown, String, Option[(StaticDomainParameters, SynchronizerId)]] =
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
        val synchronizerAlias = SynchronizerAlias.tryCreate("domain")
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
              .validate(sequencerAggregatedInfo.staticDomainParameters, cryptoConfig)
              .toEitherT[FutureUnlessShutdown]

            configToStore = MediatorDomainConfiguration(
              request.synchronizerId,
              sequencerAggregatedInfo.staticDomainParameters,
              request.sequencerConnections,
            )
            _ <- EitherT.right(domainConfigurationStore.saveConfiguration(configToStore))
          } yield (sequencerAggregatedInfo.staticDomainParameters, request.synchronizerId)
        }.map(_ => InitializeMediatorResponse())
      }

  }

  private class StartupNode(
      storage: Storage,
      crypto: Crypto,
      adminServerRegistry: CantonMutableHandlerRegistry,
      adminToken: CantonAdminToken,
      mediatorId: MediatorId,
      staticDomainParameters: StaticDomainParameters,
      authorizedTopologyManager: AuthorizedTopologyManager,
      synchronizerId: SynchronizerId,
      domainConfigurationStore: MediatorDomainConfigurationStore,
      domainTopologyStore: TopologyStore[DomainStore],
      healthService: DependenciesHealthService,
  ) extends BootstrapStage[MediatorNode, RunningNode[MediatorNode]](
        description = "Startup mediator node",
        bootstrapStageCallback,
      )
      with HasCloseContext {

    private val domainLoggerFactory =
      loggerFactory.append("synchronizerId", synchronizerId.toString)

    override protected def attempt()(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, String, Option[RunningNode[MediatorNode]]] = {

      def createDomainOutboxFactory(domainTopologyManager: DomainTopologyManager) =
        new DomainOutboxFactory(
          synchronizerId = synchronizerId,
          memberId = mediatorId,
          authorizedTopologyManager = authorizedTopologyManager,
          domainTopologyManager = domainTopologyManager,
          crypto = crypto,
          topologyConfig = config.topology,
          timeouts = timeouts,
          loggerFactory = domainLoggerFactory,
          futureSupervisor = arguments.futureSupervisor,
        )

      def createDomainTopologyManager(): Either[String, DomainTopologyManager] = {
        val outboxQueue = new DomainOutboxQueue(loggerFactory)

        val topologyManager = new DomainTopologyManager(
          nodeId = mediatorId.uid,
          clock = clock,
          crypto = crypto,
          staticDomainParameters = staticDomainParameters,
          store = domainTopologyStore,
          outboxQueue = outboxQueue,
          exitOnFatalFailures = parameters.exitOnFatalFailures,
          timeouts = timeouts,
          futureSupervisor = futureSupervisor,
          loggerFactory = loggerFactory,
        )

        if (domainTopologyManager.putIfAbsent(topologyManager).nonEmpty)
          Left("domainTopologyManager shouldn't have been set before")
        else
          topologyManager.asRight

      }

      val fetchConfig: () => FutureUnlessShutdown[Option[MediatorDomainConfiguration]] = () =>
        domainConfigurationStore.fetchConfiguration

      val saveConfig: MediatorDomainConfiguration => FutureUnlessShutdown[Unit] =
        domainConfigurationStore.saveConfiguration

      performUnlessClosingEitherUSF("starting up mediator node") {
        for {
          domainConfig <- OptionT(fetchConfig()).toRight(
            s"Mediator domain config has not been set. Must first be initialized by the domain in order to start."
          )

          domainTopologyManager <- EitherT.fromEither[FutureUnlessShutdown](
            createDomainTopologyManager()
          )
          domainOutboxFactory = createDomainOutboxFactory(domainTopologyManager)

          indexedStringStore = IndexedStringStore.create(
            storage,
            parameterConfig.cachingConfigs.indexedStrings,
            timeouts,
            domainLoggerFactory,
          )
          _ = addCloseable(indexedStringStore)

          _ <- EitherT.right[String](
            replicaManager.setup(
              adminServerRegistry,
              () =>
                mkMediatorRuntime(
                  mediatorId,
                  domainConfig,
                  indexedStringStore,
                  fetchConfig,
                  saveConfig,
                  storage,
                  crypto,
                  adminServerRegistry,
                  staticDomainParameters,
                  domainTopologyStore,
                  topologyManagerStatus = TopologyManagerStatus
                    .combined(authorizedTopologyManager, domainTopologyManager),
                  domainOutboxFactory,
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
            domainLoggerFactory,
            healthData = healthService.dependencies.map(_.toComponentStatus),
            staticDomainParameters.protocolVersion,
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
      domainConfig: MediatorDomainConfiguration,
      indexedStringStore: IndexedStringStore,
      fetchConfig: () => FutureUnlessShutdown[Option[MediatorDomainConfiguration]],
      saveConfig: MediatorDomainConfiguration => FutureUnlessShutdown[Unit],
      storage: Storage,
      crypto: Crypto,
      adminServerRegistry: CantonMutableHandlerRegistry,
      staticDomainParameters: StaticDomainParameters,
      domainTopologyStore: TopologyStore[DomainStore],
      topologyManagerStatus: TopologyManagerStatus,
      domainOutboxFactory: DomainOutboxFactory,
  ): EitherT[FutureUnlessShutdown, String, MediatorRuntime] = {
    val synchronizerId = domainConfig.synchronizerId
    val domainLoggerFactory = loggerFactory.append("synchronizerId", synchronizerId.toString)
    val synchronizerAlias = SynchronizerAlias(domainConfig.synchronizerId.uid.toLengthLimitedString)
    val sequencerInfoLoader = createSequencerInfoLoader()
    def getSequencerConnectionFromStore: FutureUnlessShutdown[Option[SequencerConnections]] =
      fetchConfig().map(_.map(_.sequencerConnections))

    for {
      indexedSynchronizerId <- EitherT
        .right(IndexedDomain.indexed(indexedStringStore)(synchronizerId))
      sequencedEventStore = SequencedEventStore(
        storage,
        indexedSynchronizerId,
        domainConfig.domainParameters.protocolVersion,
        timeouts,
        domainLoggerFactory,
      )
      sendTrackerStore = SendTrackerStore(storage)
      sequencerCounterTrackerStore = SequencerCounterTrackerStore(
        storage,
        indexedSynchronizerId,
        timeouts,
        domainLoggerFactory,
      )
      topologyProcessorAndClient <-
        EitherT
          .right(
            TopologyTransactionProcessor.createProcessorAndClientForDomain(
              domainTopologyStore,
              synchronizerId,
              new DomainCryptoPureApi(staticDomainParameters, crypto.pureCrypto),
              arguments.parameterConfig,
              arguments.clock,
              arguments.futureSupervisor,
              domainLoggerFactory,
            )()
          )
      (topologyProcessor, topologyClient) = topologyProcessorAndClient
      _ = ips.add(topologyClient)
      syncCryptoApi = new DomainSyncCryptoClient(
        mediatorId,
        synchronizerId,
        topologyClient,
        crypto,
        arguments.parameterConfig.sessionSigningKeys,
        staticDomainParameters,
        timeouts,
        futureSupervisor,
        domainLoggerFactory,
      )
      sequencerClientFactory = SequencerClientFactory(
        synchronizerId,
        syncCryptoApi,
        crypto,
        parameters.sequencerClient,
        parameters.tracing.propagation,
        arguments.testingConfig,
        domainConfig.domainParameters,
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
        domainLoggerFactory,
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
            domainConfig.domainParameters.protocolVersion,
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
            domainConfig.domainParameters.protocolVersion,
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

      timeTracker = DomainTimeTracker(
        config.timeTracker,
        clock,
        sequencerClient,
        domainConfig.domainParameters.protocolVersion,
        timeouts,
        loggerFactory,
      )
      _ = topologyClient.setDomainTimeTracker(timeTracker)

      topologyStoreIsEmpty <- EitherT.right(
        domainTopologyStore
          .maxTimestamp(CantonTimestamp.MaxValue, includeRejected = true)
          .map(_.isEmpty)
      )
      // TODO(i12076): Request topology information from all sequencers and reconcile
      _ <-
        if (topologyStoreIsEmpty) {
          new StoreBasedDomainTopologyInitializationCallback(
            mediatorId
          ).callback(
            new InitialTopologySnapshotValidator(
              synchronizerId,
              new DomainCryptoPureApi(staticDomainParameters, crypto.pureCrypto),
              domainTopologyStore,
              arguments.parameterConfig.processingTimeouts,
              domainLoggerFactory,
            ),
            topologyClient,
            sequencerClient,
            domainConfig.domainParameters.protocolVersion,
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
        domainOutboxFactory,
        timeTracker,
        parameters,
        domainConfig.domainParameters.protocolVersion,
        clock,
        arguments.metrics,
        config.mediator,
        domainLoggerFactory,
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
    new WaitForMediatorToDomainInit(
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
