// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator

import cats.Monad
import cats.data.EitherT
import cats.instances.future.*
import cats.syntax.either.*
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.DomainAlias
import com.digitalasset.canton.common.domain.grpc.SequencerInfoLoader
import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.config.*
import com.digitalasset.canton.crypto.{Crypto, CryptoHandshakeValidator, DomainSyncCryptoClient}
import com.digitalasset.canton.domain.Domain
import com.digitalasset.canton.domain.mediator.admin.gprc.{
  InitializeMediatorRequest,
  InitializeMediatorResponse,
}
import com.digitalasset.canton.domain.mediator.service.GrpcMediatorInitializationService
import com.digitalasset.canton.domain.mediator.store.{
  MediatorDomainConfiguration,
  MediatorDomainConfigurationStore,
}
import com.digitalasset.canton.domain.metrics.MediatorMetrics
import com.digitalasset.canton.domain.service.GrpcSequencerConnectionService
import com.digitalasset.canton.environment.*
import com.digitalasset.canton.health.admin.data.MediatorNodeStatus
import com.digitalasset.canton.health.{
  ComponentStatus,
  DependenciesHealthService,
  GrpcHealthReporter,
  LivenessHealthService,
  MutableHealthComponent,
}
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, HasCloseContext, Lifecycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.mediator.admin.v30.MediatorInitializationServiceGrpc
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.sequencing.SequencerConnections
import com.digitalasset.canton.sequencing.client.{
  RequestSigner,
  SequencerClient,
  SequencerClientConfig,
  SequencerClientFactory,
}
import com.digitalasset.canton.store.*
import com.digitalasset.canton.time.{Clock, DomainTimeTracker, HasUptime}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.DomainTopologyClient
import com.digitalasset.canton.topology.processing.TopologyTransactionProcessor
import com.digitalasset.canton.topology.store.TopologyStoreId.DomainStore
import com.digitalasset.canton.topology.store.{TopologyStore, TopologyStoreId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{EitherTUtil, ResourceUtil, SingleUseCell}
import com.digitalasset.canton.version.{ProtocolVersion, ProtocolVersionCompatibility}
import monocle.Lens
import monocle.macros.syntax.lens.*
import org.apache.pekko.actor.ActorSystem

import java.util.concurrent.ScheduledExecutorService
import scala.concurrent.Future

abstract class MediatorNodeConfigCommon(
    val adminApi: AdminServerConfig,
    val storage: StorageConfig,
    val crypto: CryptoConfig,
    val init: InitConfig,
    val timeTracker: DomainTimeTrackerConfig,
    val sequencerClient: SequencerClientConfig,
    val caching: CachingConfigs,
    val parameters: MediatorNodeParameterConfig,
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
    // TODO(i15561): Revert back to `false` once there is a stable Daml 3 protocol version
    override val devVersionSupport: Boolean = true,
    override val dontWarnOnDeprecatedPV: Boolean = false,
    override val batching: BatchingConfig = BatchingConfig(),
    override val caching: CachingConfigs = CachingConfigs(),
    override val useUnifiedSequencer: Boolean = false,
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
    adminApi: ClientConfig
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
      monitoring,
    )
    with ConfigDefaults[DefaultPorts, CommunityMediatorNodeConfig] {

  override val nodeTypeName: String = "mediator"

  override def replicationEnabled: Boolean = false

  override def withDefaults(ports: DefaultPorts): CommunityMediatorNodeConfig = {
    this
      .focus(_.adminApi.internalPort)
      .modify(ports.mediatorAdminApiPort.setDefaultPort)
  }
}

class MediatorNodeBootstrap(
    arguments: CantonNodeBootstrapCommonArguments[
      MediatorNodeConfigCommon,
      MediatorNodeParameters,
      MediatorMetrics,
    ],
    protected val replicaManager: MediatorReplicaManager,
    mediatorRuntimeFactory: MediatorRuntimeFactory,
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

  private val domainTopologyManager = new SingleUseCell[DomainTopologyManager]()
  private val topologyClient = new SingleUseCell[DomainTopologyClient]()

  override protected def sequencedTopologyStores: Seq[TopologyStore[DomainStore]] =
    domainTopologyManager.get.map(_.store).toList

  override protected def sequencedTopologyManagers: Seq[DomainTopologyManager] =
    domainTopologyManager.get.toList

  override protected def lookupTopologyClient(
      storeId: TopologyStoreId
  ): Option[DomainTopologyClient] =
    storeId match {
      case DomainStore(domainId, _) =>
        topologyClient.get.filter(_.domainId == domainId)
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

  private class WaitForMediatorToDomainInit(
      storage: Storage,
      crypto: Crypto,
      mediatorId: MediatorId,
      authorizedTopologyManager: AuthorizedTopologyManager,
      healthService: DependenciesHealthService,
  ) extends BootstrapStageWithStorage[MediatorNode, StartupNode, DomainId](
        "wait-for-mediator-to-domain-init",
        bootstrapStageCallback,
        storage,
        config.init.autoInit,
      )
      with GrpcMediatorInitializationService.Callback {

    adminServerRegistry
      .addServiceU(
        MediatorInitializationServiceGrpc
          .bindService(
            new GrpcMediatorInitializationService(this, loggerFactory),
            executionContext,
          )
      )

    private val domainConfigurationStore =
      MediatorDomainConfigurationStore(storage, timeouts, loggerFactory)
    addCloseable(domainConfigurationStore)
    addCloseable(deferredSequencerClientHealth)

    override protected def stageCompleted(implicit
        traceContext: TraceContext
    ): Future[Option[DomainId]] = domainConfigurationStore.fetchConfiguration.toOption.mapFilter {
      case Some(mediatorDomainConfiguration) => Some(mediatorDomainConfiguration.domainId)
      case None => None
    }.value

    override protected def buildNextStage(
        domainId: DomainId
    ): EitherT[FutureUnlessShutdown, String, StartupNode] = {
      val domainTopologyStore =
        TopologyStore(DomainStore(domainId), storage, timeouts, loggerFactory)
      addCloseable(domainTopologyStore)

      EitherT.rightT(
        new StartupNode(
          storage,
          crypto,
          mediatorId,
          authorizedTopologyManager,
          domainId,
          domainConfigurationStore,
          domainTopologyStore,
          healthService,
        )
      )
    }

    override protected def autoCompleteStage()
        : EitherT[FutureUnlessShutdown, String, Option[DomainId]] =
      EitherT.rightT(None) // this stage doesn't have auto-init

    override def initialize(request: InitializeMediatorRequest)(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, String, InitializeMediatorResponse] = {
      if (isInitialized) {
        logger.info(
          "Received a request to initialize an already initialized mediator. Skipping initialization!"
        )
        EitherT.pure(InitializeMediatorResponse())
      } else {
        val domainAlias = DomainAlias.tryCreate("domain")
        val sequencerInfoLoader = createSequencerInfoLoader()
        completeWithExternal {
          logger.info(
            s"Assigning mediator to ${request.domainId} via sequencers ${request.sequencerConnections}"
          )
          for {
            sequencerAggregatedInfo <- sequencerInfoLoader
              .loadAndAggregateSequencerEndpoints(
                domainAlias,
                Some(request.domainId),
                request.sequencerConnections,
                request.sequencerConnectionValidation,
              )
              .leftMap(error => s"Error loading sequencer endpoint information: $error")
            configToStore = MediatorDomainConfiguration(
              request.domainId,
              sequencerAggregatedInfo.staticDomainParameters,
              request.sequencerConnections,
            )
            _ <-
              domainConfigurationStore
                .saveConfiguration(configToStore)
                .leftMap(_.toString)
          } yield request.domainId
        }.map(_ => InitializeMediatorResponse())
      }
    }

  }

  private class StartupNode(
      storage: Storage,
      crypto: Crypto,
      mediatorId: MediatorId,
      authorizedTopologyManager: AuthorizedTopologyManager,
      domainId: DomainId,
      domainConfigurationStore: MediatorDomainConfigurationStore,
      domainTopologyStore: TopologyStore[DomainStore],
      healthService: DependenciesHealthService,
  ) extends BootstrapStage[MediatorNode, RunningNode[MediatorNode]](
        description = "Startup mediator node",
        bootstrapStageCallback,
      )
      with HasCloseContext {

    private val domainLoggerFactory = loggerFactory.append("domainId", domainId.toString)

    override protected def attempt()(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, String, Option[RunningNode[MediatorNode]]] = {

      def createDomainOutboxFactory(domainTopologyManager: DomainTopologyManager) =
        new DomainOutboxFactory(
          domainId = domainId,
          memberId = mediatorId,
          authorizedTopologyManager = authorizedTopologyManager,
          domainTopologyManager = domainTopologyManager,
          crypto = crypto,
          topologyConfig = config.topology,
          timeouts = timeouts,
          loggerFactory = domainLoggerFactory,
          futureSupervisor = arguments.futureSupervisor,
        )

      def createDomainTopologyManager(
          protocolVersion: ProtocolVersion
      ): Either[String, DomainTopologyManager] = {
        val outboxQueue = new DomainOutboxQueue(loggerFactory)

        val topologyManager = new DomainTopologyManager(
          nodeId = mediatorId.uid,
          clock = clock,
          crypto = crypto,
          store = domainTopologyStore,
          outboxQueue = outboxQueue,
          protocolVersion = protocolVersion,
          timeouts = timeouts,
          futureSupervisor = futureSupervisor,
          loggerFactory = loggerFactory,
        )

        if (domainTopologyManager.putIfAbsent(topologyManager).nonEmpty)
          Left("domainTopologyManager shouldn't have been set before")
        else
          topologyManager.asRight

      }

      val fetchConfig: () => EitherT[Future, String, Option[MediatorDomainConfiguration]] = () =>
        domainConfigurationStore.fetchConfiguration.leftMap(_.toString)

      val saveConfig: MediatorDomainConfiguration => EitherT[Future, String, Unit] =
        domainConfigurationStore.saveConfiguration(_).leftMap(_.toString)

      performUnlessClosingEitherU("starting up mediator node") {
        for {
          domainConfig <- fetchConfig()
            .leftMap(err => s"Failed to fetch domain configuration: $err")
            .flatMap { mediatorDomainConfigurationO =>
              EitherT.fromEither(
                mediatorDomainConfigurationO.toRight(
                  s"Mediator domain config has not been set. Must first be initialized by the domain in order to start."
                )
              )
            }

          domainTopologyManager <- EitherT.fromEither(
            createDomainTopologyManager(domainConfig.domainParameters.protocolVersion)
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
                  domainTopologyStore,
                  topologyManagerStatus = TopologyManagerStatus
                    .combined(authorizedTopologyManager, domainTopologyManager),
                  domainTopologyStateInit = new StoreBasedDomainTopologyInitializationCallback(
                    mediatorId,
                    domainTopologyStore,
                  ),
                  domainOutboxFactory,
                ),
              storage.isActive,
            )
          )
        } yield {
          val node = new MediatorNode(
            arguments.config,
            mediatorId,
            domainId,
            replicaManager,
            storage,
            clock,
            domainLoggerFactory,
            healthData = healthService.dependencies.map(_.toComponentStatus),
          )
          addCloseable(node)
          Some(new RunningNode(bootstrapStageCallback, node))
        }
      }
    }
  }

  private def createSequencerInfoLoader() = {
    new SequencerInfoLoader(
      timeouts = timeouts,
      traceContextPropagation = parameters.tracing.propagation,
      clientProtocolVersions =
        if (parameterConfig.devVersionSupport) ProtocolVersion.supported
        else
          // TODO(#15561) Remove NonEmpty construct once stableAndSupported is NonEmpty again
          NonEmpty
            .from(ProtocolVersion.stableAndSupported)
            .getOrElse(sys.error("no protocol version is considered stable in this release")),
      minimumProtocolVersion = Some(ProtocolVersion.minimum),
      dontWarnOnDeprecatedPV = parameterConfig.dontWarnOnDeprecatedPV,
      loggerFactory = loggerFactory,
    )
  }

  private def mkMediatorRuntime(
      mediatorId: MediatorId,
      domainConfig: MediatorDomainConfiguration,
      indexedStringStore: IndexedStringStore,
      fetchConfig: () => EitherT[Future, String, Option[MediatorDomainConfiguration]],
      saveConfig: MediatorDomainConfiguration => EitherT[Future, String, Unit],
      storage: Storage,
      crypto: Crypto,
      domainTopologyStore: TopologyStore[DomainStore],
      topologyManagerStatus: TopologyManagerStatus,
      domainTopologyStateInit: DomainTopologyInitializationCallback,
      domainOutboxFactory: DomainOutboxFactory,
  ): EitherT[Future, String, MediatorRuntime] = {
    val domainId = domainConfig.domainId
    val domainLoggerFactory = loggerFactory.append("domainId", domainId.toString)
    val domainAlias = DomainAlias(domainConfig.domainId.uid.toLengthLimitedString)
    val sequencerInfoLoader = createSequencerInfoLoader()
    def getSequencerConnectionFromStore = fetchConfig()
      .map(_.map(_.sequencerConnections))

    for {
      _ <- CryptoHandshakeValidator
        .validate(domainConfig.domainParameters, config.crypto)
        .toEitherT
      indexedDomainId <- EitherT.right(IndexedDomain.indexed(indexedStringStore)(domainId))
      sequencedEventStore = SequencedEventStore(
        storage,
        indexedDomainId,
        domainConfig.domainParameters.protocolVersion,
        timeouts,
        domainLoggerFactory,
      )
      sendTrackerStore = SendTrackerStore(storage)
      sequencerCounterTrackerStore = SequencerCounterTrackerStore(
        storage,
        indexedDomainId,
        timeouts,
        domainLoggerFactory,
      )
      topologyProcessorAndClient <-
        EitherT.right(
          TopologyTransactionProcessor.createProcessorAndClientForDomain(
            domainTopologyStore,
            domainId,
            domainConfig.domainParameters.protocolVersion,
            crypto.pureCrypto,
            arguments.parameterConfig,
            arguments.clock,
            arguments.futureSupervisor,
            domainLoggerFactory,
          )
        )
      (topologyProcessor, topologyClient) = topologyProcessorAndClient
      _ = ips.add(topologyClient)
      _ <- EitherTUtil.condUnitET(
        MediatorNodeBootstrap.this.topologyClient.putIfAbsent(topologyClient).isEmpty,
        "Unexpected state during initialization: topology client shouldn't have been set before",
      )
      syncCryptoApi = new DomainSyncCryptoClient(
        mediatorId,
        domainId,
        topologyClient,
        crypto,
        parameters.cachingConfigs,
        timeouts,
        futureSupervisor,
        domainLoggerFactory,
      )
      sequencerClientFactory = SequencerClientFactory(
        domainId,
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
          Domain.recordSequencerInteractions
            .get()
            .lift(member)
            .map(Domain.setMemberRecordingPath(member)),
        member =>
          Domain.replaySequencerConfig.get().lift(member).map(Domain.defaultReplayPath(member)),
        arguments.metrics.sequencerClient,
        parameters.loggingConfig,
        domainLoggerFactory,
        ProtocolVersionCompatibility.trySupportedProtocolsDomain(parameters),
        None,
      )
      sequencerClientRef =
        GrpcSequencerConnectionService.setup[MediatorDomainConfiguration](mediatorId)(
          adminServerRegistry,
          fetchConfig,
          saveConfig,
          Lens[MediatorDomainConfiguration, SequencerConnections](_.sequencerConnections)(
            connection => conf => conf.copy(sequencerConnections = connection)
          ),
          RequestSigner(syncCryptoApi, domainConfig.domainParameters.protocolVersion),
          sequencerClientFactory,
          sequencerInfoLoader,
          domainAlias,
          domainId,
        )
      // we wait here until the sequencer becomes active. this allows to reconfigure the
      // sequencer client address
      info <- GrpcSequencerConnectionService.waitUntilSequencerConnectionIsValid(
        sequencerInfoLoader,
        this,
        futureSupervisor,
        getSequencerConnectionFromStore,
      )

      requestSigner = RequestSigner(syncCryptoApi, domainConfig.domainParameters.protocolVersion)
      _ <- {
        val headSnapshot = topologyClient.headSnapshot
        for {
          // TODO(i12076): Request topology information from all sequencers and reconcile
          isMediatorActive <- EitherT.right[String](headSnapshot.isMediatorActive(mediatorId))
          _ <- Monad[EitherT[Future, String, *]].whenA(!isMediatorActive)(
            sequencerClientFactory
              .makeTransport(
                info.sequencerConnections.default,
                mediatorId,
                requestSigner,
                allowReplay = false,
              )
              .flatMap(
                ResourceUtil.withResourceEitherT(_)(
                  domainTopologyStateInit
                    .callback(topologyClient, _, domainConfig.domainParameters.protocolVersion)
                )
              )
          )
        } yield {}
      }

      sequencerClient <- sequencerClientFactory.create(
        mediatorId,
        sequencedEventStore,
        sendTrackerStore,
        requestSigner,
        info.sequencerConnections,
        info.expectedSequencers,
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

      // can just new up the enterprise mediator factory here as the mediator node is only available in enterprise setups
      mediatorRuntime <- mediatorRuntimeFactory.create(
        mediatorId,
        domainId,
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
        futureSupervisor,
        domainLoggerFactory,
      )
      _ <- mediatorRuntime.start()
    } yield mediatorRuntime
  }

  override protected def customNodeStages(
      storage: Storage,
      crypto: Crypto,
      nodeId: UniqueIdentifier,
      authorizedTopologyManager: AuthorizedTopologyManager,
      healthServer: GrpcHealthReporter,
      healthService: DependenciesHealthService,
  ): BootstrapStageOrLeaf[MediatorNode] = {
    new WaitForMediatorToDomainInit(
      storage,
      crypto,
      MediatorId(nodeId),
      authorizedTopologyManager,
      healthService,
    )
  }

  override protected def onClosed(): Unit = {
    super.onClosed()
  }

}

object MediatorNodeBootstrap {
  val LoggerFactoryKeyName: String = "mediator"
}

class MediatorNode(
    config: MediatorNodeConfigCommon,
    mediatorId: MediatorId,
    domainId: DomainId,
    protected[canton] val replicaManager: MediatorReplicaManager,
    storage: Storage,
    override val clock: Clock,
    override val loggerFactory: NamedLoggerFactory,
    healthData: => Seq[ComponentStatus],
) extends CantonNode
    with NamedLogging
    with HasUptime {

  def isActive: Boolean = replicaManager.isActive

  def status: Future[MediatorNodeStatus] = {
    val ports = Map("admin" -> config.adminApi.port)
    Future.successful(
      MediatorNodeStatus(
        mediatorId.uid,
        domainId,
        uptime(),
        ports,
        replicaManager.isActive,
        replicaManager.getTopologyQueueStatus,
        healthData,
      )
    )
  }

  override def close(): Unit =
    Lifecycle.close(
      replicaManager,
      storage,
    )(logger)

}
