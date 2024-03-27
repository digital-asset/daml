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
import com.digitalasset.canton.crypto.{
  Crypto,
  CryptoHandshakeValidator,
  DomainSyncCryptoClient,
  Fingerprint,
}
import com.digitalasset.canton.domain.Domain
import com.digitalasset.canton.domain.mediator.admin.gprc.{
  InitializeMediatorRequestX,
  InitializeMediatorResponseX,
}
import com.digitalasset.canton.domain.mediator.service.GrpcMediatorInitializationServiceX
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
  GrpcHealthReporter,
  HealthService,
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
import com.digitalasset.canton.store.db.SequencerClientDiscriminator
import com.digitalasset.canton.store.{
  IndexedStringStore,
  SendTrackerStore,
  SequencedEventStore,
  SequencerCounterTrackerStore,
}
import com.digitalasset.canton.time.{Clock, HasUptime}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.processing.TopologyTransactionProcessorX
import com.digitalasset.canton.topology.store.TopologyStoreId.DomainStore
import com.digitalasset.canton.topology.store.TopologyStoreX
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{ResourceUtil, SingleUseCell}
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
    override val devVersionSupport: Boolean = false,
    override val dontWarnOnDeprecatedPV: Boolean = false,
    override val initialProtocolVersion: ProtocolVersion = ProtocolVersion.latest,
    override val batching: BatchingConfig = BatchingConfig(),
    override val caching: CachingConfigs = CachingConfigs(),
    override val useNewTrafficControl: Boolean = false,
    override val useUnifiedSequencer: Boolean = false,
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
final case class CommunityMediatorNodeXConfig(
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
    with ConfigDefaults[DefaultPorts, CommunityMediatorNodeXConfig] {

  override val nodeTypeName: String = "mediator"

  override def replicationEnabled: Boolean = false

  override def withDefaults(ports: DefaultPorts): CommunityMediatorNodeXConfig = {
    this
      .focus(_.adminApi.internalPort)
      .modify(ports.mediatorAdminApiPort.setDefaultPort)
  }
}

class MediatorNodeBootstrapX(
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
) extends CantonNodeBootstrapX[
      MediatorNode,
      MediatorNodeConfigCommon,
      MediatorNodeParameters,
      MediatorMetrics,
    ](arguments) {

  override protected def member(uid: UniqueIdentifier): Member = MediatorId(uid)

  private val domainTopologyManager = new SingleUseCell[DomainTopologyManagerX]()

  override protected def sequencedTopologyStores: Seq[TopologyStoreX[DomainStore]] =
    domainTopologyManager.get.map(_.store).toList

  override protected def sequencedTopologyManagers: Seq[DomainTopologyManagerX] =
    domainTopologyManager.get.toList

  private lazy val deferredSequencerClientHealth =
    MutableHealthComponent(loggerFactory, SequencerClient.healthName, timeouts)
  override protected def mkNodeHealthService(storage: Storage): HealthService =
    HealthService(
      "mediator",
      logger,
      timeouts,
      Seq(storage),
      softDependencies = Seq(deferredSequencerClientHealth),
    )
  private class WaitForMediatorToDomainInit(
      storage: Storage,
      crypto: Crypto,
      mediatorId: MediatorId,
      authorizedTopologyManager: AuthorizedTopologyManagerX,
      healthService: HealthService,
  ) extends BootstrapStageWithStorage[MediatorNode, StartupNode, DomainId](
        "wait-for-mediator-to-domain-init",
        bootstrapStageCallback,
        storage,
        config.init.autoInit,
      )
      with GrpcMediatorInitializationServiceX.Callback {

    adminServerRegistry
      .addServiceU(
        MediatorInitializationServiceGrpc
          .bindService(
            new GrpcMediatorInitializationServiceX(this, loggerFactory),
            executionContext,
          ),
        true,
      )

    protected val domainConfigurationStore =
      MediatorDomainConfigurationStore(storage, timeouts, loggerFactory)
    addCloseable(domainConfigurationStore)
    addCloseable(deferredSequencerClientHealth)

    override protected def stageCompleted(implicit
        traceContext: TraceContext
    ): Future[Option[DomainId]] = domainConfigurationStore.fetchConfiguration.toOption.mapFilter {
      case Some(res) => Some(res.domainId)
      case None => None
    }.value

    override protected def buildNextStage(domainId: DomainId): StartupNode = {
      val domainTopologyStore =
        TopologyStoreX(DomainStore(domainId), storage, timeouts, loggerFactory)
      addCloseable(domainTopologyStore)

      val outboxQueue = new DomainOutboxQueue(loggerFactory)
      val topologyManager = new DomainTopologyManagerX(
        clock = clock,
        crypto = crypto,
        store = domainTopologyStore,
        outboxQueue = outboxQueue,
        enableTopologyTransactionValidation = config.topology.enableTopologyTransactionValidation,
        timeouts = timeouts,
        futureSupervisor = futureSupervisor,
        loggerFactory = loggerFactory,
      )

      if (domainTopologyManager.putIfAbsent(topologyManager).nonEmpty) {
        // TODO(#14048) how to handle this error properly?
        throw new IllegalStateException("domainTopologyManager shouldn't have been set before")
      }

      new StartupNode(
        storage,
        crypto,
        mediatorId,
        authorizedTopologyManager,
        topologyManager,
        domainId,
        domainConfigurationStore,
        domainTopologyStore,
        healthService,
      )
    }

    override protected def autoCompleteStage()
        : EitherT[FutureUnlessShutdown, String, Option[DomainId]] =
      EitherT.rightT(None) // this stage doesn't have auto-init

    override def initialize(request: InitializeMediatorRequestX)(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, String, InitializeMediatorResponseX] = {
      if (isInitialized) {
        logger.info(
          "Received a request to initialize an already initialized mediator. Skipping initialization!"
        )
        EitherT.pure(InitializeMediatorResponseX())
      } else {
        val configToStore = MediatorDomainConfiguration(
          Fingerprint.tryCreate(
            "unused"
          ), // x-nodes do not need to return the initial key
          request.domainId,
          request.domainParameters,
          request.sequencerConnections,
        )
        val sequencerInfoLoader = createSequencerInfoLoader(configToStore)
        lazy val validatedET = sequencerInfoLoader.validateSequencerConnection(
          DomainAlias.tryCreate("domain"),
          Some(request.domainId),
          configToStore.sequencerConnections,
          request.sequencerConnectionValidation,
        )
        completeWithExternal {
          logger.info(
            s"Assigning mediator to ${request.domainId} via sequencers ${request.sequencerConnections}"
          )
          for {
            _ <- validatedET.leftMap { errors =>
              s"Invalid sequencer connections provided for initialisation: $errors"
            }
            _ <-
              domainConfigurationStore
                .saveConfiguration(configToStore)
                .leftMap(_.toString)
          } yield request.domainId
        }.map(_ => InitializeMediatorResponseX())
      }
    }

  }

  private class StartupNode(
      storage: Storage,
      crypto: Crypto,
      mediatorId: MediatorId,
      authorizedTopologyManager: AuthorizedTopologyManagerX,
      domainTopologyManager: DomainTopologyManagerX,
      domainId: DomainId,
      domainConfigurationStore: MediatorDomainConfigurationStore,
      domainTopologyStore: TopologyStoreX[DomainStore],
      healthService: HealthService,
  ) extends BootstrapStage[MediatorNode, RunningNode[MediatorNode]](
        description = "Startup mediator node",
        bootstrapStageCallback,
      )
      with HasCloseContext {

    private val domainLoggerFactory = loggerFactory.append("domainId", domainId.toString)

    override protected def attempt()(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, String, Option[RunningNode[MediatorNode]]] = {

      val domainOutboxFactory = new DomainOutboxXFactory(
        domainId = domainId,
        memberId = mediatorId,
        authorizedTopologyManager = authorizedTopologyManager,
        domainTopologyManager = domainTopologyManager,
        crypto = crypto,
        topologyXConfig = config.topology,
        timeouts = timeouts,
        loggerFactory = domainLoggerFactory,
        futureSupervisor = arguments.futureSupervisor,
      )
      performUnlessClosingEitherU("starting up mediator node") {
        val indexedStringStore = IndexedStringStore.create(
          storage,
          parameterConfig.cachingConfigs.indexedStrings,
          timeouts,
          domainLoggerFactory,
        )
        addCloseable(indexedStringStore)
        for {
          domainId <- initializeNodePrerequisites(
            storage,
            crypto,
            mediatorId,
            () => domainConfigurationStore.fetchConfiguration.leftMap(_.toString),
            domainConfigurationStore.saveConfiguration(_).leftMap(_.toString),
            indexedStringStore,
            domainTopologyStore,
            TopologyManagerStatus.combined(authorizedTopologyManager, domainTopologyManager),
            domainTopologyStateInit =
              new StoreBasedDomainTopologyInitializationCallback(mediatorId, domainTopologyStore),
            domainOutboxFactory = domainOutboxFactory,
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

  private def createSequencerInfoLoader(config: MediatorDomainConfiguration) =
    new SequencerInfoLoader(
      timeouts = timeouts,
      traceContextPropagation = parameters.tracing.propagation,
      clientProtocolVersions = NonEmpty.mk(Seq, config.domainParameters.protocolVersion),
      minimumProtocolVersion = Some(config.domainParameters.protocolVersion),
      dontWarnOnDeprecatedPV = parameterConfig.dontWarnOnDeprecatedPV,
      loggerFactory = loggerFactory,
    )

  protected def initializeNodePrerequisites(
      storage: Storage,
      crypto: Crypto,
      mediatorId: MediatorId,
      fetchConfig: () => EitherT[Future, String, Option[MediatorDomainConfiguration]],
      saveConfig: MediatorDomainConfiguration => EitherT[Future, String, Unit],
      indexedStringStore: IndexedStringStore,
      domainTopologyStore: TopologyStoreX[DomainStore],
      topologyManagerStatus: TopologyManagerStatus,
      domainTopologyStateInit: DomainTopologyInitializationCallback,
      domainOutboxFactory: DomainOutboxXFactory,
  ): EitherT[Future, String, DomainId] =
    for {
      domainConfig <- fetchConfig()
        .leftMap(err => s"Failed to fetch domain configuration: $err")
        .flatMap { x =>
          EitherT.fromEither(
            x.toRight(
              s"Mediator domain config has not been set. Must first be initialized by the domain in order to start."
            )
          )
        }

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
              topologyManagerStatus,
              domainTopologyStateInit,
              domainOutboxFactory,
            ),
          storage.isActive,
        )
      )
    } yield domainConfig.domainId

  private def mkMediatorRuntime(
      mediatorId: MediatorId,
      domainConfig: MediatorDomainConfiguration,
      indexedStringStore: IndexedStringStore,
      fetchConfig: () => EitherT[Future, String, Option[MediatorDomainConfiguration]],
      saveConfig: MediatorDomainConfiguration => EitherT[Future, String, Unit],
      storage: Storage,
      crypto: Crypto,
      domainTopologyStore: TopologyStoreX[DomainStore],
      topologyManagerStatus: TopologyManagerStatus,
      domainTopologyStateInit: DomainTopologyInitializationCallback,
      domainOutboxFactory: DomainOutboxXFactory,
  ): EitherT[Future, String, MediatorRuntime] = {
    val domainId = domainConfig.domainId
    val domainLoggerFactory = loggerFactory.append("domainId", domainId.toString)
    val domainAlias = DomainAlias(domainConfig.domainId.uid.toLengthLimitedString)
    val sequencerInfoLoader = createSequencerInfoLoader(domainConfig)
    def getSequencerConnectionFromStore = fetchConfig()
      .map(_.map(_.sequencerConnections))

    for {
      _ <- CryptoHandshakeValidator
        .validate(domainConfig.domainParameters, config.crypto)
        .toEitherT
      sequencedEventStore = SequencedEventStore(
        storage,
        SequencerClientDiscriminator.UniqueDiscriminator,
        domainConfig.domainParameters.protocolVersion,
        timeouts,
        domainLoggerFactory,
      )
      sendTrackerStore = SendTrackerStore(storage)
      sequencerCounterTrackerStore = SequencerCounterTrackerStore(
        storage,
        SequencerClientDiscriminator.UniqueDiscriminator,
        timeouts,
        domainLoggerFactory,
      )
      topologyProcessorAndClient <-
        EitherT.right(
          TopologyTransactionProcessorX.createProcessorAndClientForDomain(
            domainTopologyStore,
            domainId,
            domainConfig.domainParameters.protocolVersion,
            crypto.pureCrypto,
            arguments.parameterConfig,
            config.topology.enableTopologyTransactionValidation,
            arguments.clock,
            arguments.futureSupervisor,
            domainLoggerFactory,
          )
        )
      (topologyProcessor, topologyClient) = topologyProcessorAndClient
      _ = ips.add(topologyClient)
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
        config.timeTracker,
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
      authorizedTopologyManager: AuthorizedTopologyManagerX,
      healthServer: GrpcHealthReporter,
      healthService: HealthService,
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

object MediatorNodeBootstrapX {
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
