// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain

import better.files.*
import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.traverse.*
import com.daml.error.*
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.config.{CryptoConfig, InitConfigBase}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.admin.grpc.GrpcVaultService.CommunityGrpcVaultServiceFactory
import com.digitalasset.canton.crypto.store.CryptoPrivateStore.CommunityCryptoPrivateStoreFactory
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.admin.v0.{DomainServiceGrpc, SequencerVersionServiceGrpc}
import com.digitalasset.canton.domain.admin.grpc as admingrpc
import com.digitalasset.canton.domain.config.*
import com.digitalasset.canton.domain.config.store.{
  DomainNodeSettingsStore,
  StoredDomainNodeSettings,
}
import com.digitalasset.canton.domain.initialization.*
import com.digitalasset.canton.domain.mediator.{
  CommunityMediatorRuntimeFactory,
  MediatorRuntime,
  MediatorRuntimeFactory,
}
import com.digitalasset.canton.domain.metrics.DomainMetrics
import com.digitalasset.canton.domain.sequencing.authentication.MemberAuthenticationServiceFactory
import com.digitalasset.canton.domain.sequencing.sequencer.Sequencer
import com.digitalasset.canton.domain.sequencing.sequencer.traffic.SequencerRateLimitManager
import com.digitalasset.canton.domain.sequencing.service.GrpcSequencerVersionService
import com.digitalasset.canton.domain.sequencing.{SequencerRuntime, SequencerRuntimeFactory}
import com.digitalasset.canton.domain.server.DynamicDomainGrpcServer
import com.digitalasset.canton.domain.service.ServiceAgreementManager
import com.digitalasset.canton.domain.topology.*
import com.digitalasset.canton.environment.*
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.health.admin.data.{
  DomainStatus,
  SequencerHealthStatus,
  TopologyQueueStatus,
}
import com.digitalasset.canton.health.{
  ComponentStatus,
  HealthService,
  MutableHealthComponent,
  MutableHealthQuasiComponent,
}
import com.digitalasset.canton.lifecycle.Lifecycle.CloseableServer
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, Lifecycle}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.{CantonGrpcUtil, CantonMutableHandlerRegistry}
import com.digitalasset.canton.protocol.DomainParameters.MaxRequestSize
import com.digitalasset.canton.protocol.{
  DomainParametersLookup,
  DynamicDomainParameters,
  StaticDomainParameters,
}
import com.digitalasset.canton.resource.{CommunityStorageFactory, Storage}
import com.digitalasset.canton.sequencing.SequencerConnections
import com.digitalasset.canton.sequencing.client.{grpc as _, *}
import com.digitalasset.canton.store.SequencerCounterTrackerStore
import com.digitalasset.canton.store.db.SequencerClientDiscriminator
import com.digitalasset.canton.time.{Clock, HasUptime}
import com.digitalasset.canton.topology.TopologyManagerError.DomainErrorGroup
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.*
import com.digitalasset.canton.topology.processing.TopologyTransactionProcessor
import com.digitalasset.canton.topology.store.TopologyStoreId.DomainStore
import com.digitalasset.canton.topology.store.{
  DomainTopologyStore,
  TopologyStateForInitializationService,
  TopologyStore,
  TopologyStoreId,
}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.TraceContext.withNewTraceContext
import com.digitalasset.canton.tracing.{NoTracing, TraceContext}
import com.digitalasset.canton.util.{EitherTUtil, ErrorUtil}
import com.google.common.annotations.VisibleForTesting
import org.apache.pekko.actor.ActorSystem

import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.concurrent.{ExecutionContextExecutorService, Future, blocking}

/** Startup / Bootstrapping class for domain
  *
  * The domain startup has three stages:
  * (1) start core services, wait until domainId is initialized (first time)
  * (2) start domain topology manager, wait until essential state is seeded (sequencer, identity and mediator keys are set)
  * (3) start domain entities
  */
class DomainNodeBootstrap(
    arguments: CantonNodeBootstrapCommonArguments[
      DomainConfig,
      DomainNodeParameters,
      DomainMetrics,
    ],
    sequencerRuntimeFactory: SequencerRuntimeFactory,
    mediatorFactory: MediatorRuntimeFactory,
)(implicit
    executionContext: ExecutionContextIdlenessExecutorService,
    executionSequencerFactory: ExecutionSequencerFactory,
    scheduler: ScheduledExecutorService,
    actorSystem: ActorSystem,
) extends CantonNodeBootstrapBase[Domain, DomainConfig, DomainNodeParameters, DomainMetrics](
      arguments
    )
    with DomainTopologyManagerIdentityInitialization[StoredDomainNodeSettings] {

  override def config: DomainConfig = arguments.config

  private val staticDomainParametersFromConfig =
    DomainNodeBootstrap.tryStaticDomainParamsFromConfig(
      config.init.domainParameters,
      config.crypto,
    )
  private val settingsStore = DomainNodeSettingsStore.create(
    storage,
    staticDomainParametersFromConfig,
    config.init.domainParameters.resetStoredStaticConfig,
    timeouts,
    loggerFactory,
  )
  private val sequencerTopologyStore =
    new DomainTopologyStore(storage, timeouts, loggerFactory, futureSupervisor)
  private val mediatorTopologyStore =
    new DomainTopologyStore(storage, timeouts, loggerFactory, futureSupervisor)

  // Mutable health component for the sequencer health, created during initialization
  private lazy val sequencerHealth = new MutableHealthQuasiComponent[Sequencer](
    loggerFactory,
    Sequencer.healthName,
    SequencerHealthStatus(isActive = false),
    timeouts,
    SequencerHealthStatus.shutdownStatus,
  )

  // Mutable health component for the domain topology sender health, created during initialization
  private lazy val domainTopologySenderHealth = MutableHealthComponent(
    loggerFactory,
    TopologyManagementInitialization.topologySenderHealthName,
    timeouts,
  )

  override protected def mkNodeHealthService(storage: Storage): HealthService =
    HealthService("domain", logger, timeouts, Seq(storage))

  // Holds the gRPC server started when the node is started, even when non initialized
  // If non initialized the server will expose the gRPC health service only
  private val nonInitializedDomainNodeServer =
    new AtomicReference[Option[DynamicDomainGrpcServer]](None)

  // If the node is started uninitialized, still create a non initialized server with a health service
  override def runOnSkippedInitialization: EitherT[Future, String, Unit] = {
    val wasNotSet = nonInitializedDomainNodeServer.compareAndSet(
      None,
      Some(
        makeDynamicDomainServer(
          // We use max value for the request size here as this is the default for a non initialized domain
          MaxRequestSize(NonNegativeInt.maxValue)
        )
      ),
    )
    if (!wasNotSet)
      EitherT.leftT[Future, Unit](
        "The gRPC server was already running when starting the domain node"
      )
    else EitherT.pure(())
  }

  private val domainApiServiceHealth = HealthService(
    CantonGrpcUtil.sequencerHealthCheckServiceName,
    logger,
    timeouts,
    criticalDependencies = Seq(sequencerHealth, storage),
    softDependencies = Seq(domainTopologySenderHealth),
  )

  private def makeDynamicDomainServer(maxRequestSize: MaxRequestSize) = {
    new DynamicDomainGrpcServer(
      loggerFactory,
      maxRequestSize,
      parameters,
      config.publicApi,
      arguments.metrics.metricsFactory,
      arguments.metrics.grpcMetrics,
      healthReporter,
      domainApiServiceHealth,
    )
  }

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var topologyManager: Option[DomainTopologyManager] = None
  private val protocolVersion = config.init.domainParameters.protocolVersion.unwrap

  override protected def autoInitializeIdentity(
      initConfigBase: InitConfigBase
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    withNewTraceContext { implicit traceContext =>
      for {
        initialized <- initializeTopologyManagerIdentity(
          name,
          DynamicDomainParameters.initialValues(clock, protocolVersion),
          initConfigBase,
          staticDomainParametersFromConfig,
        )
        (nodeId, topologyManager, namespaceKey) = initialized
        domainId = DomainId(nodeId.identity)
        _ <- initializeMediator(domainId, namespaceKey, topologyManager)
        _ <- initializeSequencerServices
        _ <- initializeSequencer(domainId, topologyManager, namespaceKey)
        // store the static domain parameters in our settings store
        _ <- settingsStore
          .saveSettings(StoredDomainNodeSettings(staticDomainParametersFromConfig))
          .leftMap(_.toString)
          .mapK(FutureUnlessShutdown.outcomeK)
        // finally, store the node id (which means we have completed initialisation)
        // as all methods above are idempotent, if we die during initialisation, we should come back here
        // and resume until we've stored the node id
        _ <- storeId(nodeId).mapK(FutureUnlessShutdown.outcomeK)
        _ <- startDomain(topologyManager, staticDomainParametersFromConfig)
      } yield ()
    }

  override def onClosed(): Unit = blocking {
    synchronized {
      logger.info("Stopping domain node")
      // If we shutdown still uninitialized, we explicitly close the gRPC server here
      // Otherwise it will be closed as part of closing of the sequencer node
      nonInitializedDomainNodeServer
        .getAndSet(None)
        .map(_.publicServer)
        .foreach(Lifecycle.close(_)(logger))
      super.onClosed()
      Lifecycle.close(
        sequencerTopologyStore,
        mediatorTopologyStore,
        sequencerHealth,
        domainTopologySenderHealth,
        domainApiServiceHealth,
      )(logger)
    }
  }

  private def initializeMediator(
      domainId: DomainId,
      namespaceKey: SigningPublicKey,
      topologyManager: DomainTopologyManager,
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    // In a domain without a dedicated DomainTopologyManager, the mediator always gets the same ID as the domain.
    val mediatorId = MediatorId(domainId)
    for {
      mediatorKey <- CantonNodeBootstrapCommon
        .getOrCreateSigningKey(crypto.value)(
          s"$name-mediator-signing"
        )
        .mapK(FutureUnlessShutdown.outcomeK)
      _ <- authorizeStateUpdate(
        topologyManager,
        namespaceKey,
        OwnerToKeyMapping(mediatorId, mediatorKey),
        protocolVersion,
      )
      _ <- authorizeStateUpdate(
        topologyManager,
        namespaceKey,
        MediatorDomainState(RequestSide.Both, domainId, mediatorId),
        protocolVersion,
      )
    } yield ()
  }

  private def initializeSequencer(
      domainId: DomainId,
      topologyManager: DomainTopologyManager,
      namespaceKey: SigningPublicKey,
  ): EitherT[FutureUnlessShutdown, String, PublicKey] = for {
    sequencerKey <- CantonNodeBootstrapCommon
      .getOrCreateSigningKey(crypto.value)(
        s"$name-sequencer-signing"
      )
      .mapK(FutureUnlessShutdown.outcomeK)
    _ <- authorizeStateUpdate(
      topologyManager,
      namespaceKey,
      OwnerToKeyMapping(SequencerId(domainId), sequencerKey),
      protocolVersion,
    )
  } yield sequencerKey

  override protected def initializeIdentityManagerAndServices(
      nodeId: NodeId,
      staticDomainParameters: StaticDomainParameters,
  ): Either[String, DomainTopologyManager] = {
    // starts second stage
    ErrorUtil.requireState(topologyManager.isEmpty, "Topology manager is already initialized.")

    logger.debug("Starting domain topology manager")
    val manager = new DomainTopologyManager(
      DomainTopologyManagerId(nodeId.identity),
      clock,
      authorizedTopologyStore,
      crypto.value,
      parameters.processingTimeouts,
      staticDomainParameters.protocolVersion,
      loggerFactory,
      futureSupervisor,
    )
    topologyManager = Some(manager)
    startTopologyManagementWriteService(manager)
    Right(manager)
  }

  /** If we're running a sequencer within the domain node itself, then locally start some core services */
  private def initializeSequencerServices: EitherT[FutureUnlessShutdown, String, Unit] = {
    adminServerRegistry
      .addServiceU(
        SequencerVersionServiceGrpc.bindService(
          new GrpcSequencerVersionService(protocolVersion, loggerFactory),
          executionContext,
        )
      )
    EitherT.pure(())
  }

  override protected def initialize(id: NodeId): EitherT[FutureUnlessShutdown, String, Unit] = {
    for {
      // TODO(#11052) fix node initialization such that we don't store "inconsistent" init data
      //    domain nodes get first initialized with init_id and then subsequently they get initialized
      //    with another init call (which then writes to the node config store).
      //    fix this and either support crash recovery for init data or only persist once everything
      //    is properly initialized
      staticDomainParameters <- settingsStore.fetchSettings
        .map(
          _.fold(staticDomainParametersFromConfig)(_.staticDomainParameters)
        )
        .leftMap(_.toString)
        .mapK(FutureUnlessShutdown.outcomeK)
      manager <- EitherT
        .fromEither[FutureUnlessShutdown](
          initializeIdentityManagerAndServices(id, staticDomainParameters)
        )
      _ <- startIfDomainManagerReadyOrDefer(manager, staticDomainParameters)
    } yield ()
  }

  /** The Domain cannot be started until the domain manager has keys for all domain entities available. These keys
    * can be provided by console commands or external processes via the admin API so there are no guarantees for when they
    * arrive. If they are not immediately available, we add an observer to the topology manager which will be triggered after
    * every change to the topology manager. If after one of these changes we find that the domain manager has the keys it
    * requires to be initialized we will then start the domain.
    * TODO(i12893): if we defer startup of the domain the initialization check and eventual domain startup will
    * occur within the topology manager transaction observer. currently exceptions will bubble
    * up into the topology transaction processor however if a error is encountered it is just
    * logged here leaving the domain in a dangling state. Ideally this would trigger a managed
    * shutdown of some form allow allowing another startup attempt to be run if appropriate, however
    * I don't believe we currently have a means of doing this.
    */
  private def startIfDomainManagerReadyOrDefer(
      manager: DomainTopologyManager,
      staticDomainParameters: StaticDomainParameters,
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    def deferStart: EitherT[FutureUnlessShutdown, String, Unit] = {
      logger.info("Deferring domain startup until domain manager has been fully initialized")
      manager.addObserver(new DomainIdentityStateObserver {
        val attemptedStart = new AtomicBoolean(false)

        override def addedSignedTopologyTransaction(
            timestamp: CantonTimestamp,
            transaction: Seq[SignedTopologyTransaction[TopologyChangeOp]],
        )(implicit traceContext: TraceContext): Unit = {
          if (!attemptedStart.get()) {
            val initTimeout = parameters.processingTimeouts.unbounded
            val managerInitialized =
              initTimeout.await(
                s"Domain startup waiting for the domain topology manager to be initialised"
              )(manager.isInitialized(mustHaveActiveMediator = true))
            if (managerInitialized) {
              if (attemptedStart.compareAndSet(false, true)) {
                manager.removeObserver(this)
                // we're now the top level error handler of starting a domain so log appropriately
                val domainStarted =
                  initTimeout.await("Domain startup awaiting domain ready to handle requests")(
                    startDomain(manager, staticDomainParameters).value.onShutdown(
                      Left("Aborted startup due to shutdown")
                    )
                  )
                domainStarted match {
                  case Left(error) =>
                    logger.error(s"Deferred domain startup failed with error: $error")
                  case Right(_) => // nothing to do
                }
              }
            }
          }
        }
      })
      EitherT.pure[FutureUnlessShutdown, String](())
    }

    for {
      // if the domain is starting up after previously running its identity will have been stored and will be immediately available
      alreadyInitialized <- EitherT
        .right(manager.isInitialized(mustHaveActiveMediator = true))
        .mapK(FutureUnlessShutdown.outcomeK)
      // if not, then create an observer of topology transactions that will check each time whether full identity has been generated
      _ <- if (alreadyInitialized) startDomain(manager, staticDomainParameters) else deferStart
    } yield ()
  }

  /** Attempt to create the domain and only return and call setInstance once it is ready to handle requests */
  private def startDomain(
      manager: DomainTopologyManager,
      staticDomainParameters: StaticDomainParameters,
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    startInstanceUnlessClosing(performUnlessClosingEitherU(functionFullName) {
      // store with all topology transactions which were timestamped and distributed via sequencer
      val domainId = manager.id.domainId
      val sequencedTopologyStore =
        sequencerTopologyStore.initOrGet(DomainStore(domainId))
      val publicSequencerConnectionEitherT =
        config.publicApi.toSequencerConnectionConfig.toConnection.toEitherT[Future]

      for {
        publicSequencerConnection <- publicSequencerConnectionEitherT
        managerDiscriminator <- EitherT.right(
          SequencerClientDiscriminator.fromDomainMember(manager.id, indexedStringStore)
        )
        topologyManagerSequencerCounterTrackerStore = SequencerCounterTrackerStore(
          storage,
          managerDiscriminator,
          timeouts,
          loggerFactory,
        )
        initialKeys <- EitherT.right(manager.getKeysForBootstrapping())
        processorAndClient <- EitherT.right(
          TopologyTransactionProcessor.createProcessorAndClientForDomain(
            sequencedTopologyStore,
            manager.id,
            domainId,
            protocolVersion,
            crypto.value,
            SigningPublicKey.collect(initialKeys),
            parameters,
            clock,
            futureSupervisor,
            loggerFactory.append("client", "topology-manager"),
          )
        )
        (topologyProcessor, topologyClient) = processorAndClient
        sequencerClientFactoryFactory = (client: DomainTopologyClientWithInit) =>
          new DomainNodeSequencerClientFactory(
            domainId,
            arguments.metrics,
            client,
            parameters,
            crypto.value,
            staticDomainParameters,
            arguments.testingConfig,
            clock,
            futureSupervisor,
            loggerFactory,
          )

        syncCrypto: DomainSyncCryptoClient = {
          ips.add(topologyClient).discard
          new SyncCryptoApiProvider(
            manager.id,
            ips,
            crypto.value,
            parameters.cachingConfigs,
            timeouts,
            futureSupervisor,
            loggerFactory,
          )
            .tryForDomain(domainId)
        }

        // Setup the service agreement manager and its storage
        agreementManager <- config.serviceAgreement
          .traverse { agreementFile =>
            ServiceAgreementManager
              .create(
                agreementFile.toScala,
                storage,
                crypto.value.pureCrypto,
                staticDomainParameters.protocolVersion,
                timeouts,
                loggerFactory,
              )
          }
          .toEitherT[Future]

        memberAuthFactory = MemberAuthenticationServiceFactory.forOld(
          domainId,
          clock,
          nonceExpirationTime = config.publicApi.nonceExpirationTime.asJava,
          tokenExpirationTime = config.publicApi.tokenExpirationTime.asJava,
          timeouts = parameters.processingTimeouts,
          loggerFactory,
          topologyProcessor,
        )
        sequencerRuntime <- sequencerRuntimeFactory
          .create(
            domainId,
            SequencerId(domainId.uid),
            crypto.value,
            // The sequencer is using the topology manager's topology client
            manager.id,
            topologyClient,
            storage,
            clock,
            config,
            staticDomainParameters,
            arguments.testingConfig,
            parameters.processingTimeouts,
            agreementManager,
            memberAuthFactory,
            parameters,
            arguments.metrics.sequencer,
            indexedStringStore,
            futureSupervisor,
            Option.empty[TopologyStateForInitializationService],
            Option.empty[SequencerRateLimitManager],
            Option.empty[TopologyManagerStatus],
            loggerFactory,
            logger,
          )
        _ = sequencerHealth.set(sequencerRuntime.sequencer)
        domainIdentityService = DomainTopologyManagerRequestService.create(
          config.topology,
          manager,
          topologyClient,
          parameters.processingTimeouts,
          loggerFactory,
          futureSupervisor,
        )
        domainParamsLookup = DomainParametersLookup.forSequencerDomainParameters(
          staticDomainParameters,
          config.publicApi.overrideMaxRequestSize,
          topologyClient,
          futureSupervisor,
          loggerFactory,
        )

        maxRequestSize <- EitherTUtil
          .fromFuture(
            domainParamsLookup.getApproximate(),
            error => s"Unable to retrieve the domain parameters: ${error.getMessage}",
          )
          .map(paramsO =>
            paramsO.map(_.maxRequestSize).getOrElse(MaxRequestSize(NonNegativeInt.maxValue))
          )
        // must happen before the init of topology management since it will call the embedded sequencer's public api
        dynamicServer = nonInitializedDomainNodeServer
          .getAndSet(None)
          .map(_.initialize(sequencerRuntime))
          .getOrElse(makeDynamicDomainServer(maxRequestSize).initialize(sequencerRuntime))
        topologyManagementArtefacts <- TopologyManagementInitialization(
          config,
          domainId,
          storage,
          clock,
          crypto.value,
          syncCrypto,
          sequencedTopologyStore,
          SequencerConnections.single(publicSequencerConnection),
          manager,
          domainIdentityService,
          topologyManagerSequencerCounterTrackerStore,
          topologyProcessor,
          topologyClient,
          initialKeys,
          sequencerClientFactoryFactory(topologyClient),
          parameters,
          futureSupervisor,
          indexedStringStore,
          loggerFactory,
        )
        _ = domainTopologySenderHealth.set(topologyManagementArtefacts.dispatcher.sender)
        mediatorRuntime <- EmbeddedMediatorInitialization(
          domainId,
          parameters,
          staticDomainParameters.protocolVersion,
          clock,
          crypto.value,
          mediatorTopologyStore.initOrGet(DomainStore(domainId, discriminator = "M")),
          config.timeTracker,
          storage,
          sequencerClientFactoryFactory,
          SequencerConnections.single(publicSequencerConnection),
          arguments.metrics,
          mediatorFactory,
          indexedStringStore,
          futureSupervisor,
          loggerFactory.append("node", "mediator"),
        )

        domain = {
          logger.debug("Starting domain services")
          new Domain(
            config,
            clock,
            staticDomainParameters,
            adminServerRegistry,
            manager,
            agreementManager,
            topologyManagementArtefacts,
            storage,
            sequencerRuntime,
            mediatorRuntime,
            dynamicServer.publicServer,
            loggerFactory,
            domainApiServiceHealth.dependencies.map(_.toComponentStatus),
          )
        }
      } yield domain
    })

  override def isActive: Boolean = true

  override protected def sequencedTopologyStores: Seq[TopologyStore[TopologyStoreId]] =
    sequencerTopologyStore.get().toList
}

object DomainNodeBootstrap {
  val LoggerFactoryKeyName: String = "domain"

  trait Factory[DC <: DomainConfig] {

    def create(
        arguments: NodeFactoryArguments[DC, DomainNodeParameters, DomainMetrics]
    )(implicit
        actorSystem: ActorSystem,
        scheduler: ScheduledExecutorService,
        executionContext: ExecutionContextIdlenessExecutorService,
        executionSequencerFactory: ExecutionSequencerFactory,
        traceContext: TraceContext,
    ): Either[String, DomainNodeBootstrap]

  }

  private[domain] def tryStaticDomainParamsFromConfig(
      domainParametersConfig: DomainParametersConfig,
      cryptoConfig: CryptoConfig,
  ): StaticDomainParameters =
    domainParametersConfig
      .toStaticDomainParameters(cryptoConfig)
      .valueOr(err =>
        throw new IllegalArgumentException(
          s"Failed to convert static domain params: ${err}"
        )
      )

  object CommunityDomainFactory extends Factory[CommunityDomainConfig] {

    override def create(
        arguments: NodeFactoryArguments[CommunityDomainConfig, DomainNodeParameters, DomainMetrics]
    )(implicit
        actorSystem: ActorSystem,
        scheduler: ScheduledExecutorService,
        executionContext: ExecutionContextIdlenessExecutorService,
        executionSequencerFactory: ExecutionSequencerFactory,
        traceContext: TraceContext,
    ): Either[String, DomainNodeBootstrap] =
      arguments
        .toCantonNodeBootstrapCommonArguments(
          new CommunityStorageFactory(arguments.config.storage),
          new CommunityCryptoFactory,
          new CommunityCryptoPrivateStoreFactory,
          new CommunityGrpcVaultServiceFactory,
        )
        .map { arguments =>
          new DomainNodeBootstrap(
            arguments,
            new SequencerRuntimeFactory.Community(arguments.config.sequencer),
            CommunityMediatorRuntimeFactory,
          )
        }

  }
}

/** A domain in the system.
  *
  * The domain offers to the participant nodes:
  * - sequencing for total-order multicast.
  * - mediator as part of the transaction protocol coordination.
  * - identity providing service.
  *
  * @param config Domain configuration [[com.digitalasset.canton.domain.config.DomainConfig]] parsed from config file.
  */
class Domain(
    val config: DomainConfig,
    override protected val clock: Clock,
    staticDomainParameters: StaticDomainParameters,
    adminApiRegistry: CantonMutableHandlerRegistry,
    val domainTopologyManager: DomainTopologyManager,
    val agreementManager: Option[ServiceAgreementManager],
    topologyManagementArtefacts: TopologyManagementComponents,
    storage: Storage,
    sequencerRuntime: SequencerRuntime,
    @VisibleForTesting
    val mediatorRuntime: MediatorRuntime,
    publicServer: CloseableServer,
    protected val loggerFactory: NamedLoggerFactory,
    healthData: => Seq[ComponentStatus],
)(implicit executionContext: ExecutionContextExecutorService)
    extends CantonNode
    with NamedLogging
    with HasUptime
    with NoTracing {

  registerAdminServices()

  logger.debug("Domain successfully initialized")

  override def isActive: Boolean = true

  override def status: Future[DomainStatus] =
    for {
      activeMembers <- sequencerRuntime.fetchActiveMembers()
      sequencer <- sequencerRuntime.health
    } yield {
      val ports = Map("admin" -> config.adminApi.port, "public" -> config.publicApi.port)
      val participants = activeMembers.collect { case x: ParticipantId =>
        x
      }
      val topologyQueues = TopologyQueueStatus(
        manager = domainTopologyManager.queueSize,
        dispatcher = topologyManagementArtefacts.dispatcher.queueSize,
        clients = topologyManagementArtefacts.client.numPendingChanges,
      )
      DomainStatus(
        domainTopologyManager.id.uid,
        uptime(),
        ports,
        participants,
        sequencer,
        topologyQueues,
        healthData,
      )
    }

  override def close(): Unit = {
    logger.debug("Stopping domain runner")
    Lifecycle.close(
      topologyManagementArtefacts,
      domainTopologyManager,
      mediatorRuntime,
      sequencerRuntime,
      publicServer,
      storage,
    )(logger)
  }

  def registerAdminServices(): Unit = {
    // The domain admin-API services
    sequencerRuntime.registerAdminGrpcServices { service =>
      adminApiRegistry.addServiceU(service)
    }
    mediatorRuntime.registerAdminGrpcServices { service =>
      adminApiRegistry.addServiceU(service)
    }
    adminApiRegistry
      .addServiceU(
        DomainServiceGrpc
          .bindService(
            new admingrpc.GrpcDomainService(
              staticDomainParameters,
              agreementManager,
            ),
            executionContext,
          )
      )
  }
}

object Domain extends DomainErrorGroup {

  /** If the function maps `member` to `recordConfig`,
    * the sequencer client for `member` will record all sends requested and events received to the directory specified
    * by the recording config.
    * A new recording starts whenever the domain is restarted.
    */
  @VisibleForTesting
  val recordSequencerInteractions: AtomicReference[PartialFunction[Member, RecordingConfig]] =
    new AtomicReference(PartialFunction.empty)

  /** If the function maps `member` to `path`,
    * the sequencer client for `member` will replay events from `path` instead of pulling them from the sequencer.
    * A new replay starts whenever the domain is restarted.
    */
  @VisibleForTesting
  val replaySequencerConfig: AtomicReference[PartialFunction[Member, ReplayConfig]] =
    new AtomicReference(PartialFunction.empty)

  def setMemberRecordingPath(member: Member)(config: RecordingConfig): RecordingConfig = {
    val namePrefix = member.show.stripSuffix("...")
    config.setFilename(namePrefix)
  }

  def defaultReplayPath(member: Member)(config: ReplayConfig): ReplayConfig =
    config.copy(recordingConfig = setMemberRecordingPath(member)(config.recordingConfig))

  abstract class GrpcSequencerAuthenticationErrorGroup extends ErrorGroup

  @Explanation(
    """This error indicates that the initialisation of a domain node failed due to invalid arguments."""
  )
  @Resolution("""Consult the error details.""")
  object FailedToInitialiseDomainNode
      extends ErrorCode(
        id = "DOMAIN_NODE_INITIALISATION_FAILED",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Failure(override val cause: String)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(cause)

    final case class Shutdown()(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(cause = "Node is being shutdown")
  }

}
