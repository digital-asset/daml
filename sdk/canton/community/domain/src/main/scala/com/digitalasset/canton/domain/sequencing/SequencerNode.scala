// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing

import cats.data.EitherT
import com.digitalasset.canton.admin.domain.v30.SequencerStatusServiceGrpc
import com.digitalasset.canton.auth.CantonAdminToken
import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.config.NonNegativeFiniteDuration as _
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.connection.GrpcApiInfoService
import com.digitalasset.canton.connection.v30.ApiInfoServiceGrpc
import com.digitalasset.canton.crypto.{Crypto, DomainCryptoPureApi, DomainSyncCryptoClient}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.domain.metrics.SequencerMetrics
import com.digitalasset.canton.domain.sequencing.admin.data.{
  SequencerHealthStatus,
  SequencerNodeStatus,
}
import com.digitalasset.canton.domain.sequencing.admin.grpc.{
  InitializeSequencerRequest,
  InitializeSequencerResponse,
}
import com.digitalasset.canton.domain.sequencing.authentication.MemberAuthenticationServiceFactory
import com.digitalasset.canton.domain.sequencing.config.{
  SequencerNodeConfigCommon,
  SequencerNodeParameters,
}
import com.digitalasset.canton.domain.sequencing.sequencer.*
import com.digitalasset.canton.domain.sequencing.sequencer.store.{
  SequencerDomainConfiguration,
  SequencerDomainConfigurationStore,
}
import com.digitalasset.canton.domain.sequencing.service.{
  GrpcSequencerInitializationService,
  GrpcSequencerStatusService,
}
import com.digitalasset.canton.domain.server.DynamicGrpcServer
import com.digitalasset.canton.environment.*
import com.digitalasset.canton.health.*
import com.digitalasset.canton.health.admin.data.{WaitingForExternalInput, WaitingForInitialization}
import com.digitalasset.canton.lifecycle.{
  FutureUnlessShutdown,
  HasCloseContext,
  Lifecycle,
  PromiseUnlessShutdown,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.{CantonGrpcUtil, CantonMutableHandlerRegistry}
import com.digitalasset.canton.protocol.DomainParameters.MaxRequestSize
import com.digitalasset.canton.protocol.DomainParametersLookup.SequencerDomainParameters
import com.digitalasset.canton.protocol.{
  DomainParametersLookup,
  DynamicDomainParametersLookup,
  StaticDomainParameters,
}
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.sequencer.admin.v30.SequencerInitializationServiceGrpc
import com.digitalasset.canton.sequencing.client.SequencerClient.SequencerTransports
import com.digitalasset.canton.sequencing.client.{
  RequestSigner,
  SendTracker,
  SequencedEventValidatorFactory,
  SequencerClientImplPekko,
}
import com.digitalasset.canton.store.{
  IndexedDomain,
  IndexedStringStore,
  SendTrackerStore,
  SequencedEventStore,
}
import com.digitalasset.canton.time.*
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.DomainTopologyClient
import com.digitalasset.canton.topology.processing.TopologyTransactionProcessor
import com.digitalasset.canton.topology.store.TopologyStoreId.DomainStore
import com.digitalasset.canton.topology.store.{
  StoreBasedTopologyStateForInitializationService,
  TopologyStore,
  TopologyStoreId,
}
import com.digitalasset.canton.topology.transaction.{
  DomainTrustCertificate,
  MediatorDomainState,
  SequencerDomainState,
  SignedTopologyTransaction,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{EitherTUtil, SingleUseCell}
import com.digitalasset.canton.version.{ProtocolVersion, ReleaseVersion}
import io.grpc.ServerServiceDefinition
import org.apache.pekko.actor.ActorSystem

import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.Future

object SequencerNodeBootstrap {
  trait Factory[C <: SequencerNodeConfigCommon] {
    def create(
        arguments: NodeFactoryArguments[
          C,
          SequencerNodeParameters,
          SequencerMetrics,
        ]
    )(implicit
        executionContext: ExecutionContextIdlenessExecutorService,
        scheduler: ScheduledExecutorService,
        actorSystem: ActorSystem,
    ): Either[String, SequencerNodeBootstrap]
  }

  val LoggerFactoryKeyName: String = "sequencer"
}

class SequencerNodeBootstrap(
    arguments: CantonNodeBootstrapCommonArguments[
      SequencerNodeConfigCommon,
      SequencerNodeParameters,
      SequencerMetrics,
    ],
    mkSequencerFactory: MkSequencerFactory,
)(implicit
    executionContext: ExecutionContextIdlenessExecutorService,
    scheduler: ScheduledExecutorService,
    actorSystem: ActorSystem,
) extends CantonNodeBootstrapImpl[
      SequencerNode,
      SequencerNodeConfigCommon,
      SequencerNodeParameters,
      SequencerMetrics,
    ](arguments) {

  override protected def member(uid: UniqueIdentifier): Member = SequencerId(uid)

  override protected def customNodeStages(
      storage: Storage,
      crypto: Crypto,
      adminServerRegistry: CantonMutableHandlerRegistry,
      adminToken: CantonAdminToken,
      nodeId: UniqueIdentifier,
      manager: AuthorizedTopologyManager,
      healthReporter: GrpcHealthReporter,
      healthService: DependenciesHealthService,
  ): BootstrapStageOrLeaf[SequencerNode] =
    new WaitForSequencerToDomainInit(
      storage,
      crypto,
      adminServerRegistry,
      adminToken,
      SequencerId(nodeId),
      manager,
      healthReporter,
      healthService,
    )

  override protected val adminTokenConfig: Option[String] = config.adminApi.adminToken

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

  private class WaitForSequencerToDomainInit(
      storage: Storage,
      crypto: Crypto,
      adminServerRegistry: CantonMutableHandlerRegistry,
      adminToken: CantonAdminToken,
      sequencerId: SequencerId,
      manager: AuthorizedTopologyManager,
      healthReporter: GrpcHealthReporter,
      healthService: DependenciesHealthService,
  ) extends BootstrapStageWithStorage[SequencerNode, StartupNode, StageResult](
        description = "wait-for-sequencer-to-domain-init",
        bootstrapStageCallback,
        storage,
        config.init.autoInit,
      )
      with GrpcSequencerInitializationService.Callback {

    override def getAdminToken: Option[String] = Some(adminToken.secret)

    // add initialization service
    adminServerRegistry.addServiceU(
      SequencerInitializationServiceGrpc.bindService(
        new GrpcSequencerInitializationService(this, loggerFactory)(executionContext),
        executionContext,
      )
    )
    adminServerRegistry.addServiceU(
      ApiInfoServiceGrpc.bindService(
        new GrpcApiInfoService(CantonGrpcUtil.ApiName.AdminApi),
        executionContext,
      )
    )

    // Holds the gRPC server started when the node is started, even when non initialized
    // If non initialized the server will expose the gRPC health service only
    protected val nonInitializedSequencerNodeServer =
      new AtomicReference[Option[DynamicGrpcServer]](None)
    addCloseable(new AutoCloseable() {
      override def close(): Unit =
        nonInitializedSequencerNodeServer.getAndSet(None).foreach(_.publicServer.close())
    })
    addCloseable(sequencerPublicApiHealthService)
    addCloseable(sequencerHealth)

    private def mkFactory(
        protocolVersion: ProtocolVersion
    )(implicit traceContext: TraceContext) = {
      logger.debug(s"Creating sequencer factory with ${config.sequencer}")
      val ret = mkSequencerFactory(
        protocolVersion,
        Some(config.health),
        clock,
        scheduler,
        arguments.metrics,
        storage,
        sequencerId,
        arguments.parameterConfig,
        arguments.futureSupervisor,
        loggerFactory,
      )(config.sequencer)
      addCloseable(ret)
      ret
    }

    /** if node is not initialized, create a dynamic domain server such that we can serve a health end-point until
      * we are initialised
      */
    private def initSequencerNodeServer(): Unit =
      if (nonInitializedSequencerNodeServer.get().isEmpty) {
        // the sequential initialisation queue ensures that this is thread safe
        nonInitializedSequencerNodeServer
          .set(
            Some(
              makeDynamicGrpcServer(
                // We use max value for the request size here as this is the default for a non initialized sequencer
                MaxRequestSize(NonNegativeInt.maxValue),
                healthReporter,
              )
            )
          )
          .discard
      }

    private val domainConfigurationStore =
      SequencerDomainConfigurationStore(storage, timeouts, loggerFactory)

    override protected def stageCompleted(implicit
        traceContext: TraceContext
    ): Future[Option[StageResult]] =
      domainConfigurationStore.fetchConfiguration.toOption
        .map {
          case Some(existing) =>
            Some(
              StageResult(
                existing.domainParameters,
                mkFactory(existing.domainParameters.protocolVersion),
                new DomainTopologyManager(
                  sequencerId.uid,
                  clock,
                  crypto,
                  existing.domainParameters,
                  store = createDomainTopologyStore(existing.domainId),
                  outboxQueue = new DomainOutboxQueue(loggerFactory),
                  exitOnFatalFailures = parameters.exitOnFatalFailures,
                  timeouts,
                  futureSupervisor,
                  loggerFactory,
                ),
                sequencerSnapshot = None,
              )
            )
          case None =>
            // create sequencer server such that we can expose a health endpoint until initialized
            initSequencerNodeServer()
            None
        }
        .value
        .map(_.flatten)

    private def createDomainTopologyStore(domainId: DomainId): TopologyStore[DomainStore] = {
      val store =
        TopologyStore(DomainStore(domainId), storage, timeouts, loggerFactory)
      addCloseable(store)
      store
    }

    override protected def buildNextStage(
        result: StageResult
    ): EitherT[FutureUnlessShutdown, String, StartupNode] =
      for {
        _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
          domainTopologyManager.putIfAbsent(result.domainTopologyManager).isEmpty,
          "Unexpected state during initialization: domain topology manager shouldn't have been set before",
        )
      } yield {
        new StartupNode(
          storage,
          crypto,
          adminServerRegistry,
          adminToken,
          sequencerId,
          result.sequencerFactory,
          result.staticDomainParameters,
          manager,
          result.domainTopologyManager,
          nonInitializedSequencerNodeServer.getAndSet(None),
          result.sequencerSnapshot,
          healthReporter,
          healthService,
        )
      }

    override def waitingFor: Option[WaitingForExternalInput] =
      Some(WaitingForInitialization)

    override protected def autoCompleteStage()
        : EitherT[FutureUnlessShutdown, String, Option[StageResult]] =
      EitherT.rightT(None) // this stage doesn't have auto-init

    override def initialize(request: InitializeSequencerRequest)(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, String, InitializeSequencerResponse] =
      if (isInitialized) {
        logger.info(
          "Received a request to initialize an already initialized sequencer. Skipping initialization!"
        )
        EitherT.pure(InitializeSequencerResponse(replicated = config.sequencer.supportsReplicas))
      } else {
        completeWithExternalUS {
          logger.info(
            s"Assigning sequencer to domain ${if (request.sequencerSnapshot.isEmpty) "from beginning"
              else "with existing snapshot"}"
          )
          val sequencerFactory = mkFactory(request.domainParameters.protocolVersion)
          val domainIds = request.topologySnapshot.result
            .map(_.mapping)
            .collect { case SequencerDomainState(domain, _, _, _) => domain }
            .toSet
          for {
            // TODO(#12390) validate initalisation request, as from here on, it must succeed
            //    - authorization validation etc is done during manager.add
            //    - so we need:
            //        - there must be a dynamic domain parameter
            //        - there must have one mediator and one sequencer group
            //        - each member must have the necessary keys
            domainId <- EitherT.fromEither[FutureUnlessShutdown](
              domainIds.headOption.toRight("No domain id within topology state defined!")
            )
            _ <- request.sequencerSnapshot
              .map { snapshot =>
                logger.debug("Uploading sequencer snapshot to sequencer driver")
                val initialState = SequencerInitialState(
                  domainId,
                  snapshot,
                  request.topologySnapshot.result.view
                    .map(tx => (tx.sequenced.value, tx.validFrom.value)),
                )
                // TODO(#14070) make initialize idempotent to support crash recovery during init
                sequencerFactory
                  .initialize(initialState, sequencerId)
                  .mapK(FutureUnlessShutdown.outcomeK)
              }
              .getOrElse {
                logger.debug("Skipping sequencer snapshot")
                EitherT.rightT[FutureUnlessShutdown, String](())
              }
            store = createDomainTopologyStore(domainId)
            outboxQueue = new DomainOutboxQueue(loggerFactory)
            topologyManager = new DomainTopologyManager(
              sequencerId.uid,
              clock,
              crypto,
              request.domainParameters,
              store,
              outboxQueue,
              exitOnFatalFailures = parameters.exitOnFatalFailures,
              timeouts,
              futureSupervisor,
              loggerFactory,
            )
            _ = logger.debug(
              s"Storing ${request.topologySnapshot.result.length} txs in the domain store"
            )
            _ <- EitherT
              .right(store.bootstrap(request.topologySnapshot))
              .mapK(FutureUnlessShutdown.outcomeK)
            _ = if (logger.underlying.isDebugEnabled()) {
              logger.debug(
                s"Bootstrapped sequencer topology domain store with transactions ${request.topologySnapshot.result}"
              )
            }
            _ <- domainConfigurationStore
              .saveConfiguration(
                SequencerDomainConfiguration(
                  domainId,
                  request.domainParameters,
                )
              )
              .leftMap(e => s"Unable to save parameters: ${e.toString}")
              .mapK(FutureUnlessShutdown.outcomeK)
          } yield StageResult(
            request.domainParameters,
            sequencerFactory,
            topologyManager,
            request.sequencerSnapshot,
          )
        }.map { _ =>
          InitializeSequencerResponse(replicated = config.sequencer.supportsReplicas)
        }
      }
  }

  private class StartupNode(
      storage: Storage,
      crypto: Crypto,
      adminServerRegistry: CantonMutableHandlerRegistry,
      adminToken: CantonAdminToken,
      sequencerId: SequencerId,
      sequencerFactory: SequencerFactory,
      staticDomainParameters: StaticDomainParameters,
      authorizedTopologyManager: AuthorizedTopologyManager,
      domainTopologyManager: DomainTopologyManager,
      preinitializedServer: Option[DynamicGrpcServer],
      sequencerSnapshot: Option[SequencerSnapshot],
      healthReporter: GrpcHealthReporter,
      healthService: DependenciesHealthService,
  ) extends BootstrapStage[SequencerNode, RunningNode[SequencerNode]](
        description = "Startup sequencer node",
        bootstrapStageCallback,
      )
      with HasCloseContext {
    override def getAdminToken: Option[String] = Some(adminToken.secret)
    // save one argument and grab the domainId from the store ...
    private val domainId = domainTopologyManager.store.storeId.domainId
    private val domainLoggerFactory = loggerFactory.append("domainId", domainId.toString)

    preinitializedServer.foreach(x => addCloseable(x.publicServer))

    override protected def attempt()(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, String, Option[RunningNode[SequencerNode]]] = {

      val domainOutboxFactory = new DomainOutboxFactorySingleCreate(
        domainId,
        sequencerId,
        authorizedTopologyManager,
        domainTopologyManager,
        crypto,
        config.topology,
        timeouts,
        arguments.futureSupervisor,
        loggerFactory,
      )

      val domainTopologyStore = domainTopologyManager.store

      addCloseable(domainOutboxFactory)

      performUnlessClosingEitherU("starting up runtime") {
        val indexedStringStore = IndexedStringStore.create(
          storage,
          parameterConfig.cachingConfigs.indexedStrings,
          timeouts,
          domainLoggerFactory,
        )
        addCloseable(indexedStringStore)
        for {
          processorAndClient <- EitherT.right(
            TopologyTransactionProcessor.createProcessorAndClientForDomain(
              domainTopologyStore,
              domainId,
              staticDomainParameters.protocolVersion,
              new DomainCryptoPureApi(staticDomainParameters, crypto.pureCrypto),
              parameters,
              clock,
              futureSupervisor,
              domainLoggerFactory,
            )
          )
          (topologyProcessor, topologyClient) = processorAndClient
          _ = addCloseable(topologyProcessor)
          _ = addCloseable(topologyClient)
          _ = ips.add(topologyClient)
          _ <- EitherTUtil.condUnitET[Future](
            SequencerNodeBootstrap.this.topologyClient.putIfAbsent(topologyClient).isEmpty,
            "Unexpected state during initialization: topology client shouldn't have been set before",
          )
          membersToRegister <- {
            val tsInit = SignedTopologyTransaction.InitialTopologySequencingTime.immediateSuccessor
            // When the sequencer is started on a fresh domain there's no sequencer snapshot,
            // so we need to register all members present in the topology snapshot
            if (topologyClient.approximateTimestamp == tsInit) {
              // This sequencer node was started for the first time and initialized with a topology state.
              // Therefore, we fetch all members who have a registered role on the domain and pass them
              // to the underlying sequencer driver to register them as known members.
              EitherT.right[String](
                domainTopologyStore
                  .findPositiveTransactions(
                    tsInit,
                    asOfInclusive = false,
                    isProposal = false,
                    types = Seq(
                      DomainTrustCertificate.code,
                      SequencerDomainState.code,
                      MediatorDomainState.code,
                    ),
                    filterUid = None,
                    filterNamespace = None,
                  )
                  .map { transactions =>
                    val participants = transactions
                      .collectOfMapping[DomainTrustCertificate]
                      .collectLatestByUniqueKey
                      .toTopologyState
                      .map(_.participantId)
                      .toSet
                    val sequencers = transactions
                      .collectOfMapping[SequencerDomainState]
                      .collectLatestByUniqueKey
                      .toTopologyState
                      .flatMap(sds => sds.active.forgetNE ++ sds.observers)
                      .toSet
                    val mediators = transactions
                      .collectOfMapping[MediatorDomainState]
                      .collectLatestByUniqueKey
                      .toTopologyState
                      .flatMap(mds => mds.active.forgetNE ++ mds.observers)
                      .toSet
                    participants ++ sequencers ++ mediators
                  }
              )
            } else EitherT.rightT[Future, String](Set.empty[Member])
          }

          memberAuthServiceFactory = MemberAuthenticationServiceFactory(
            domainId,
            clock,
            config.publicApi.nonceExpirationInterval.asJava,
            config.publicApi.maxTokenExpirationInterval.asJava,
            useExponentialRandomTokenExpiration =
              config.publicApi.useExponentialRandomTokenExpiration,
            parameters.processingTimeouts,
            domainLoggerFactory,
            topologyProcessor,
          )

          syncCrypto = new DomainSyncCryptoClient(
            sequencerId,
            domainId,
            topologyClient,
            crypto,
            parameters.cachingConfigs,
            staticDomainParameters,
            parameters.processingTimeouts,
            futureSupervisor,
            loggerFactory,
          )
          runtimeReadyPromise = new PromiseUnlessShutdown[Unit](
            "sequencer-runtime-ready",
            futureSupervisor,
          )
          sequencer <- EitherT.right[String](
            sequencerFactory.create(
              domainId,
              sequencerId,
              clock,
              clock,
              syncCrypto,
              futureSupervisor,
              config.trafficConfig,
              runtimeReadyPromise.futureUS,
              sequencerSnapshot,
            )
          )
          domainParamsLookup = DomainParametersLookup.forSequencerDomainParameters(
            staticDomainParameters,
            config.publicApi.overrideMaxRequestSize,
            topologyClient,
            futureSupervisor,
            loggerFactory,
          )
          indexedDomain <- EitherT.right[String](
            IndexedDomain.indexed(indexedStringStore)(domainId)
          )
          sequencedEventStore = SequencedEventStore(
            storage,
            indexedDomain,
            staticDomainParameters.protocolVersion,
            timeouts,
            loggerFactory,
          )
          firstSequencerCounterServeableForSequencer <-
            EitherT.right[String](sequencer.firstSequencerCounterServeableForSequencer)

          _ = addCloseable(sequencedEventStore)
          sequencerClient = new SequencerClientImplPekko[
            DirectSequencerClientTransport.SubscriptionError
          ](
            domainId,
            sequencerId,
            SequencerTransports.default(
              sequencerId,
              new DirectSequencerClientTransport(
                sequencer,
                parameters.processingTimeouts,
                loggerFactory,
                staticDomainParameters.protocolVersion,
              ),
            ),
            parameters.sequencerClient,
            arguments.testingConfig,
            staticDomainParameters.protocolVersion,
            domainParamsLookup,
            parameters.processingTimeouts,
            // Since the sequencer runtime trusts itself, there is no point in validating the events.
            SequencedEventValidatorFactory.noValidation(domainId, warn = false),
            clock,
            RequestSigner(syncCrypto, staticDomainParameters.protocolVersion, loggerFactory),
            sequencedEventStore,
            new SendTracker(
              Map(),
              SendTrackerStore(storage),
              arguments.metrics.sequencerClient,
              loggerFactory,
              timeouts,
              None,
              sequencerId,
            ),
            arguments.metrics.sequencerClient,
            None,
            replayEnabled = false,
            syncCrypto,
            parameters.loggingConfig,
            None,
            parameters.exitOnFatalFailures,
            loggerFactory,
            futureSupervisor,
            firstSequencerCounterServeableForSequencer, // TODO(#18401): Review this value
          )
          timeTracker = DomainTimeTracker(
            config.timeTracker,
            clock,
            sequencerClient,
            staticDomainParameters.protocolVersion,
            timeouts,
            loggerFactory,
          )
          _ = topologyClient.setDomainTimeTracker(timeTracker)
          sequencerRuntime = new SequencerRuntime(
            sequencerId,
            sequencer,
            sequencerClient,
            staticDomainParameters,
            parameters,
            config.publicApi,
            timeTracker,
            arguments.metrics,
            indexedDomain,
            syncCrypto,
            domainTopologyManager,
            domainTopologyStore,
            topologyClient,
            topologyProcessor,
            Some(TopologyManagerStatus.combined(authorizedTopologyManager, domainTopologyManager)),
            storage,
            clock,
            SequencerAuthenticationConfig(
              config.publicApi.nonceExpirationInterval,
              config.publicApi.maxTokenExpirationInterval,
            ),
            Seq(sequencerId) ++ membersToRegister,
            futureSupervisor,
            memberAuthServiceFactory,
            new StoreBasedTopologyStateForInitializationService(
              domainTopologyStore,
              domainLoggerFactory,
            ),
            Some(domainOutboxFactory),
            domainLoggerFactory,
            runtimeReadyPromise,
          )
          _ <- sequencerRuntime.initializeAll()
          _ = addCloseable(sequencer)
          server <- createSequencerServer(
            sequencerRuntime,
            domainParamsLookup,
            preinitializedServer,
            healthReporter,
            adminServerRegistry,
          )
        } yield {
          // if close handle hasn't been registered yet, register it now
          if (preinitializedServer.isEmpty) {
            addCloseable(server.publicServer)
          }
          val node = new SequencerNode(
            config,
            clock,
            sequencerRuntime,
            adminToken,
            domainLoggerFactory,
            server,
            (healthService.dependencies ++ sequencerPublicApiHealthService.dependencies).map(
              _.toComponentStatus
            ),
            staticDomainParameters.protocolVersion,
          )
          addCloseable(node)
          Some(new RunningNode(bootstrapStageCallback, node))
        }
      }
    }
  }

  private case class StageResult(
      staticDomainParameters: StaticDomainParameters,
      sequencerFactory: SequencerFactory,
      domainTopologyManager: DomainTopologyManager,
      sequencerSnapshot: Option[SequencerSnapshot],
  )

  // Deferred health component for the sequencer health, created during initialization
  private lazy val sequencerHealth = new MutableHealthQuasiComponent[Sequencer](
    loggerFactory,
    Sequencer.healthName,
    SequencerHealthStatus(isActive = false),
    timeouts,
    SequencerHealthStatus.shutdownStatus,
  )

  // The service exposed by the gRPC health endpoint of sequencer public API
  // This will be used by sequencer clients who perform client-side load balancing to determine sequencer health
  private lazy val sequencerPublicApiHealthService = DependenciesHealthService(
    CantonGrpcUtil.sequencerHealthCheckServiceName,
    logger,
    timeouts,
    criticalDependencies = Seq(sequencerHealth),
  )

  override protected def mkNodeHealthService(
      storage: Storage
  ): (DependenciesHealthService, LivenessHealthService) = {
    val readiness = DependenciesHealthService(
      "sequencer",
      logger,
      timeouts,
      Seq(storage),
    )
    val liveness = LivenessHealthService.alwaysAlive(logger, timeouts)
    (readiness, liveness)
  }

  override protected def bindNodeStatusService(): ServerServiceDefinition =
    SequencerStatusServiceGrpc.bindService(
      new GrpcSequencerStatusService(getNodeStatus, loggerFactory),
      executionContext,
    )

  // Creates a dynamic GRPC server that initially only exposes a health endpoint, and can later be
  // setup with the sequencer runtime to provide the full sequencer API
  private def makeDynamicGrpcServer(
      maxRequestSize: MaxRequestSize,
      grpcHealthReporter: GrpcHealthReporter,
  ) =
    new DynamicGrpcServer(
      loggerFactory,
      maxRequestSize,
      arguments.parameterConfig,
      config.publicApi,
      arguments.metrics.grpcMetrics,
      grpcHealthReporter,
      sequencerPublicApiHealthService,
    )

  private def createSequencerServer(
      runtime: SequencerRuntime,
      domainParamsLookup: DynamicDomainParametersLookup[SequencerDomainParameters],
      server: Option[DynamicGrpcServer],
      healthReporter: GrpcHealthReporter,
      adminServerRegistry: CantonMutableHandlerRegistry,
  ): EitherT[Future, String, DynamicGrpcServer] = {
    runtime.registerAdminGrpcServices(service => adminServerRegistry.addServiceU(service))
    for {
      maxRequestSize <- EitherT
        .right(domainParamsLookup.getApproximate())
        .map(paramsO =>
          paramsO.map(_.maxRequestSize).getOrElse(MaxRequestSize(NonNegativeInt.maxValue))
        )
      sequencerNodeServer = server
        .getOrElse(
          makeDynamicGrpcServer(maxRequestSize, healthReporter)
        )
        .initialize(runtime)
      // wait for the server to be initialized before reporting a serving health state
      _ = sequencerHealth.set(runtime.sequencer)
    } yield sequencerNodeServer
  }
}

class SequencerNode(
    config: SequencerNodeConfigCommon,
    override protected val clock: Clock,
    val sequencer: SequencerRuntime,
    override val adminToken: CantonAdminToken,
    protected val loggerFactory: NamedLoggerFactory,
    sequencerNodeServer: DynamicGrpcServer,
    healthData: => Seq[ComponentStatus],
    protocolVersion: ProtocolVersion,
) extends CantonNode
    with NamedLogging
    with HasUptime {

  override type Status = SequencerNodeStatus

  logger.info(s"Creating sequencer server with public api ${config.publicApi}")(TraceContext.empty)

  override def isActive = true

  override def status: SequencerNodeStatus = {
    val healthStatus = sequencer.health
    val activeMembers = sequencer.fetchActiveMembers()

    val ports = Map("public" -> config.publicApi.port, "admin" -> config.adminApi.port)

    SequencerNodeStatus(
      sequencer.domainId.unwrap,
      sequencer.domainId,
      uptime(),
      ports,
      activeMembers,
      healthStatus,
      topologyQueue = sequencer.topologyQueue,
      admin = sequencer.adminStatus,
      healthData,
      ReleaseVersion.current,
      protocolVersion,
    )
  }

  override def close(): Unit =
    Lifecycle.close(
      sequencer,
      sequencerNodeServer.publicServer,
    )(logger)
}
