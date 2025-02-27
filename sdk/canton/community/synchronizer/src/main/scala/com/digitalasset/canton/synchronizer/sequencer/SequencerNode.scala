// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import cats.data.EitherT
import com.digitalasset.canton.admin.sequencer.v30.SequencerStatusServiceGrpc
import com.digitalasset.canton.auth.CantonAdminToken
import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.config.SessionSigningKeysConfig
import com.digitalasset.canton.connection.GrpcApiInfoService
import com.digitalasset.canton.connection.v30.ApiInfoServiceGrpc
import com.digitalasset.canton.crypto.{Crypto, SynchronizerCryptoClient, SynchronizerCryptoPureApi}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.environment.*
import com.digitalasset.canton.health.*
import com.digitalasset.canton.health.admin.data.{WaitingForExternalInput, WaitingForInitialization}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.lifecycle.{
  FutureUnlessShutdown,
  HasCloseContext,
  LifeCycle,
  PromiseUnlessShutdown,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.{CantonGrpcUtil, CantonMutableHandlerRegistry}
import com.digitalasset.canton.protocol.SynchronizerParameters.MaxRequestSize
import com.digitalasset.canton.protocol.SynchronizerParametersLookup.SequencerSynchronizerParameters
import com.digitalasset.canton.protocol.{
  DynamicSynchronizerParametersLookup,
  StaticSynchronizerParameters,
  SynchronizerParametersLookup,
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
  IndexedStringStore,
  IndexedSynchronizer,
  SendTrackerStore,
  SequencedEventStore,
}
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.admin.data.{
  SequencerHealthStatus,
  SequencerNodeStatus,
}
import com.digitalasset.canton.synchronizer.sequencer.admin.grpc.{
  InitializeSequencerRequest,
  InitializeSequencerResponse,
}
import com.digitalasset.canton.synchronizer.sequencer.config.{
  SequencerNodeConfigCommon,
  SequencerNodeParameters,
}
import com.digitalasset.canton.synchronizer.sequencer.store.{
  SequencerSynchronizerConfiguration,
  SequencerSynchronizerConfigurationStore,
}
import com.digitalasset.canton.synchronizer.sequencing.authentication.MemberAuthenticationServiceFactory
import com.digitalasset.canton.synchronizer.sequencing.service.{
  GrpcSequencerInitializationService,
  GrpcSequencerStatusService,
}
import com.digitalasset.canton.synchronizer.sequencing.topology.{
  SequencedEventStoreBasedTopologyHeadInitializer,
  SequencerSnapshotBasedTopologyHeadInitializer,
}
import com.digitalasset.canton.synchronizer.server.DynamicGrpcServer
import com.digitalasset.canton.time.*
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.SynchronizerTopologyClient
import com.digitalasset.canton.topology.processing.{
  InitialTopologySnapshotValidator,
  TopologyTransactionProcessor,
}
import com.digitalasset.canton.topology.store.StoredTopologyTransactions.GenericStoredTopologyTransactions
import com.digitalasset.canton.topology.store.TopologyStoreId.SynchronizerStore
import com.digitalasset.canton.topology.store.{
  StoreBasedTopologyStateForInitializationService,
  TopologyStore,
  TopologyStoreId,
}
import com.digitalasset.canton.topology.transaction.{
  MediatorSynchronizerState,
  SequencerSynchronizerState,
  SynchronizerTrustCertificate,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{EitherTUtil, SingleUseCell}
import com.digitalasset.canton.version.{ProtocolVersion, ReleaseVersion}
import io.grpc.ServerServiceDefinition
import org.apache.pekko.actor.ActorSystem

import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.atomic.AtomicReference

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

  override def metrics: BaseMetrics = arguments.metrics
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
    new WaitForSequencerToSynchronizerInit(
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

  private val synchronizerTopologyManager = new SingleUseCell[SynchronizerTopologyManager]()
  private val topologyClient = new SingleUseCell[SynchronizerTopologyClient]()

  override protected def sequencedTopologyStores: Seq[TopologyStore[SynchronizerStore]] =
    synchronizerTopologyManager.get.map(_.store).toList

  override protected def sequencedTopologyManagers: Seq[SynchronizerTopologyManager] =
    synchronizerTopologyManager.get.toList

  override protected def lookupTopologyClient(
      storeId: TopologyStoreId
  ): Option[SynchronizerTopologyClient] =
    storeId match {
      case SynchronizerStore(synchronizerId) =>
        topologyClient.get.filter(_.synchronizerId == synchronizerId)
      case _ => None
    }

  private class WaitForSequencerToSynchronizerInit(
      storage: Storage,
      crypto: Crypto,
      adminServerRegistry: CantonMutableHandlerRegistry,
      adminToken: CantonAdminToken,
      sequencerId: SequencerId,
      manager: AuthorizedTopologyManager,
      healthReporter: GrpcHealthReporter,
      healthService: DependenciesHealthService,
  ) extends BootstrapStageWithStorage[SequencerNode, StartupNode, StageResult](
        description = "wait-for-sequencer-to-synchronizer-init",
        bootstrapStageCallback,
        storage,
        config.init.autoInit,
      )
      with GrpcSequencerInitializationService.Callback {

    override def getAdminToken: Option[String] = Some(adminToken.secret)

    // add initialization service
    val (initializationServiceDef, _) = adminServerRegistry.addService(
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
    private val nonInitializedSequencerNodeServer =
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

    /** if node is not initialized, create a dynamic synchronizer server such that we can serve a
      * health end-point until we are initialised
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

    private val synchronizerConfigurationStore =
      SequencerSynchronizerConfigurationStore(storage, timeouts, loggerFactory)

    override protected def stageCompleted(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Option[StageResult]] =
      synchronizerConfigurationStore.fetchConfiguration.toOption
        .map {
          case Some(existing) =>
            Some(
              StageResult(
                existing.synchronizerParameters,
                mkFactory(existing.synchronizerParameters.protocolVersion),
                new SynchronizerTopologyManager(
                  sequencerId.uid,
                  clock,
                  crypto,
                  existing.synchronizerParameters,
                  store = createSynchronizerTopologyStore(
                    existing.synchronizerId,
                    existing.synchronizerParameters.protocolVersion,
                  ),
                  outboxQueue = new SynchronizerOutboxQueue(loggerFactory),
                  exitOnFatalFailures = parameters.exitOnFatalFailures,
                  timeouts,
                  futureSupervisor,
                  loggerFactory,
                ),
                topologyAndSequencerSnapshot = None,
              )
            )
          case None =>
            // create sequencer server such that we can expose a health endpoint until initialized
            initSequencerNodeServer()
            None
        }
        .value
        .map(_.flatten)

    private def finalizeInitialization(
        synchronizerId: SynchronizerId,
        staticSynchronizerParameters: StaticSynchronizerParameters,
    ): EitherT[FutureUnlessShutdown, String, Unit] = {
      logger.info(s"Finalizing initialization for synchronizer $synchronizerId")
      synchronizerConfigurationStore
        .saveConfiguration(
          SequencerSynchronizerConfiguration(
            synchronizerId,
            staticSynchronizerParameters,
          )
        )
        .leftMap(e => s"Unable to save parameters: ${e.toString}")
    }

    private def createSynchronizerTopologyStore(
        synchronizerId: SynchronizerId,
        protocolVersion: ProtocolVersion,
    ): TopologyStore[SynchronizerStore] = {
      val store =
        TopologyStore(
          SynchronizerStore(synchronizerId),
          storage,
          protocolVersion,
          timeouts,
          loggerFactory,
        )
      addCloseable(store)
      store
    }

    override protected def buildNextStage(
        result: StageResult
    ): EitherT[FutureUnlessShutdown, String, StartupNode] =
      for {
        _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
          synchronizerTopologyManager.putIfAbsent(result.synchronizerTopologyManager).isEmpty,
          "Unexpected state during initialization: synchronizer topology manager shouldn't have been set before",
        )
      } yield {
        adminServerRegistry.removeServiceU(initializationServiceDef)
        new StartupNode(
          storage,
          crypto,
          adminServerRegistry,
          adminToken,
          sequencerId,
          result.sequencerFactory,
          result.staticSynchronizerParameters,
          manager,
          result.synchronizerTopologyManager,
          nonInitializedSequencerNodeServer.getAndSet(None),
          result.topologyAndSequencerSnapshot,
          () =>
            finalizeInitialization(
              result.synchronizerTopologyManager.synchronizerId,
              result.staticSynchronizerParameters,
            ),
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
            s"Assigning sequencer to synchronizer ${if (request.sequencerSnapshot.isEmpty) "from beginning"
              else "with existing snapshot"}"
          )
          val sequencerFactory = mkFactory(request.synchronizerParameters.protocolVersion)
          val synchronizerIds = request.topologySnapshot.result
            .map(_.mapping)
            .collect { case SequencerSynchronizerState(synchronizer, _, _, _) => synchronizer }
            .toSet
          for {
            synchronizerId <- EitherT.fromEither[FutureUnlessShutdown](
              synchronizerIds.headOption.toRight(
                "No synchronizer id within topology state defined!"
              )
            )
            store = createSynchronizerTopologyStore(
              synchronizerId,
              request.synchronizerParameters.protocolVersion,
            )
            outboxQueue = new SynchronizerOutboxQueue(loggerFactory)
            topologyManager = new SynchronizerTopologyManager(
              sequencerId.uid,
              clock,
              crypto,
              request.synchronizerParameters,
              store,
              outboxQueue,
              exitOnFatalFailures = parameters.exitOnFatalFailures,
              timeouts,
              futureSupervisor,
              loggerFactory,
            )
          } yield StageResult(
            request.synchronizerParameters,
            sequencerFactory,
            topologyManager,
            Some(request.topologySnapshot -> request.sequencerSnapshot),
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
      staticSynchronizerParameters: StaticSynchronizerParameters,
      authorizedTopologyManager: AuthorizedTopologyManager,
      synchronizerTopologyManager: SynchronizerTopologyManager,
      preinitializedServer: Option[DynamicGrpcServer],
      topologyAndSequencerSnapshot: Option[
        (GenericStoredTopologyTransactions, Option[SequencerSnapshot])
      ],
      finalizeInitialization: () => EitherT[FutureUnlessShutdown, String, Unit],
      healthReporter: GrpcHealthReporter,
      healthService: DependenciesHealthService,
  ) extends BootstrapStage[SequencerNode, RunningNode[SequencerNode]](
        description = "Startup sequencer node",
        bootstrapStageCallback,
      )
      with HasCloseContext {
    override def getAdminToken: Option[String] = Some(adminToken.secret)
    // save one argument and grab the synchronizerId from the store ...
    private val synchronizerId = synchronizerTopologyManager.synchronizerId
    private val synchronizerLoggerFactory =
      loggerFactory.append("synchronizerId", synchronizerId.toString)

    preinitializedServer.foreach(x => addCloseable(x.publicServer))

    override protected def attempt()(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, String, Option[RunningNode[SequencerNode]]] = {

      val synchronizerOutboxFactory = new SynchronizerOutboxFactorySingleCreate(
        synchronizerId,
        sequencerId,
        authorizedTopologyManager,
        synchronizerTopologyManager,
        crypto,
        config.topology,
        timeouts,
        arguments.futureSupervisor,
        loggerFactory,
      )

      val synchronizerTopologyStore = synchronizerTopologyManager.store

      addCloseable(synchronizerOutboxFactory)

      performUnlessClosingEitherUSF("starting up runtime") {
        val indexedStringStore = IndexedStringStore.create(
          storage,
          parameters.cachingConfigs.indexedStrings,
          timeouts,
          synchronizerLoggerFactory,
        )
        addCloseable(indexedStringStore)
        for {
          indexedSynchronizer <- EitherT.right[String](
            IndexedSynchronizer.indexed(indexedStringStore)(synchronizerId)
          )
          sequencedEventStore = SequencedEventStore(
            storage,
            indexedSynchronizer,
            staticSynchronizerParameters.protocolVersion,
            timeouts,
            loggerFactory,
          )

          membersToRegister <- {
            topologyAndSequencerSnapshot match {
              case None =>
                EitherT.rightT[FutureUnlessShutdown, String](Set.empty[Member])
              case Some((initialTopologyTransactions, sequencerSnapshot)) =>
                val topologySnapshotValidator = new InitialTopologySnapshotValidator(
                  synchronizerId,
                  staticSynchronizerParameters.protocolVersion,
                  new SynchronizerCryptoPureApi(staticSynchronizerParameters, crypto.pureCrypto),
                  synchronizerTopologyStore,
                  config.topology.insecureIgnoreMissingExtraKeySignaturesInInitialSnapshot,
                  parameters.processingTimeouts,
                  loggerFactory,
                )
                for {
                  _ <- topologySnapshotValidator.validateAndApplyInitialTopologySnapshot(
                    initialTopologyTransactions
                  )
                  _ <- sequencerSnapshot
                    .map { snapshot =>
                      logger.debug("Uploading sequencer snapshot to sequencer driver")
                      val initialState = SequencerInitialState(
                        synchronizerId,
                        snapshot,
                        initialTopologyTransactions.result.view
                          .map(tx => (tx.sequenced.value, tx.validFrom.value)),
                      )
                      // TODO(#14070) make initialize idempotent to support crash recovery during init
                      sequencerFactory
                        .initialize(initialState, sequencerId)
                    }
                    .getOrElse {
                      logger.debug("No sequencer snapshot provided for initialization")
                      EitherT.rightT[FutureUnlessShutdown, String](())
                    }
                  _ <- finalizeInitialization()

                  // This sequencer node was started for the first time and initialized with a topology state.
                  // Therefore, we fetch all members who have a registered role on the synchronizer and pass them
                  // to the underlying sequencer driver to register them as known members.
                  transactions <- EitherT.right[String](
                    synchronizerTopologyStore
                      .findPositiveTransactions(
                        CantonTimestamp.MaxValue,
                        asOfInclusive = false,
                        isProposal = false,
                        types = Seq(
                          SynchronizerTrustCertificate.code,
                          SequencerSynchronizerState.code,
                          MediatorSynchronizerState.code,
                        ),
                        filterUid = None,
                        filterNamespace = None,
                      )
                  )
                } yield {
                  val participants = transactions
                    .collectOfMapping[SynchronizerTrustCertificate]
                    .collectLatestByUniqueKey
                    .toTopologyState
                    .map(_.participantId)
                    .toSet
                  val sequencers = transactions
                    .collectOfMapping[SequencerSynchronizerState]
                    .collectLatestByUniqueKey
                    .toTopologyState
                    .flatMap(sds => sds.active.forgetNE ++ sds.observers)
                    .toSet
                  val mediators = transactions
                    .collectOfMapping[MediatorSynchronizerState]
                    .collectLatestByUniqueKey
                    .toTopologyState
                    .flatMap(mds => mds.active.forgetNE ++ mds.observers)
                    .toSet
                  participants ++ sequencers ++ mediators
                }
            }
          }

          topologyHeadInitializer = topologyAndSequencerSnapshot.flatMap(_._2) match {
            case Some(snapshot) =>
              new SequencerSnapshotBasedTopologyHeadInitializer(snapshot, synchronizerTopologyStore)
            case None =>
              new SequencedEventStoreBasedTopologyHeadInitializer(
                sequencedEventStore,
                synchronizerTopologyStore,
              )
          }
          processorAndClient <- EitherT
            .right(
              TopologyTransactionProcessor.createProcessorAndClientForSynchronizer(
                synchronizerTopologyStore,
                synchronizerId,
                new SynchronizerCryptoPureApi(staticSynchronizerParameters, crypto.pureCrypto),
                parameters,
                clock,
                futureSupervisor,
                synchronizerLoggerFactory,
              )(topologyHeadInitializer)
            )
          (topologyProcessor, topologyClient) = processorAndClient
          _ = addCloseable(topologyProcessor)
          _ = addCloseable(topologyClient)
          _ = ips.add(topologyClient)
          _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
            SequencerNodeBootstrap.this.topologyClient.putIfAbsent(topologyClient).isEmpty,
            "Unexpected state during initialization: topology client shouldn't have been set before",
          )

          memberAuthServiceFactory = MemberAuthenticationServiceFactory(
            synchronizerId,
            clock,
            config.publicApi.nonceExpirationInterval.asJava,
            config.publicApi.maxTokenExpirationInterval.asJava,
            useExponentialRandomTokenExpiration =
              config.publicApi.useExponentialRandomTokenExpiration,
            parameters.processingTimeouts,
            synchronizerLoggerFactory,
            topologyProcessor,
          )

          // Session signing keys are used only if they are configured in Canton's configuration file.
          syncCryptoWithOptionalSessionKeys = SynchronizerCryptoClient
            .createWithOptionalSessionKeys(
              sequencerId,
              synchronizerId,
              topologyClient,
              staticSynchronizerParameters,
              crypto,
              new SynchronizerCryptoPureApi(staticSynchronizerParameters, crypto.pureCrypto),
              // TODO(#22362): Enable correct config
              // parameters.sessionSigningKeys
              SessionSigningKeysConfig.disabled,
              parameters.batchingConfig.parallelism.unwrap,
              parameters.processingTimeouts,
              futureSupervisor,
              loggerFactory,
            )
          runtimeReadyPromise = PromiseUnlessShutdown.supervised[Unit](
            "sequencer-runtime-ready",
            futureSupervisor,
          )
          sequencer <- EitherT
            .right[String](
              sequencerFactory.create(
                synchronizerId,
                sequencerId,
                clock,
                clock,
                syncCryptoWithOptionalSessionKeys,
                futureSupervisor,
                config.trafficConfig,
                runtimeReadyPromise.futureUS,
                topologyAndSequencerSnapshot.flatMap { case (_, sequencerSnapshot) =>
                  sequencerSnapshot
                },
              )
            )
          synchronizerParamsLookup = SynchronizerParametersLookup
            .forSequencerSynchronizerParameters(
              staticSynchronizerParameters,
              config.publicApi.overrideMaxRequestSize,
              topologyClient,
              loggerFactory,
            )
          firstSequencerCounterServeableForSequencer <-
            EitherT
              .right[String](sequencer.firstSequencerCounterServeableForSequencer)

          _ = addCloseable(sequencedEventStore)
          sequencerClient = new SequencerClientImplPekko[
            DirectSequencerClientTransport.SubscriptionError
          ](
            synchronizerId,
            sequencerId,
            SequencerTransports.default(
              sequencerId,
              new DirectSequencerClientTransport(
                sequencer,
                parameters.processingTimeouts,
                loggerFactory,
                staticSynchronizerParameters.protocolVersion,
              ),
            ),
            parameters.sequencerClient,
            arguments.testingConfig,
            staticSynchronizerParameters.protocolVersion,
            synchronizerParamsLookup,
            parameters.processingTimeouts,
            // Since the sequencer runtime trusts itself, there is no point in validating the events.
            SequencedEventValidatorFactory.noValidation(synchronizerId, warn = false),
            clock,
            RequestSigner(
              syncCryptoWithOptionalSessionKeys,
              staticSynchronizerParameters.protocolVersion,
              loggerFactory,
            ),
            sequencedEventStore,
            new SendTracker(
              Map(),
              SendTrackerStore(storage),
              arguments.metrics.sequencerClient,
              loggerFactory,
              timeouts,
              None,
            ),
            arguments.metrics.sequencerClient,
            None,
            replayEnabled = false,
            syncCryptoWithOptionalSessionKeys,
            parameters.loggingConfig,
            None,
            parameters.exitOnFatalFailures,
            loggerFactory,
            futureSupervisor,
            firstSequencerCounterServeableForSequencer, // TODO(#18401): Review this value
          )
          timeTracker = SynchronizerTimeTracker(
            config.timeTracker,
            clock,
            sequencerClient,
            staticSynchronizerParameters.protocolVersion,
            timeouts,
            loggerFactory,
          )
          _ = topologyClient.setSynchronizerTimeTracker(timeTracker)

          // sequencer authentication uses a different set of signing keys and thus should not use session keys
          syncCryptoForAuthentication = SynchronizerCryptoClient.create(
            sequencerId,
            synchronizerId,
            topologyClient,
            staticSynchronizerParameters,
            crypto,
            new SynchronizerCryptoPureApi(staticSynchronizerParameters, crypto.pureCrypto),
            parameters.batchingConfig.parallelism.unwrap,
            parameters.processingTimeouts,
            futureSupervisor,
            loggerFactory,
          )
          sequencerRuntime = new SequencerRuntime(
            sequencerId,
            sequencer,
            sequencerClient,
            staticSynchronizerParameters,
            parameters,
            config.publicApi,
            timeTracker,
            arguments.metrics,
            indexedSynchronizer,
            syncCryptoWithOptionalSessionKeys,
            syncCryptoForAuthentication,
            synchronizerTopologyManager,
            synchronizerTopologyStore,
            topologyClient,
            topologyProcessor,
            Some(
              TopologyManagerStatus.combined(authorizedTopologyManager, synchronizerTopologyManager)
            ),
            storage,
            clock,
            SequencerAuthenticationConfig(
              config.publicApi.nonceExpirationInterval,
              config.publicApi.maxTokenExpirationInterval,
            ),
            Seq(sequencerId) ++ membersToRegister,
            memberAuthServiceFactory,
            new StoreBasedTopologyStateForInitializationService(
              synchronizerTopologyStore,
              synchronizerLoggerFactory,
            ),
            Some(synchronizerOutboxFactory),
            synchronizerLoggerFactory,
            runtimeReadyPromise,
          )
          _ <- sequencerRuntime.initializeAll()
          _ = addCloseable(sequencer)
          server <- createSequencerServer(
            sequencerRuntime,
            synchronizerParamsLookup,
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
            synchronizerLoggerFactory,
            server,
            (healthService.dependencies ++ sequencerPublicApiHealthService.dependencies).map(
              _.toComponentStatus
            ),
            staticSynchronizerParameters.protocolVersion,
          )
          addCloseable(node)
          Some(new RunningNode(bootstrapStageCallback, node))
        }
      }
    }
  }

  private case class StageResult(
      staticSynchronizerParameters: StaticSynchronizerParameters,
      sequencerFactory: SequencerFactory,
      synchronizerTopologyManager: SynchronizerTopologyManager,
      topologyAndSequencerSnapshot: Option[
        (GenericStoredTopologyTransactions, Option[SequencerSnapshot])
      ],
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
      synchronizerParamsLookup: DynamicSynchronizerParametersLookup[
        SequencerSynchronizerParameters
      ],
      server: Option[DynamicGrpcServer],
      healthReporter: GrpcHealthReporter,
      adminServerRegistry: CantonMutableHandlerRegistry,
  ): EitherT[FutureUnlessShutdown, String, DynamicGrpcServer] = {
    runtime.registerAdminGrpcServices(service => adminServerRegistry.addServiceU(service))
    for {
      maxRequestSize <- EitherT
        .right(synchronizerParamsLookup.getApproximate())
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
      sequencer.synchronizerId.unwrap,
      sequencer.synchronizerId,
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
    LifeCycle.close(
      sequencer,
      sequencerNodeServer.publicServer,
    )(logger)
}
