// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import cats.data.EitherT
import com.digitalasset.canton.admin.sequencer.v30.SequencerStatusServiceGrpc
import com.digitalasset.canton.auth.CantonAdminTokenDispenser
import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.config.AdminTokenConfig
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.connection.GrpcApiInfoService
import com.digitalasset.canton.connection.v30.ApiInfoServiceGrpc
import com.digitalasset.canton.crypto.{Crypto, SynchronizerCrypto, SynchronizerCryptoClient}
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
import com.digitalasset.canton.networking.grpc.ratelimiting.ActiveRequestCounterInterceptor
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
  IndexedPhysicalSynchronizer,
  IndexedStringStore,
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
  SequencerNodeConfig,
  SequencerNodeParameters,
}
import com.digitalasset.canton.synchronizer.sequencer.store.{
  SequencerSynchronizerConfiguration,
  SequencerSynchronizerConfigurationStore,
}
import com.digitalasset.canton.synchronizer.sequencing.authentication.grpc.SequencerAuthenticationServerInterceptor
import com.digitalasset.canton.synchronizer.sequencing.authentication.{
  MemberAuthenticationServiceFactory,
  MemberAuthenticationStore,
}
import com.digitalasset.canton.synchronizer.sequencing.service.channel.GrpcSequencerChannelService
import com.digitalasset.canton.synchronizer.sequencing.service.{
  GrpcSequencerAuthenticationService,
  GrpcSequencerInitializationService,
  GrpcSequencerService,
  GrpcSequencerStatusService,
}
import com.digitalasset.canton.synchronizer.sequencing.topology.{
  SequencedEventStoreBasedTopologyHeadInitializer,
  SequencerSnapshotBasedTopologyHeadInitializer,
}
import com.digitalasset.canton.synchronizer.server.DynamicGrpcServer
import com.digitalasset.canton.time.*
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.admin.grpc.PSIdLookup
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
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.{EitherTUtil, SingleUseCell}
import com.digitalasset.canton.version.{ProtocolVersion, ReleaseVersion}
import com.google.common.annotations.VisibleForTesting
import io.grpc.ServerServiceDefinition
import org.apache.pekko.actor.ActorSystem

import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.atomic.AtomicReference

object SequencerNodeBootstrap {

  val LoggerFactoryKeyName: String = "sequencer"
}

class SequencerNodeBootstrap(
    arguments: CantonNodeBootstrapCommonArguments[
      SequencerNodeConfig,
      SequencerNodeParameters,
      SequencerMetrics,
    ]
)(implicit
    executionContext: ExecutionContextIdlenessExecutorService,
    scheduler: ScheduledExecutorService,
    actorSystem: ActorSystem,
) extends CantonNodeBootstrapImpl[
      SequencerNode,
      SequencerNodeConfig,
      SequencerNodeParameters,
      SequencerMetrics,
    ](arguments) {

  override def metrics: BaseMetrics = arguments.metrics
  override protected def member(uid: UniqueIdentifier): Member = SequencerId(uid)

  override protected def customNodeStages(
      storage: Storage,
      indexedStringStore: IndexedStringStore,
      crypto: Crypto,
      adminServerRegistry: CantonMutableHandlerRegistry,
      adminTokenDispenser: CantonAdminTokenDispenser,
      nodeId: UniqueIdentifier,
      manager: AuthorizedTopologyManager,
      healthReporter: GrpcHealthReporter,
      healthService: DependenciesHealthService,
  ): BootstrapStageOrLeaf[SequencerNode] =
    new WaitForSequencerToSynchronizerInit(
      storage,
      indexedStringStore,
      crypto,
      adminServerRegistry,
      adminTokenDispenser,
      SequencerId(nodeId),
      manager,
      healthReporter,
      healthService,
    )

  override protected val adminTokenConfig: AdminTokenConfig =
    config.adminApi.adminTokenConfig

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
        topologyClient.get.filter(_.psid == synchronizerId)
      case _ => None
    }

  override protected lazy val lookupActivePSId: PSIdLookup =
    synchronizerId =>
      synchronizerTopologyManager.get
        .map(_.psid)
        .filter(_.logical == synchronizerId)

  private class WaitForSequencerToSynchronizerInit(
      storage: Storage,
      indexedStringStore: IndexedStringStore,
      crypto: Crypto,
      adminServerRegistry: CantonMutableHandlerRegistry,
      adminTokenDispenser: CantonAdminTokenDispenser,
      sequencerId: SequencerId,
      manager: AuthorizedTopologyManager,
      healthReporter: GrpcHealthReporter,
      healthService: DependenciesHealthService,
  ) extends BootstrapStageWithStorage[SequencerNode, StartupNode, StageResult](
        description = "wait-for-sequencer-to-synchronizer-init",
        bootstrapStageCallback,
        storage,
        false, // has no auto-init
      )
      with GrpcSequencerInitializationService.Callback {

    override def getAdminToken: Option[String] = Some(adminTokenDispenser.getCurrentToken.secret)

    // add initialization service
    private val (initializationServiceDef, _) = adminServerRegistry.addService(
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
    // TODO(#25118) clean up manual cache removal
    addCloseable(new AutoCloseable {
      override def close(): Unit = {
        // This is a pretty ugly work around for the fact that cache metrics are left dangling and
        // are not closed properly
        arguments.metrics.trafficControl.purchaseCache.closeAcquired()
        arguments.metrics.trafficControl.consumedCache.closeAcquired()
        arguments.metrics.eventBuffer.closeAcquired()
        arguments.metrics.memberCache.closeAcquired()
        arguments.metrics.payloadCache.closeAcquired()
      }
    })

    private def createSequencerFactory(
        protocolVersion: ProtocolVersion
    )(implicit traceContext: TraceContext) = {
      logger.debug(s"Creating sequencer factory with ${config.sequencer}")
      val factory = SequencerMetaFactory.createFactory(
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
      )(config.sequencer, config.topology.useTimeProofsToObserveEffectiveTime)
      addCloseable(factory)
      factory
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
      synchronizerConfigurationStore.fetchConfiguration.toOption.flatMapF {
        case Some(existing) =>
          createTopologyManager(existing.synchronizerId, existing.synchronizerParameters).map {
            topologyManager =>
              Some(
                StageResult(
                  existing.synchronizerParameters,
                  createSequencerFactory(existing.synchronizerParameters.protocolVersion),
                  topologyManager,
                  topologyAndSequencerSnapshot = None,
                )
              )
          }
        case None =>
          // create sequencer server such that we can expose a health endpoint until initialized
          initSequencerNodeServer()
          FutureUnlessShutdown.pure(None)
      }.value

    private def finalizeInitialization(
        synchronizerId: PhysicalSynchronizerId,
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
        synchronizerId: PhysicalSynchronizerId
    ): FutureUnlessShutdown[TopologyStore[SynchronizerStore]] =
      TopologyStore
        .create(
          SynchronizerStore(synchronizerId),
          storage,
          indexedStringStore,
          synchronizerId.protocolVersion,
          timeouts,
          parameters.batchingConfig,
          loggerFactory,
        )
        .map { store =>
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
          indexedStringStore,
          SynchronizerCrypto(crypto, result.staticSynchronizerParameters),
          adminServerRegistry,
          adminTokenDispenser,
          sequencerId,
          result.sequencerFactory,
          result.staticSynchronizerParameters,
          manager,
          result.synchronizerTopologyManager,
          nonInitializedSequencerNodeServer.getAndSet(None),
          result.topologyAndSequencerSnapshot,
          () =>
            finalizeInitialization(
              result.synchronizerTopologyManager.psid,
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
          val sequencerFactory =
            createSequencerFactory(request.synchronizerParameters.protocolVersion)
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
            topologyManager <- EitherT.right(
              createTopologyManager(
                PhysicalSynchronizerId(synchronizerId, request.synchronizerParameters),
                request.synchronizerParameters,
              )
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

    private def createTopologyManager(
        synchronizerId: PhysicalSynchronizerId,
        synchronizerParameters: StaticSynchronizerParameters,
    ): FutureUnlessShutdown[SynchronizerTopologyManager] =
      createSynchronizerTopologyStore(synchronizerId).map(topologyStore =>
        new SynchronizerTopologyManager(
          sequencerId.uid,
          clock,
          SynchronizerCrypto(crypto, synchronizerParameters),
          synchronizerParameters,
          topologyStore,
          outboxQueue = new SynchronizerOutboxQueue(loggerFactory),
          dispatchQueueBackpressureLimit = parameters.general.dispatchQueueBackpressureLimit,
          disableOptionalTopologyChecks = config.topology.disableOptionalTopologyChecks,
          exitOnFatalFailures = parameters.exitOnFatalFailures,
          timeouts,
          futureSupervisor,
          loggerFactory,
        )
      )

  }

  private class StartupNode(
      storage: Storage,
      indexedStringStore: IndexedStringStore,
      crypto: SynchronizerCrypto,
      adminServerRegistry: CantonMutableHandlerRegistry,
      adminTokenDispenser: CantonAdminTokenDispenser,
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
    override def getAdminToken: Option[String] = Some(adminTokenDispenser.getCurrentToken.secret)
    // save one argument and grab the synchronizerId from the store ...
    private val psid = synchronizerTopologyManager.psid
    private val synchronizerLoggerFactory =
      loggerFactory.append("psid", psid.toString)

    preinitializedServer.foreach(x => addCloseable(x.publicServer))

    override protected def attempt()(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, String, Option[RunningNode[SequencerNode]]] = {

      val synchronizerOutboxFactory = new SynchronizerOutboxFactorySingleCreate(
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

      synchronizeWithClosing("starting up runtime") {

        for {
          physicalSynchronizerIdx <- EitherT.right[String](
            IndexedPhysicalSynchronizer.indexed(indexedStringStore)(psid)
          )

          sequencedEventStore = SequencedEventStore(
            storage,
            physicalSynchronizerIdx,
            timeouts,
            loggerFactory,
          )

          membersToRegister <- {
            topologyAndSequencerSnapshot match {
              case None =>
                EitherT.rightT[FutureUnlessShutdown, String](Set.empty[Member])
              case Some((initialTopologyTransactions, sequencerSnapshot)) =>
                val topologySnapshotValidator = new InitialTopologySnapshotValidator(
                  crypto.pureCrypto,
                  synchronizerTopologyStore,
                  Some(crypto.staticSynchronizerParameters),
                  validateInitialSnapshot = config.topology.validateInitialTopologySnapshot,
                  loggerFactory,
                  // only filter out completed proposals if this is a bootstrap from genesis.
                  cleanupTopologySnapshot = sequencerSnapshot.isEmpty,
                )
                for {
                  _ <- topologySnapshotValidator.validateAndApplyInitialTopologySnapshot(
                    initialTopologyTransactions
                  )
                  _ <- sequencerSnapshot
                    .map { snapshot =>
                      logger.debug("Uploading sequencer snapshot to sequencer driver")
                      val initialState = SequencerInitialState(
                        psid,
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
                synchronizerPredecessor = None,
                crypto.pureCrypto,
                parameters,
                arguments.config.topology,
                clock,
                crypto.staticSynchronizerParameters,
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
            psid,
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
              psid,
              topologyClient,
              staticSynchronizerParameters,
              crypto,
              cryptoConfig,
              parameters.batchingConfig.parallelism,
              parameters.cachingConfigs.publicKeyConversionCache,
              parameters.processingTimeouts,
              futureSupervisor,
              loggerFactory,
            )
          runtimeReadyPromise = PromiseUnlessShutdown.supervised[Unit](
            "sequencer-runtime-ready",
            futureSupervisor,
          )

          // sequencer authentication uses a different set of signing keys and thus should not use session keys
          syncCryptoForAuthentication = SynchronizerCryptoClient.create(
            sequencerId,
            psid.logical,
            topologyClient,
            staticSynchronizerParameters,
            crypto,
            parameters.batchingConfig.parallelism,
            parameters.cachingConfigs.publicKeyConversionCache,
            parameters.processingTimeouts,
            futureSupervisor,
            loggerFactory,
          )

          authenticationConfig = SequencerAuthenticationConfig(
            config.publicApi.nonceExpirationInterval,
            config.publicApi.maxTokenExpirationInterval,
          )

          synchronizerParamsLookup = SynchronizerParametersLookup
            .forSequencerSynchronizerParameters(
              config.publicApi.overrideMaxRequestSize,
              topologyClient,
              loggerFactory,
            )

          topologyStateForInitializationService =
            new StoreBasedTopologyStateForInitializationService(
              synchronizerTopologyStore,
              config.parameters.sequencingTimeLowerBoundExclusive,
              synchronizerLoggerFactory,
            )

          sequencerSynchronizerParamsLookup: DynamicSynchronizerParametersLookup[
            SequencerSynchronizerParameters
          ] =
            SynchronizerParametersLookup.forSequencerSynchronizerParameters(
              config.publicApi.overrideMaxRequestSize,
              topologyClient,
              loggerFactory,
            )

          sequencerChannelServiceO = Option.when(
            parameters.unsafeEnableOnlinePartyReplication
          )(
            GrpcSequencerChannelService(
              authenticationConfig.check,
              clock,
              staticSynchronizerParameters.protocolVersion,
              parameters.processingTimeouts,
              loggerFactory,
            )
          )

          sequencerServiceCell = new SingleUseCell[GrpcSequencerService]

          authenticationServices = {
            val authenticationService = memberAuthServiceFactory.createAndSubscribe(
              syncCryptoForAuthentication,
              new MemberAuthenticationStore(),
              // closing the subscription when the token expires will force the client to try to reconnect
              // immediately and notice it is unauthenticated, which will cause it to also start re-authenticating
              // it's important to disconnect the member AFTER we expired the token, as otherwise, the member
              // can still re-subscribe with the token just before we removed it
              Traced.lift { case (member, tc) =>
                sequencerServiceCell
                  .getOrElse(throw new IllegalStateException("sequencer service not initialized"))
                  .disconnectMember(member)(tc)
                sequencerChannelServiceO.foreach(_.disconnectMember(member)(tc))
              },
              runtimeReadyPromise.futureUS.map(_ =>
                ()
              ), // on shutdown, MemberAuthenticationStore will be closed via closeContext
            )

            val sequencerAuthenticationService =
              new GrpcSequencerAuthenticationService(
                authenticationService,
                staticSynchronizerParameters.protocolVersion,
                loggerFactory,
              )

            val sequencerAuthInterceptor =
              new SequencerAuthenticationServerInterceptor(authenticationService, loggerFactory)

            AuthenticationServices(
              syncCryptoForAuthentication,
              authenticationService,
              sequencerAuthenticationService,
              sequencerAuthInterceptor,
            )
          }

          sequencer <- EitherT
            .right[String](
              sequencerFactory.create(
                sequencerId,
                clock,
                syncCryptoWithOptionalSessionKeys,
                futureSupervisor,
                config.trafficConfig,
                runtimeReadyPromise.futureUS,
                topologyAndSequencerSnapshot.flatMap { case (_, sequencerSnapshot) =>
                  sequencerSnapshot
                },
                Some(authenticationServices),
              )
            )

          sequencerService = GrpcSequencerService(
            sequencer,
            arguments.metrics,
            authenticationConfig.check,
            clock,
            sequencerSynchronizerParamsLookup,
            parameters,
            staticSynchronizerParameters.protocolVersion,
            topologyStateForInitializationService,
            loggerFactory,
            config.acknowledgementsConflateWindow,
          )
          _ = sequencerServiceCell.putIfAbsent(sequencerService)

          directPool = new DirectSequencerConnectionXPool(
            sequencer,
            psid,
            sequencerId,
            staticSynchronizerParameters,
            parameters.processingTimeouts,
            loggerFactory,
          )

          _ = addCloseable(sequencedEventStore)
          sequencerClient = new SequencerClientImplPekko[
            DirectSequencerClientTransport.SubscriptionError
          ](
            psid,
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
            connectionPool = directPool,
            parameters.sequencerClient,
            arguments.testingConfig,
            synchronizerParamsLookup,
            parameters.processingTimeouts,
            // Since the sequencer runtime trusts itself, there is no point in validating the events.
            SequencedEventValidatorFactory.noValidation(psid, warn = false),
            clock,
            RequestSigner(
              syncCryptoWithOptionalSessionKeys,
              staticSynchronizerParameters.protocolVersion,
              loggerFactory,
            ),
            sequencedEventStore,
            new SendTracker(
              Map(),
              SendTrackerStore(),
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
          )
          timeTracker = SynchronizerTimeTracker(
            config.timeTracker,
            clock,
            sequencerClient,
            timeouts,
            loggerFactory,
          )
          _ = topologyClient.setSynchronizerTimeTracker(timeTracker)

          sequencerRuntime = new SequencerRuntime(
            sequencerId,
            sequencer,
            sequencerClient,
            staticSynchronizerParameters,
            parameters,
            timeTracker,
            arguments.metrics,
            physicalSynchronizerIdx,
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
            Seq(sequencerId) ++ membersToRegister,
            authenticationServices,
            sequencerService,
            sequencerChannelServiceO,
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
            adminTokenDispenser,
            synchronizerLoggerFactory,
            server,
            (healthService.dependencies ++ sequencerPublicApiHealthService.dependencies).map(
              _.toComponentStatus
            ),
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
    // We use the storage as a fatal dependency so that we transition liveness to NOT_SERVING if
    // the storage fails continuously for longer than `failedToFatalDelay`.
    val liveness = LivenessHealthService(logger, timeouts, fatalDependencies = Seq(storage))
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
    config: SequencerNodeConfig,
    override protected val clock: Clock,
    val sequencer: SequencerRuntime,
    override val adminTokenDispenser: CantonAdminTokenDispenser,
    protected val loggerFactory: NamedLoggerFactory,
    sequencerNodeServer: DynamicGrpcServer,
    healthData: => Seq[ComponentStatus],
) extends CantonNode
    with NamedLogging
    with HasUptime {

  // Provide access such that it can be modified in tests
  @VisibleForTesting
  def activeRequestCounter: Option[ActiveRequestCounterInterceptor] =
    sequencerNodeServer.activeRequestCounter

  override type Status = SequencerNodeStatus

  logger.info(s"Creating sequencer server with public api ${config.publicApi}")(TraceContext.empty)

  override def isActive = true

  override def status: SequencerNodeStatus = {
    val healthStatus = sequencer.health
    val activeMembers = sequencer.fetchActiveMembers()

    val ports = Map("public" -> config.publicApi.port, "admin" -> config.adminApi.port)

    SequencerNodeStatus(
      sequencer.psid.logical.unwrap,
      sequencer.psid,
      uptime(),
      ports,
      activeMembers,
      healthStatus,
      topologyQueue = sequencer.topologyQueue,
      admin = sequencer.adminStatus,
      healthData,
      ReleaseVersion.current,
    )
  }

  override def close(): Unit =
    LifeCycle.close(
      sequencer,
      sequencerNodeServer.publicServer,
    )(logger)
}
