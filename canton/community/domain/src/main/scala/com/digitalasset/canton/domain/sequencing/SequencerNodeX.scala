// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing

import cats.data.EitherT
import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.config.NonNegativeFiniteDuration as _
import com.digitalasset.canton.crypto.Crypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.admin.v30.SequencerInitializationServiceGrpc
import com.digitalasset.canton.domain.metrics.SequencerMetrics
import com.digitalasset.canton.domain.sequencing.admin.grpc.{
  InitializeSequencerRequestX,
  InitializeSequencerResponseX,
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
import com.digitalasset.canton.domain.sequencing.service.GrpcSequencerInitializationServiceX
import com.digitalasset.canton.domain.sequencing.traffic.store.TrafficLimitsStore
import com.digitalasset.canton.domain.sequencing.traffic.{
  EnterpriseSequencerRateLimitManager,
  TopologyTransactionTrafficSubscription,
}
import com.digitalasset.canton.domain.server.DynamicDomainGrpcServer
import com.digitalasset.canton.environment.*
import com.digitalasset.canton.health.{ComponentStatus, GrpcHealthReporter, HealthService}
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, HasCloseContext}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.protocol.DomainParameters.MaxRequestSize
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.store.IndexedStringStore
import com.digitalasset.canton.time.*
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.processing.{EffectiveTime, TopologyTransactionProcessorX}
import com.digitalasset.canton.topology.store.StoredTopologyTransactionsX.GenericStoredTopologyTransactionsX
import com.digitalasset.canton.topology.store.TopologyStoreId.DomainStore
import com.digitalasset.canton.topology.store.{
  StoreBasedTopologyStateForInitializationService,
  TopologyStoreX,
}
import com.digitalasset.canton.topology.transaction.{
  OwnerToKeyMappingX,
  SequencerDomainStateX,
  TrafficControlStateX,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.traffic.TopUpEvent
import com.digitalasset.canton.util.SingleUseCell
import com.digitalasset.canton.version.ProtocolVersion
import io.grpc.ServerServiceDefinition
import org.apache.pekko.actor.ActorSystem

import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContextExecutorService, Future}

object SequencerNodeBootstrapX {
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
    ): Either[String, SequencerNodeBootstrapX]
  }

  val LoggerFactoryKeyName: String = "sequencerx"
}

class SequencerNodeBootstrapX(
    arguments: CantonNodeBootstrapCommonArguments[
      SequencerNodeConfigCommon,
      SequencerNodeParameters,
      SequencerMetrics,
    ],
    mkSequencerFactory: MkSequencerFactory,
    override protected val createEnterpriseAdminService: (
        Sequencer,
        NamedLoggerFactory,
    ) => Option[ServerServiceDefinition],
)(implicit
    executionContext: ExecutionContextIdlenessExecutorService,
    scheduler: ScheduledExecutorService,
    actorSystem: ActorSystem,
) extends CantonNodeBootstrapX[
      SequencerNodeX,
      SequencerNodeConfigCommon,
      SequencerNodeParameters,
      SequencerMetrics,
    ](arguments)
    with SequencerNodeBootstrapCommon[SequencerNodeX, SequencerNodeConfigCommon] {

  override protected def member(uid: UniqueIdentifier): Member = SequencerId(uid)

  override protected def customNodeStages(
      storage: Storage,
      crypto: Crypto,
      nodeId: UniqueIdentifier,
      manager: AuthorizedTopologyManagerX,
      healthReporter: GrpcHealthReporter,
      healthService: HealthService,
  ): BootstrapStageOrLeaf[SequencerNodeX] = {
    new WaitForSequencerToDomainInit(
      storage,
      crypto,
      SequencerId(nodeId),
      manager,
      healthReporter,
      healthService,
    )
  }

  override protected def mediatorsProcessParticipantTopologyRequests: Boolean = true

  private val domainTopologyManager = new SingleUseCell[DomainTopologyManagerX]()

  override protected def sequencedTopologyStores: Seq[TopologyStoreX[DomainStore]] =
    domainTopologyManager.get.map(_.store).toList

  override protected def sequencedTopologyManagers: Seq[DomainTopologyManagerX] =
    domainTopologyManager.get.toList

  private class WaitForSequencerToDomainInit(
      storage: Storage,
      crypto: Crypto,
      sequencerId: SequencerId,
      manager: AuthorizedTopologyManagerX,
      healthReporter: GrpcHealthReporter,
      healthService: HealthService,
  ) extends BootstrapStageWithStorage[
        SequencerNodeX,
        StartupNode,
        (StaticDomainParameters, SequencerFactory, DomainTopologyManagerX, TrafficLimitsStore),
      ](
        "wait-for-sequencer-to-domain-init",
        bootstrapStageCallback,
        storage,
        config.init.autoInit,
      )
      with GrpcSequencerInitializationServiceX.Callback {

    // add initialization service
    adminServerRegistry.addServiceU(
      SequencerInitializationServiceGrpc.bindService(
        new GrpcSequencerInitializationServiceX(this, loggerFactory)(executionContext),
        executionContext,
      )
    )

    // Holds the gRPC server started when the node is started, even when non initialized
    // If non initialized the server will expose the gRPC health service only
    protected val nonInitializedSequencerNodeServer =
      new AtomicReference[Option[DynamicDomainGrpcServer]](None)
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
        loggerFactory,
      )(config.sequencer)
      addCloseable(ret)
      ret
    }

    private def mkTrafficLimitsStore(protocolVersion: ProtocolVersion) = {
      TrafficLimitsStore(
        storage,
        protocolVersion,
        timeouts,
        loggerFactory,
      )
    }

    /** if node is not initialized, create a dynamic domain server such that we can serve a health end-point until
      * we are initialised
      */
    private def initSequencerNodeServer(): Unit = {
      if (nonInitializedSequencerNodeServer.get().isEmpty) {
        // the sequential initialisation queue ensures that this is thread safe
        nonInitializedSequencerNodeServer
          .set(
            Some(
              makeDynamicDomainServer(
                // We use max value for the request size here as this is the default for a non initialized sequencer
                MaxRequestSize(NonNegativeInt.maxValue),
                healthReporter,
              )
            )
          )
          .discard
      }
    }

    private val domainConfigurationStore =
      SequencerDomainConfigurationStore(storage, timeouts, loggerFactory)

    override protected def stageCompleted(implicit
        traceContext: TraceContext
    ): Future[Option[
      (StaticDomainParameters, SequencerFactory, DomainTopologyManagerX, TrafficLimitsStore)
    ]] = {
      domainConfigurationStore.fetchConfiguration.toOption
        .map {
          case Some(existing) =>
            Some(
              (
                existing.domainParameters,
                mkFactory(existing.domainParameters.protocolVersion),
                new DomainTopologyManagerX(
                  clock,
                  crypto,
                  store = createDomainTopologyStore(existing.domainId),
                  outboxQueue = new DomainOutboxQueue(loggerFactory),
                  config.topologyX.enableTopologyTransactionValidation,
                  timeouts,
                  futureSupervisor,
                  loggerFactory,
                ),
                mkTrafficLimitsStore(existing.domainParameters.protocolVersion),
              )
            )
          case None =>
            // create sequencer server such that we can expose a health endpoint until initialized
            initSequencerNodeServer()
            None
        }
        .value
        .map(_.flatten)
    }

    private def createDomainTopologyStore(domainId: DomainId): TopologyStoreX[DomainStore] = {
      val store =
        TopologyStoreX(DomainStore(domainId), storage, timeouts, loggerFactory)
      addCloseable(store)
      store
    }

    override protected def buildNextStage(
        result: (
            StaticDomainParameters,
            SequencerFactory,
            DomainTopologyManagerX,
            TrafficLimitsStore,
        )
    ): StartupNode = {
      val (domainParameters, sequencerFactory, domainTopologyMgr, trafficLimitsStore) = result
      if (domainTopologyManager.putIfAbsent(domainTopologyMgr).nonEmpty) {
        // TODO(#14048) how to handle this error properly?
        throw new IllegalStateException("domainTopologyManager shouldn't have been set before")
      }
      new StartupNode(
        storage,
        crypto,
        sequencerId,
        sequencerFactory,
        domainParameters,
        manager,
        domainTopologyMgr,
        nonInitializedSequencerNodeServer.getAndSet(None),
        healthReporter,
        healthService,
        trafficLimitsStore,
      )
    }

    override protected def autoCompleteStage(): EitherT[FutureUnlessShutdown, String, Option[
      (StaticDomainParameters, SequencerFactory, DomainTopologyManagerX, TrafficLimitsStore)
    ]] =
      EitherT.rightT(None) // this stage doesn't have auto-init

    // Extract the top event state from the topology snapshot
    private def extractTopUpEventsFromTopologySnapshot(
        snapshot: GenericStoredTopologyTransactionsX,
        lastTopologyUpdate: Option[CantonTimestamp],
    ): Map[Member, TopUpEvent] = {
      snapshot.result
        .flatMap(_.selectMapping[TrafficControlStateX])
        .map { tx =>
          tx.transaction.transaction.mapping.member ->
            TopUpEvent(
              tx.transaction.transaction.mapping.totalExtraTrafficLimit,
              tx.validFrom.value,
              tx.transaction.transaction.serial,
            )
        }
        .toMap
    }

    override def initialize(request: InitializeSequencerRequestX)(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, String, InitializeSequencerResponseX] = {
      if (isInitialized) {
        logger.info(
          "Received a request to initialize an already initialized sequencer. Skipping initialization!"
        )
        EitherT.pure(InitializeSequencerResponseX(replicated = config.sequencer.supportsReplicas))
      } else {
        completeWithExternalUS {
          logger.info(
            s"Assigning sequencer to domain ${if (request.sequencerSnapshot.isEmpty) "from beginning"
              else "with existing snapshot"}"
          )
          val sequencerFactory = mkFactory(request.domainParameters.protocolVersion)
          val domainIds = request.topologySnapshot.result
            .map(_.transaction.transaction.mapping)
            .collect { case SequencerDomainStateX(domain, _, _, _) => domain }
            .toSet
          val trafficControlStore = mkTrafficLimitsStore(
            request.domainParameters.protocolVersion
          )
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
                  .flatMap { _ =>
                    val topUps = extractTopUpEventsFromTopologySnapshot(
                      request.topologySnapshot,
                      initialState.latestTopologyClientTimestamp,
                    )
                    EitherT.liftF[Future, String, Unit](trafficControlStore.initialize(topUps))
                  }
                  .mapK(FutureUnlessShutdown.outcomeK)
              }
              .getOrElse {
                logger.debug("Skipping sequencer snapshot")
                EitherT.rightT[FutureUnlessShutdown, String](())
              }
            store = createDomainTopologyStore(domainId)
            outboxQueue = new DomainOutboxQueue(loggerFactory)
            topologyManager = new DomainTopologyManagerX(
              clock,
              crypto,
              store,
              outboxQueue,
              config.topologyX.enableTopologyTransactionValidation,
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
          } yield (request.domainParameters, sequencerFactory, topologyManager, trafficControlStore)
        }.map { _ =>
          InitializeSequencerResponseX(replicated = config.sequencer.supportsReplicas)
        }
      }
    }
  }

  private class StartupNode(
      storage: Storage,
      crypto: Crypto,
      sequencerId: SequencerId,
      sequencerFactory: SequencerFactory,
      staticDomainParameters: StaticDomainParameters,
      authorizedTopologyManager: AuthorizedTopologyManagerX,
      domainTopologyManager: DomainTopologyManagerX,
      preinitializedServer: Option[DynamicDomainGrpcServer],
      healthReporter: GrpcHealthReporter,
      healthService: HealthService,
      trafficLimitsStore: TrafficLimitsStore,
  ) extends BootstrapStage[SequencerNodeX, RunningNode[SequencerNodeX]](
        description = "Startup sequencer node",
        bootstrapStageCallback,
      )
      with HasCloseContext {
    // save one argument and grab the domainId from the store ...
    private val domainId = domainTopologyManager.store.storeId.domainId
    private val domainLoggerFactory = loggerFactory.append("domainId", domainId.toString)

    preinitializedServer.foreach(x => addCloseable(x.publicServer))

    private def createRateLimitManager(
        topologyProcessor: TopologyTransactionProcessorX
    ) = {
      val rateLimiter = new EnterpriseSequencerRateLimitManager(
        trafficLimitsStore,
        loggerFactory,
        futureSupervisor,
        timeouts,
        arguments.metrics,
      )

      topologyProcessor.subscribe(
        new TopologyTransactionTrafficSubscription(
          rateLimiter,
          domainLoggerFactory,
        )
      )
      addCloseable(rateLimiter)
      Some(rateLimiter)
    }

    override protected def attempt()(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, String, Option[RunningNode[SequencerNodeX]]] = {

      val domainOutboxFactory = new DomainOutboxXFactorySingleCreate(
        domainId,
        sequencerId,
        authorizedTopologyManager,
        domainTopologyManager,
        crypto,
        config.topologyX,
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
            TopologyTransactionProcessorX.createProcessorAndClientForDomain(
              domainTopologyStore,
              domainId,
              staticDomainParameters.protocolVersion,
              crypto.pureCrypto,
              parameters,
              config.topologyX.enableTopologyTransactionValidation,
              clock,
              futureSupervisor,
              domainLoggerFactory,
            )
          )
          (topologyProcessor, topologyClient) = processorAndClient
          // Create a rate limiter manager
          rateLimiter = createRateLimitManager(topologyProcessor)
          maxStoreTimestamp <- EitherT.right(domainTopologyStore.maxTimestamp())
          membersToRegister <- {
            ips.add(topologyClient)
            addCloseable(topologyProcessor)
            addCloseable(topologyClient)
            // TODO(#14073) more robust initialization: if we upload the genesis state, we need to poke
            //    the topology client by a small increment, as otherwise it won't see the keys (asOfExclusive)
            //    also, right now, we initialize the mediators here. subsequently, we subscribe to the
            //    topology processor and keep on adding new nodes
            val tsInit = CantonTimestamp.MinValue.immediateSuccessor

            if (topologyClient.approximateTimestamp == tsInit) {
              val tsNext = EffectiveTime(
                maxStoreTimestamp
                  .map(_._2.value)
                  .getOrElse(tsInit)
                  .immediateSuccessor
              )
              topologyClient.updateHead(tsNext, tsNext.toApproximate, false)
              // this sequencer node was started for the first time an initialized with a topology state.
              // therefore we fetch all members who have registered a key (OwnerToKeyMappingX) and pass them
              // to the underlying sequencer driver to register them as known members
              EitherT.right[String](
                domainTopologyStore
                  .findPositiveTransactions(
                    tsNext.value,
                    asOfInclusive = false,
                    isProposal = false,
                    types = Seq(OwnerToKeyMappingX.code),
                    filterUid = None,
                    filterNamespace = None,
                  )
                  .map(
                    _.collectOfMapping[OwnerToKeyMappingX].collectLatestByUniqueKey.toTopologyState
                      .map(_.member)
                      .toSet
                  )
              )
            } else EitherT.rightT[Future, String](Set.empty[Member])
          }

          memberAuthServiceFactory = MemberAuthenticationServiceFactory(
            domainId,
            clock,
            config.publicApi.nonceExpirationTime.asJava,
            config.publicApi.tokenExpirationTime.asJava,
            parameters.processingTimeouts,
            domainLoggerFactory,
            topologyProcessor,
          )
          sequencer <- createSequencerRuntime(
            sequencerFactory,
            domainId,
            sequencerId,
            Seq(sequencerId) ++ membersToRegister,
            topologyClient,
            topologyProcessor,
            Some(TopologyManagerStatus.combined(authorizedTopologyManager, domainTopologyManager)),
            staticDomainParameters,
            storage,
            crypto,
            indexedStringStore,
            Future.unit, // domain is already initialised
            Future.successful(true),
            arguments,
            Some(
              new StoreBasedTopologyStateForInitializationService(
                domainTopologyStore,
                domainLoggerFactory,
              )
            ),
            Some(domainOutboxFactory),
            memberAuthServiceFactory,
            rateLimiter,
            domainLoggerFactory,
          )
          // TODO(#14073) subscribe to processor BEFORE sequencer client is created
          _ = addCloseable(sequencer)
          server <- createSequencerServer(
            sequencer,
            staticDomainParameters,
            topologyClient,
            preinitializedServer,
            healthReporter,
            domainLoggerFactory,
          )
        } yield {
          // if close handle hasn't been registered yet, register it now
          if (preinitializedServer.isEmpty) {
            addCloseable(server.publicServer)
          }
          val node = new SequencerNodeX(
            config,
            arguments.metrics,
            arguments.parameterConfig,
            clock,
            sequencer,
            domainLoggerFactory,
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
}

// TODO(#15161): Rename SequencerNodeX to SequencerNode, also remove X from SequencerNodeBootstrapX above
class SequencerNodeX(
    config: SequencerNodeConfigCommon,
    metrics: SequencerMetrics,
    parameters: SequencerNodeParameters,
    clock: Clock,
    sequencer: SequencerRuntime,
    loggerFactory: NamedLoggerFactory,
    sequencerNodeServer: DynamicDomainGrpcServer,
    components: => Seq[ComponentStatus],
)(implicit executionContext: ExecutionContextExecutorService)
    extends SequencerNodeCommon(
      config,
      metrics,
      parameters,
      clock,
      sequencer,
      loggerFactory,
      sequencerNodeServer,
      components,
    ) {

  override def close(): Unit = {
    super.close()
  }
}
