// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.environment

import cats.data.EitherT
import cats.syntax.functorFilter.*
import cats.syntax.option.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton
import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.concurrent.{
  ExecutionContextIdlenessExecutorService,
  FutureSupervisor,
}
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.{InitConfigBase, LocalNodeConfig, ProcessingTimeout}
import com.digitalasset.canton.connection.GrpcApiInfoService
import com.digitalasset.canton.connection.v30.ApiInfoServiceGrpc
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.admin.v0.VaultServiceGrpc
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.health.admin.data.NodeStatus
import com.digitalasset.canton.health.admin.grpc.GrpcStatusService
import com.digitalasset.canton.health.admin.v0.StatusServiceGrpc
import com.digitalasset.canton.health.{
  DependenciesHealthService,
  GrpcHealthReporter,
  GrpcHealthServer,
  HealthService,
  LivenessHealthService,
  ServiceHealthStatusManager,
}
import com.digitalasset.canton.lifecycle.{
  FlagCloseable,
  FutureUnlessShutdown,
  HasCloseContext,
  Lifecycle,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.{CantonGrpcUtil, CantonServerBuilder}
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.store.IndexedStringStore
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.admin.grpc.{
  GrpcInitializationService,
  GrpcTopologyAggregationService,
  GrpcTopologyManagerReadService,
  GrpcTopologyManagerWriteService,
}
import com.digitalasset.canton.topology.admin.v0.{
  InitializationServiceGrpc,
  TopologyManagerWriteServiceGrpc,
}
import com.digitalasset.canton.topology.client.IdentityProvidingServiceClient
import com.digitalasset.canton.topology.store.{InitializationStore, TopologyStore, TopologyStoreId}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.TraceContext.withNewTraceContext
import com.digitalasset.canton.tracing.{NoTracing, TraceContext}
import com.digitalasset.canton.util.retry.RetryUtil.NoExnRetryable
import com.digitalasset.canton.util.{SimpleExecutionQueue, retry}
import com.digitalasset.canton.version.{ProtocolVersion, ReleaseProtocolVersion}
import com.digitalasset.canton.watchdog.WatchdogService
import io.grpc.ServerServiceDefinition
import io.grpc.protobuf.services.ProtoReflectionService
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.actor.ActorSystem
import org.slf4j.event.Level

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import java.util.concurrent.{Executors, ScheduledExecutorService}
import scala.concurrent.duration.*
import scala.concurrent.{Future, blocking}

/** Bootstrapping class used to drive the initialization of a canton node (domain and participant)
  *
  * (wait for unique id) -> receive initId ->  notify actual implementation via idInitialized
  */
abstract class CantonNodeBootstrapBase[
    T <: CantonNode,
    NodeConfig <: LocalNodeConfig,
    ParameterConfig <: CantonNodeParameters,
    Metrics <: BaseMetrics,
](
    protected val arguments: CantonNodeBootstrapCommonArguments[
      NodeConfig,
      ParameterConfig,
      Metrics,
    ]
)(
    implicit val executionContext: ExecutionContextIdlenessExecutorService,
    implicit val scheduler: ScheduledExecutorService,
    implicit val actorSystem: ActorSystem,
) extends CantonNodeBootstrap[T]
    with HasCloseContext
    with NoTracing {

  override def name: InstanceName = arguments.name
  override def clock: Clock = arguments.clock
  def config: NodeConfig = arguments.config
  def parameterConfig: ParameterConfig = arguments.parameterConfig
  // TODO(#14048) unify parameters and parameterConfig
  def parameters: ParameterConfig = parameterConfig
  override def timeouts: ProcessingTimeout = arguments.parameterConfig.processingTimeouts
  override def loggerFactory: NamedLoggerFactory = arguments.loggerFactory
  protected def futureSupervisor: FutureSupervisor = arguments.futureSupervisor

  protected val cryptoConfig = config.crypto
  protected val adminApiConfig = config.adminApi
  protected val initConfig = config.init
  protected val tracerProvider = arguments.tracerProvider
  protected implicit val tracer: Tracer = tracerProvider.tracer
  protected val initQueue: SimpleExecutionQueue = new SimpleExecutionQueue(
    s"init-queue-${arguments.name}",
    arguments.futureSupervisor,
    timeouts,
    loggerFactory,
  )

  private val isRunningVar = new AtomicBoolean(true)
  private val isWaitingForIdVar = new AtomicBoolean(false)
  protected def isRunning: Boolean = isRunningVar.get()
  def isWaitingForId: Boolean = isWaitingForIdVar.get()

  /** Can this node be initialized by a replica */
  protected def supportsReplicaInitialization: Boolean = false
  private val initializationWatcherRef = new AtomicReference[Option[InitializationWatcher]](None)

  // reference to the node once it has been started
  private val ref: AtomicReference[Option[T]] = new AtomicReference(None)
  private val starting = new AtomicBoolean(false)
  // accessors to both the running node and for testing whether it has been set
  override def getNode: Option[T] = ref.get()
  def isInitialized: Boolean = ref.get().isDefined
  override def isActive: Boolean = storage.isActive
  private val nodeId = new AtomicReference[Option[NodeId]](None)

  // TODO(i3168): Move to a error-safe node initialization approach
  protected val storage: Storage =
    arguments.storageFactory
      .tryCreate(
        connectionPoolForParticipant,
        arguments.parameterConfig.logQueryCost,
        clock,
        Some(scheduler),
        arguments.metrics.storageMetrics,
        parameterConfig.processingTimeouts,
        loggerFactory,
      )

  protected val initializationStore = InitializationStore(storage, timeouts, loggerFactory)
  protected val indexedStringStore =
    IndexedStringStore.create(
      storage,
      parameterConfig.cachingConfigs.indexedStrings,
      timeouts,
      loggerFactory,
    )

  override val crypto: Crypto =
    timeouts.unbounded.await(
      description = "initialize CryptoFactory",
      logFailing = Some(Level.ERROR),
    )(
      arguments.cryptoFactory
        .create(
          cryptoConfig,
          storage,
          arguments.cryptoPrivateStoreFactory,
          ReleaseProtocolVersion.latest,
          timeouts,
          loggerFactory,
          tracerProvider,
        )
        .valueOr(err => throw new RuntimeException(s"Failed to initialize crypto: $err"))
    )

  // This absolutely must be a "def", because it is used during class initialization.
  protected def connectionPoolForParticipant: Boolean = false

  protected val ips = new IdentityProvidingServiceClient()

  private def status: Future[NodeStatus[NodeStatus.Status]] = {
    getNode
      .map(_.status.map(NodeStatus.Success(_)))
      .getOrElse(Future.successful(NodeStatus.NotInitialized(isActive)))
  }

  protected def registerHealthGauge(): Unit = {
    arguments.metrics.healthMetrics
      .registerHealthGauge(
        name.toProtoPrimitive,
        () => getNode.map(_.status.map(_.active)).getOrElse(Future(false)),
      )
      .discard // we still want to report the health even if the node is closed
  }

  // The admin-API services
  logger.info(s"Starting admin-api services on ${adminApiConfig}")
  protected val (adminServer, adminServerRegistry) = {
    val builder = CantonServerBuilder
      .forConfig(
        adminApiConfig,
        arguments.metrics.prefix,
        arguments.metrics.metricsFactory,
        executionContext,
        loggerFactory,
        parameterConfig.loggingConfig.api,
        parameterConfig.tracing,
        arguments.metrics.grpcMetrics,
      )

    val registry = builder.mutableHandlerRegistry()

    val server = builder
      .addService(
        StatusServiceGrpc.bindService(
          new GrpcStatusService(
            status,
            arguments.writeHealthDumpToFile,
            parameterConfig.processingTimeouts,
            loggerFactory,
          ),
          executionContext,
        )
      )
      .addService(ProtoReflectionService.newInstance(), false)
      .addService(
        ApiInfoServiceGrpc.bindService(
          new GrpcApiInfoService(CantonGrpcUtil.ApiName.AdminApi),
          executionContext,
        )
      )
      .build
      .start()
    (Lifecycle.toCloseableServer(server, logger, "AdminServer"), registry)
  }

  protected def mkNodeHealthService(
      storage: Storage
  ): (DependenciesHealthService, LivenessHealthService)

  protected def mkHealthComponents(
      nodeHealthService: HealthService,
      nodeLivenessService: LivenessHealthService,
  ): (GrpcHealthReporter, Option[GrpcHealthServer]) = {
    val healthReporter: GrpcHealthReporter = new GrpcHealthReporter(loggerFactory)
    val grpcNodeHealthManager =
      ServiceHealthStatusManager(
        "Health API",
        new io.grpc.protobuf.services.HealthStatusManager(),
        Set(nodeHealthService, nodeLivenessService),
      )
    val grpcHealthServer = config.monitoring.grpcHealthServer.map { healthConfig =>
      healthReporter.registerHealthManager(grpcNodeHealthManager)

      val executor = Executors.newFixedThreadPool(healthConfig.parallelism)

      new GrpcHealthServer(
        healthConfig,
        arguments.metrics.metricsFactory,
        executor,
        loggerFactory,
        parameterConfig.loggingConfig.api,
        parameterConfig.tracing,
        arguments.metrics.grpcMetrics,
        timeouts,
        grpcNodeHealthManager.manager,
      )
    }
    (healthReporter, grpcHealthServer)
  }

  locally {
    registerHealthGauge()
  }

  adminServerRegistry.addServiceU(
    VaultServiceGrpc.bindService(
      arguments.grpcVaultServiceFactory
        .create(
          crypto,
          parameterConfig.enablePreviewFeatures,
          timeouts,
          loggerFactory,
        ),
      executionContext,
    )
  )

  protected val authorizedTopologyStore =
    TopologyStore(
      TopologyStoreId.AuthorizedStore,
      storage,
      timeouts,
      loggerFactory,
      futureSupervisor,
    )
  this.adminServerRegistry
    .addService(
      canton.topology.admin.v0.TopologyManagerReadServiceGrpc
        .bindService(
          new GrpcTopologyManagerReadService(
            sequencedTopologyStores,
            ips,
            crypto,
            loggerFactory,
          ),
          executionContext,
        )
    )
    .discard

  this.adminServerRegistry
    .addService(
      InitializationServiceGrpc
        .bindService(
          new GrpcInitializationService(clock, this, crypto.cryptoPublicStore),
          executionContext,
        )
    )
    ._2
    .addService(
      canton.topology.admin.v0.TopologyAggregationServiceGrpc
        .bindService(
          new GrpcTopologyAggregationService(
            // TODO(#14048) remove map filter
            sequencedTopologyStores.mapFilter(TopologyStoreId.select[TopologyStoreId.DomainStore]),
            ips,
            loggerFactory,
          ),
          executionContext,
        )
    )
    ._2
    .addService(
      canton.topology.admin.v0.TopologyManagerReadServiceGrpc
        .bindService(
          new GrpcTopologyManagerReadService(
            sequencedTopologyStores :+ authorizedTopologyStore,
            ips,
            crypto,
            loggerFactory,
          ),
          executionContext,
        )
    )
    .discard

  /** When a node is not initialized and auto-init is false, we skip initialization.
    * This can be overridden to give a chance to a node to still perform some operation after it has been started
    * but not yet initialized.
    */
  def runOnSkippedInitialization: EitherT[Future, String, Unit] = EitherT.pure[Future, String](())

  /** Attempt to start the node with this identity. */
  protected def initialize(uid: NodeId): EitherT[FutureUnlessShutdown, String, Unit]

  /** Generate an identity for the node. */
  protected def autoInitializeIdentity(
      initConfigBase: InitConfigBase
  ): EitherT[FutureUnlessShutdown, String, Unit]

  final protected def storeId(id: NodeId): EitherT[Future, String, Unit] =
    for {
      previous <- EitherT.right(initializationStore.id)
      result <- previous match {
        case Some(existing) =>
          EitherT.leftT[Future, Unit](s"Node is already initialized with id [$existing]")
        case None =>
          logger.info(s"Initializing node with id $id")
          EitherT.right[String](for {
            _ <- initializationStore.setId(id)
            _ = nodeId.set(Some(id))
          } yield ())
      }
    } yield result

  /** Attempt to start the node.
    * If a previously initialized identifier is available the node will be immediately initialized.
    * If there is no existing identity and autoinit is enabled an identity will be automatically generated and then the node will initialize.
    * If there is no existing identity and autoinit is disabled start will immediately exit to wait for an identity to be externally provided through [[initializeWithProvidedId]].
    */
  def start(): EitherT[Future, String, Unit] = {
    // The passive replica waits for the active replica to initialize the unique identifier
    def waitForActiveId(): EitherT[Future, String, Option[NodeId]] = EitherT {
      val timeout = parameterConfig.processingTimeouts
      val resultAfterRetry = retry
        .Pause(
          logger,
          FlagCloseable(logger, timeout),
          timeout.activeInit.retries(timeout.activeInitRetryDelay.duration),
          timeout.activeInitRetryDelay.asFiniteApproximation,
          functionFullName,
        )
        .apply(
          {
            if (storage.isActive) Future.successful(Right(None))
            else {
              isWaitingForIdVar.set(true)
              initializationStore.id
                .map(
                  _.toRight("Active replica failed to initialize unique identifier")
                    .map(Some(_))
                )
            }
          },
          NoExnRetryable,
        )

      resultAfterRetry.onComplete { _ =>
        isWaitingForIdVar.set(false)
      }

      resultAfterRetry
    }

    initQueue
      .executeEUS(
        for {
          // if we're a passive replica but the node is set to auto-initialize, wait here until the node has established an id
          id <-
            performUnlessClosingEitherU("waitOrFetchActiveId") {
              if (!storage.isActive && initConfig.autoInit) waitForActiveId()
              else
                EitherT.right[String](
                  initializationStore.id
                ) // otherwise just fetch what's that immediately
            }
          _ <- id.fold(
            if (initConfig.autoInit) {
              logger.info(
                "Node is not initialized yet. Performing automated default initialization."
              )
              autoInitializeIdentity(initConfig)
            } else {
              logger.info(
                "Node is not initialized yet. You have opted for manual configuration by yourself."
              )
              performUnlessClosingEitherU("runOnSkippedInitialization")(runOnSkippedInitialization)
            }
          )(startWithStoredNodeId)
        } yield (),
        functionFullName,
      )
      .map { _ =>
        // if we're still not initialized and support a replica doing on our behalf, start a watcher to handle that happening
        if (getId.isEmpty && supportsReplicaInitialization) waitForReplicaInitialization()
      }
      .onShutdown(Left("Aborted due to shutdown"))
  }

  /** Poll the datastore to see if the id has been initialized in case a replica initializes the node */
  private def waitForReplicaInitialization(): Unit = blocking {
    synchronized {
      withNewTraceContext { implicit traceContext =>
        if (isRunning && initializationWatcherRef.get().isEmpty) {
          val initializationWatcher = new InitializationWatcher(initializationStore, loggerFactory)
          initializationWatcher.watch(nodeId =>
            initQueue
              .executeEUS(
                startWithStoredNodeId(nodeId),
                "waitForReplicaInitializationStartWithStoredNodeId",
              )
          )
          initializationWatcherRef.set(initializationWatcher.some)
        }
      }
    }
  }

  protected def startWithStoredNodeId(id: NodeId): EitherT[FutureUnlessShutdown, String, Unit] =
    if (nodeId.compareAndSet(None, Some(id))) {
      logger.info(s"Resuming as existing instance with uid=${id}")
      initialize(id).leftMap { err =>
        logger.info(s"Failed to initialize node, trying to clean up: $err")
        close()
        err
      }
    } else {
      EitherT.leftT[FutureUnlessShutdown, Unit]("Node identity has already been initialized")
    }

  def getId: Option[NodeId] = nodeId.get()

  /** kick off initialisation during startup */
  protected def startInstanceUnlessClosing(
      instanceET: => EitherT[FutureUnlessShutdown, String, T]
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    if (isInitialized) {
      logger.warn("Will not start instance again as it is already initialised")
      EitherT.pure[FutureUnlessShutdown, String](())
    } else {
      if (starting.compareAndSet(false, true))
        instanceET.map { instance =>
          val previous = ref.getAndSet(Some(instance))
          // potentially over-defensive, but ensures a runner will not be set twice.
          // if called twice it indicates a bug in initialization.
          previous.foreach { shouldNotBeThere =>
            logger.error(s"Runner has already been set: $shouldNotBeThere")
          }
        }
      else {
        logger.warn("Will not start instance again as it is already starting up")
        EitherT.pure[FutureUnlessShutdown, String](())
      }
    }
  }

  /** All existing domain stores */
  protected def sequencedTopologyStores: Seq[TopologyStore[TopologyStoreId]]

  /** Initialize the node with an externally provided identity. */
  def initializeWithProvidedId(nodeId: NodeId): EitherT[Future, String, Unit] = initQueue
    .executeEUS(
      {
        for {
          _ <- performUnlessClosingEitherU("storeNodeId")(storeId(nodeId))
          _ <- initialize(nodeId)
        } yield ()
      },
      functionFullName,
    )
    .onShutdown(Left("Aborted due to shutdown"))

  protected def startTopologyManagementWriteService[E <: CantonError](
      topologyManager: TopologyManager[E]
  ): Unit = {
    adminServerRegistry
      .addServiceU(
        topologyManagerWriteService(topologyManager)
      )
  }

  protected def topologyManagerWriteService[E <: CantonError](
      topologyManager: TopologyManager[E]
  ): ServerServiceDefinition = {
    TopologyManagerWriteServiceGrpc.bindService(
      new GrpcTopologyManagerWriteService(
        topologyManager,
        crypto.cryptoPublicStore,
        parameterConfig.initialProtocolVersion,
        loggerFactory,
      ),
      executionContext,
    )

  }

  // utility functions used by automatic initialization of domain and participant
  protected def authorizeStateUpdate[E <: CantonError](
      manager: TopologyManager[E],
      key: SigningPublicKey,
      mapping: TopologyStateUpdateMapping,
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    authorizeIfNew(
      manager,
      TopologyStateUpdate.createAdd(mapping, protocolVersion),
      key,
      protocolVersion,
    )

  protected def authorizeIfNew[E <: CantonError, Op <: TopologyChangeOp](
      manager: TopologyManager[E],
      transaction: TopologyTransaction[Op],
      signingKey: SigningPublicKey,
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = for {
    exists <- EitherT
      .right(
        manager.signedMappingAlreadyExists(transaction.element.mapping, signingKey.fingerprint)
      )
      .mapK(FutureUnlessShutdown.outcomeK)
    res <-
      if (exists) {
        logger.debug(s"Skipping existing ${transaction.element.mapping}")
        EitherT.rightT[FutureUnlessShutdown, String](())
      } else
        manager
          .authorize(transaction, Some(signingKey.fingerprint), protocolVersion, false)
          .leftMap(_.toString)
          .map(_ => ())
  } yield res

  /** Health service component of the node
    */
  protected lazy val (
    nodeHealthService: DependenciesHealthService,
    nodeLivenessService: LivenessHealthService,
  ) = mkNodeHealthService(storage)

  protected val (healthReporter, grpcHealthServer) =
    mkHealthComponents(nodeHealthService, nodeLivenessService)

  private val watchdogServiceO = arguments.parameterConfig.watchdog
    .filter(_.enabled)
    .map(watchdogConfig =>
      WatchdogService.SysExitOnNotServing(
        watchdogConfig.checkInterval,
        watchdogConfig.killDelay,
        nodeLivenessService,
        loggerFactory,
        timeouts,
      )
    )

  override protected def onClosed(): Unit = {
    if (isRunningVar.getAndSet(false)) {
      val stores = List[AutoCloseable](
        initializationStore,
        indexedStringStore,
        authorizedTopologyStore,
      )
      val instances = List(
        initQueue,
        Lifecycle.toCloseableOption(initializationWatcherRef.get()),
        adminServerRegistry,
        adminServer,
      ) ++ grpcHealthServer.toList ++ getNode.toList ++ stores ++ List(
        crypto,
        storage,
        clock,
        nodeHealthService,
        nodeLivenessService,
      ) ++ watchdogServiceO.toList
      Lifecycle.close(instances: _*)(logger)
      logger.debug(s"Successfully completed shutdown of $name")
    } else {
      logger.warn(
        s"Unnecessary second close of node $name invoked. Ignoring it.",
        new Exception("location"),
      )
    }
    super.onClosed()
  }

  protected class InitializationWatcher(
      initializationStore: InitializationStore,
      protected val loggerFactory: NamedLoggerFactory,
  ) extends FlagCloseable
      with NamedLogging {
    override protected def timeouts: ProcessingTimeout =
      arguments.parameterConfig.processingTimeouts
    def watch(
        startWithStoredNodeId: NodeId => EitherT[FutureUnlessShutdown, String, Unit]
    )(implicit traceContext: TraceContext): Unit = {
      logger.debug(s"Waiting for a node id to be stored to start this node instance")
      // we try forever - 1 to avoid logging every attempt at warning
      retry
        .Backoff(
          logger,
          this,
          retry.Forever - 1,
          initialDelay = 500.millis,
          maxDelay = 5.seconds,
          "waitForIdInitialization",
        )
        .apply(initializationStore.id, NoExnRetryable)
        .foreach(_.foreach { id =>
          if (getId.isDefined) {
            logger.debug("A stored id has been found but the id has already been set so ignoring")
          } else {
            logger.info("Starting node as we have found a stored id")
            startWithStoredNodeId(id).onShutdown(Right(())).value.foreach {
              case Left(error) =>
                // if we are already successfully initialized likely this was just called twice due to a race between
                // the waiting and an initialize call
                if (isInitialized) {
                  logger.debug(
                    s"An error was returned when starting the node due to finding a stored id but we are already initialized: $error"
                  )
                } else if (isClosing) {
                  // If the node is currently shutting down, not being able to initialize is anyway not a problem.
                  // The error is most likely due to part of the start up procedure failing due to the shutdown.
                  logger.debug(
                    s"An error was returned when starting the node due to finding a stored id, but the node is currently shutting down: $error"
                  )
                } else if (getId.isDefined) {
                  logger.info(
                    "The node startup has been initialised by another thread. This can happen if the background initialization watcher races with a manual init."
                  )
                } else {
                  logger.error(s"Failed to start the node when finding a stored id: $error")
                }
              case _ =>
            }
          }
        })
    }
  }

}
