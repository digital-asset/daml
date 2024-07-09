// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.environment

import better.files.File
import cats.data.EitherT
import cats.syntax.functorFilter.*
import com.daml.metrics.HealthMetrics
import com.daml.metrics.api.MetricHandle.LabeledMetricsFactory
import com.daml.metrics.api.MetricName
import com.daml.metrics.grpc.GrpcServerMetrics
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.{
  ExecutionContextIdlenessExecutorService,
  FutureSupervisor,
}
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.{
  CryptoConfig,
  InitConfigBase,
  LocalNodeConfig,
  ProcessingTimeout,
  TestingConfigInternal,
}
import com.digitalasset.canton.connection.GrpcApiInfoService
import com.digitalasset.canton.connection.v30.ApiInfoServiceGrpc
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.admin.grpc.GrpcVaultService.GrpcVaultServiceFactory
import com.digitalasset.canton.crypto.admin.v30.VaultServiceGrpc
import com.digitalasset.canton.crypto.store.CryptoPrivateStore.CryptoPrivateStoreFactory
import com.digitalasset.canton.crypto.store.{CryptoPrivateStoreError, CryptoPublicStoreError}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.environment.CantonNodeBootstrap.HealthDumpFunction
import com.digitalasset.canton.error.FatalError
import com.digitalasset.canton.health.admin.data.{
  NodeStatus,
  WaitingForExternalInput,
  WaitingForId,
  WaitingForNodeTopology,
}
import com.digitalasset.canton.health.admin.grpc.GrpcStatusService
import com.digitalasset.canton.health.admin.v30.StatusServiceGrpc
import com.digitalasset.canton.health.{
  DependenciesHealthService,
  GrpcHealthReporter,
  GrpcHealthServer,
  HttpHealthServer,
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
import com.digitalasset.canton.metrics.DbStorageMetrics
import com.digitalasset.canton.networking.grpc.{CantonGrpcUtil, CantonServerBuilder}
import com.digitalasset.canton.resource.{Storage, StorageFactory}
import com.digitalasset.canton.telemetry.ConfiguredOpenTelemetry
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.admin.grpc.{
  GrpcIdentityInitializationService,
  GrpcTopologyAggregationService,
  GrpcTopologyManagerReadService,
  GrpcTopologyManagerWriteService,
}
import com.digitalasset.canton.topology.admin.v30 as adminV30
import com.digitalasset.canton.topology.client.{
  DomainTopologyClient,
  IdentityProvidingServiceClient,
}
import com.digitalasset.canton.topology.store.TopologyStoreId.DomainStore
import com.digitalasset.canton.topology.store.{InitializationStore, TopologyStore, TopologyStoreId}
import com.digitalasset.canton.topology.transaction.{
  NamespaceDelegation,
  OwnerToKeyMapping,
  SignedTopologyTransaction,
  TopologyChangeOp,
  TopologyMapping,
}
import com.digitalasset.canton.tracing.{NoTracing, TraceContext, TracerProvider}
import com.digitalasset.canton.util.{FutureUtil, SimpleExecutionQueue}
import com.digitalasset.canton.version.{ProtocolVersion, ReleaseProtocolVersion}
import com.digitalasset.canton.watchdog.WatchdogService
import io.grpc.protobuf.services.ProtoReflectionService
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.actor.ActorSystem

import java.util.concurrent.{Executors, ScheduledExecutorService}
import scala.concurrent.{ExecutionContext, Future}

/** When a canton node is created it first has to obtain an identity before most of its services can be started.
  * This process will begin when `start` is called and will try to perform as much as permitted by configuration automatically.
  * If external action is required before this process can complete `start` will return successfully but `isInitialized` will still be false.
  * When the node is successfully initialized the underlying node will be available through `getNode`.
  */
trait CantonNodeBootstrap[+T <: CantonNode] extends FlagCloseable with NamedLogging {

  def name: InstanceName
  def clock: Clock
  def isInitialized: Boolean

  def start(): EitherT[Future, String, Unit]

  def getNode: Option[T]

  /** Access to the private and public store to support local key inspection commands */
  def crypto: Option[Crypto]
  def isActive: Boolean
}

object CantonNodeBootstrap {
  type HealthDumpFunction = File => Future[Unit]
}

trait BaseMetrics {
  def prefix: MetricName

  def openTelemetryMetricsFactory: LabeledMetricsFactory

  def grpcMetrics: GrpcServerMetrics
  def healthMetrics: HealthMetrics
  def storageMetrics: DbStorageMetrics
}

final case class CantonNodeBootstrapCommonArguments[
    +NodeConfig <: LocalNodeConfig,
    ParameterConfig <: CantonNodeParameters,
    M <: BaseMetrics,
](
    name: InstanceName,
    config: NodeConfig,
    parameterConfig: ParameterConfig,
    testingConfig: TestingConfigInternal,
    clock: Clock,
    metrics: M,
    storageFactory: StorageFactory,
    cryptoFactory: CryptoFactory,
    cryptoPrivateStoreFactory: CryptoPrivateStoreFactory,
    grpcVaultServiceFactory: GrpcVaultServiceFactory,
    futureSupervisor: FutureSupervisor,
    loggerFactory: NamedLoggerFactory,
    writeHealthDumpToFile: HealthDumpFunction,
    configuredOpenTelemetry: ConfiguredOpenTelemetry,
    tracerProvider: TracerProvider,
)

/** CantonNodeBootstrapImpl insists that nodes have their own topology manager
  * and that they have the ability to auto-initialize their identity on their own.
  */
abstract class CantonNodeBootstrapImpl[
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

  protected val cryptoConfig: CryptoConfig = config.crypto
  protected val initConfig: InitConfigBase = config.init
  protected val tracerProvider: TracerProvider = arguments.tracerProvider
  protected implicit val tracer: Tracer = tracerProvider.tracer
  protected val initQueue: SimpleExecutionQueue = new SimpleExecutionQueue(
    s"init-queue-${arguments.name}",
    arguments.futureSupervisor,
    timeouts,
    loggerFactory,
  )

  // This absolutely must be a "def", because it is used during class initialization.
  protected def connectionPoolForParticipant: Boolean = false

  protected val ips = new IdentityProvidingServiceClient()

  private val adminApiConfig = config.adminApi

  private def status: Future[NodeStatus[NodeStatus.Status]] = {
    getNode
      .map(_.status.map(NodeStatus.Success(_)))
      .getOrElse(
        Future.successful(NodeStatus.NotInitialized(isActive, waitingFor))
      )
  }

  private def waitingFor: Option[WaitingForExternalInput] = {
    def nextStage(stage: BootstrapStage[?, ?]): Option[BootstrapStage[?, ?]] = {
      stage.next match {
        case Some(s: BootstrapStage[_, _]) => nextStage(s)
        case Some(_: RunningNode[_]) => None
        // BootstrapStageOrLeaf is not a sealed class, therefore we need to catch any other
        // possible subclass
        case Some(_) => None
        case None => Some(stage)
      }
    }
    nextStage(startupStage).flatMap(_.waitingFor)
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
  logger.info(s"Starting admin-api services on $adminApiConfig")
  protected val (adminServer, adminServerRegistry) = {
    val builder = CantonServerBuilder
      .forConfig(
        adminApiConfig,
        arguments.metrics.prefix,
        arguments.metrics.openTelemetryMetricsFactory,
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
      .addService(ProtoReflectionService.newInstance(), withLogging = false)
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
      nodeHealthService: DependenciesHealthService,
      livenessService: LivenessHealthService,
  ): (GrpcHealthReporter, Option[GrpcHealthServer], Option[HttpHealthServer]) = {
    val healthReporter: GrpcHealthReporter = new GrpcHealthReporter(loggerFactory)
    val grpcNodeHealthManager =
      ServiceHealthStatusManager(
        "Health API",
        new io.grpc.protobuf.services.HealthStatusManager(),
        Set(nodeHealthService, livenessService),
      )
    val grpcHealthServer = config.monitoring.grpcHealthServer.map { healthConfig =>
      healthReporter.registerHealthManager(grpcNodeHealthManager)

      val executor = Executors.newFixedThreadPool(healthConfig.parallelism)

      new GrpcHealthServer(
        healthConfig,
        arguments.metrics.openTelemetryMetricsFactory,
        executor,
        loggerFactory,
        parameterConfig.loggingConfig.api,
        parameterConfig.tracing,
        arguments.metrics.grpcMetrics,
        timeouts,
        grpcNodeHealthManager.manager,
      )
    }
    val httpHealthServer = config.monitoring.httpHealthServer.map { healthConfig =>
      new HttpHealthServer(
        nodeHealthService,
        healthConfig.address,
        healthConfig.port,
        timeouts,
        loggerFactory,
      )
    }
    (healthReporter, grpcHealthServer, httpHealthServer)
  }

  protected def customNodeStages(
      storage: Storage,
      crypto: Crypto,
      nodeId: UniqueIdentifier,
      manager: AuthorizedTopologyManager,
      healthReporter: GrpcHealthReporter,
      healthService: DependenciesHealthService,
  ): BootstrapStageOrLeaf[T]

  /** member depends on node type */
  protected def member(uid: UniqueIdentifier): Member

  override def isInitialized: Boolean = startupStage.getNode.isDefined
  override def isActive: Boolean = startupStage.next.forall(_.storage.isActive)

  override def start(): EitherT[Future, String, Unit] =
    startupStage.start().onShutdown(Left("Aborted due to shutdown"))

  override def getNode: Option[T] = startupStage.getNode
  override def crypto: Option[Crypto] = startupStage.next.flatMap(_.next).map(_.crypto)

  /** callback for topology read service
    *
    * this callback must be implemented by all node types, providing access to the domain
    * topology stores which are only available in a later startup stage (domain nodes) or
    * in the node runtime itself (participant sync domain)
    */
  // TODO(#14048) implement me!
  protected def sequencedTopologyStores: Seq[TopologyStore[DomainStore]] = Seq()

  protected def sequencedTopologyManagers: Seq[DomainTopologyManager] = Seq()

  protected val bootstrapStageCallback = new BootstrapStage.Callback {
    override def loggerFactory: NamedLoggerFactory = CantonNodeBootstrapImpl.this.loggerFactory
    override def timeouts: ProcessingTimeout = CantonNodeBootstrapImpl.this.timeouts
    override def abortThisNodeOnStartupFailure(): Unit = {
      // TODO(#14048) bubble this up into env ensuring that the node is properly deregistered from env if we fail during
      //   async startup. (node should be removed from running nodes)
      //   we can't call node.close() here as this thing is executed within a performUnlessClosing, so we'd deadlock
      FatalError.exitOnFatalError(s"startup of node $name failed", logger)
    }
    override val queue: SimpleExecutionQueue = initQueue
    override def ec: ExecutionContext = CantonNodeBootstrapImpl.this.executionContext
  }

  protected def lookupTopologyClient(storeId: TopologyStoreId): Option[DomainTopologyClient]

  private val startupStage =
    new BootstrapStage[T, SetupCrypto](
      description = "Initialise storage",
      bootstrapStageCallback,
    ) {
      override protected def attempt()(implicit
          traceContext: TraceContext
      ): EitherT[FutureUnlessShutdown, String, Option[SetupCrypto]] = {
        EitherT(
          FutureUnlessShutdown.lift(
            arguments.storageFactory
              .create(
                connectionPoolForParticipant,
                arguments.parameterConfig.logQueryCost,
                arguments.clock,
                Some(scheduler),
                arguments.metrics.storageMetrics,
                arguments.parameterConfig.processingTimeouts,
                bootstrapStageCallback.loggerFactory,
              )
              .value
          )
        ).map { storage =>
          registerHealthGauge()
          // init health services once
          val (healthService, livenessService) = mkNodeHealthService(storage)
          addCloseable(healthService)
          addCloseable(livenessService)
          val (healthReporter, grpcHealthServer, httpHealthServer) = {
            mkHealthComponents(healthService, livenessService)
          }
          arguments.parameterConfig.watchdog
            .filter(_.enabled)
            .foreach(watchdogConfig => {
              val watchdog = WatchdogService.SysExitOnNotServing(
                watchdogConfig.checkInterval,
                watchdogConfig.killDelay,
                livenessService,
                bootstrap.loggerFactory,
                bootstrap.timeouts,
              )
              addCloseable(watchdog)
            })
          grpcHealthServer.foreach(addCloseable)
          httpHealthServer.foreach(addCloseable)
          addCloseable(storage)
          Some(new SetupCrypto(storage, healthReporter, healthService))
        }
      }
    }

  private class SetupCrypto(
      val storage: Storage,
      val healthReporter: GrpcHealthReporter,
      healthService: DependenciesHealthService,
  ) extends BootstrapStage[T, SetupNodeId](
        description = "Init crypto module",
        bootstrapStageCallback,
      )
      with HasCloseContext {

    override protected def attempt()(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, String, Option[SetupNodeId]] = {
      // crypto factory doesn't write to the db during startup, hence,
      // we won't have "isPassive" issues here
      performUnlessClosingEitherUSF("create-crypto")(
        arguments.cryptoFactory
          .create(
            cryptoConfig,
            storage,
            arguments.cryptoPrivateStoreFactory,
            ReleaseProtocolVersion.latest,
            bootstrapStageCallback.timeouts,
            bootstrapStageCallback.loggerFactory,
            tracerProvider,
          )
          .map { crypto =>
            addCloseable(crypto)
            adminServerRegistry.addServiceU(
              VaultServiceGrpc.bindService(
                arguments.grpcVaultServiceFactory
                  .create(
                    crypto,
                    parameterConfig.enablePreviewFeatures,
                    bootstrapStageCallback.timeouts,
                    bootstrapStageCallback.loggerFactory,
                  ),
                executionContext,
              )
            )
            Some(new SetupNodeId(storage, crypto, healthReporter, healthService))
          }
      )
    }
  }

  private class SetupNodeId(
      storage: Storage,
      val crypto: Crypto,
      healthReporter: GrpcHealthReporter,
      healthService: DependenciesHealthService,
  ) extends BootstrapStageWithStorage[T, GenerateOrAwaitNodeTopologyTx, UniqueIdentifier](
        description = "Init node id",
        bootstrapStageCallback,
        storage,
        config.init.autoInit,
      )
      with HasCloseContext
      with GrpcIdentityInitializationService.Callback {

    private val initializationStore = InitializationStore(
      storage,
      bootstrapStageCallback.timeouts,
      bootstrapStageCallback.loggerFactory,
    )
    addCloseable(initializationStore)
    private val authorizedStore =
      TopologyStore(
        TopologyStoreId.AuthorizedStore,
        storage,
        bootstrapStageCallback.timeouts,
        bootstrapStageCallback.loggerFactory,
      )
    addCloseable(authorizedStore)

    adminServerRegistry
      .addServiceU(
        adminV30.IdentityInitializationServiceGrpc
          .bindService(
            new GrpcIdentityInitializationService(
              clock,
              this,
              crypto.cryptoPublicStore,
              bootstrapStageCallback.loggerFactory,
            ),
            executionContext,
          )
      )

    override def waitingFor: Option[WaitingForExternalInput] = Some(WaitingForId)

    override protected def stageCompleted(implicit
        traceContext: TraceContext
    ): Future[Option[UniqueIdentifier]] = initializationStore.uid

    override protected def buildNextStage(
        uid: UniqueIdentifier
    ): EitherT[FutureUnlessShutdown, String, GenerateOrAwaitNodeTopologyTx] =
      EitherT.rightT(
        new GenerateOrAwaitNodeTopologyTx(
          uid,
          authorizedStore,
          storage,
          crypto,
          healthReporter,
          healthService,
        )
      )

    override protected def autoCompleteStage()
        : EitherT[FutureUnlessShutdown, String, Option[UniqueIdentifier]] = {
      for {
        // create namespace key
        namespaceKey <- CantonNodeBootstrapImpl.getOrCreateSigningKey(crypto)(s"$name-namespace")
        // create id
        identifierName = arguments.config.init.identity
          .flatMap(_.nodeIdentifier.identifierName)
          .getOrElse(name.unwrap)
        uid <- EitherT
          .fromEither[FutureUnlessShutdown](
            UniqueIdentifier
              .create(identifierName, namespaceKey.fingerprint)
          )
          .leftMap(err => s"Failed to convert name to identifier: $err")
        _ <- EitherT
          .right[String](initializationStore.setUid(uid))
          .mapK(FutureUnlessShutdown.outcomeK)
      } yield Option(uid)
    }

    override def initializeWithProvidedId(uid: UniqueIdentifier)(implicit
        traceContext: TraceContext
    ): EitherT[Future, String, Unit] =
      completeWithExternal(
        EitherT.right(initializationStore.setUid(uid).map(_ => uid))
      ).onShutdown(Left("Node has been shutdown"))

    override def getId: Option[UniqueIdentifier] = next.map(_.nodeId)
    override def isInitialized: Boolean = getId.isDefined
  }

  private class GenerateOrAwaitNodeTopologyTx(
      val nodeId: UniqueIdentifier,
      authorizedStore: TopologyStore[TopologyStoreId.AuthorizedStore],
      storage: Storage,
      crypto: Crypto,
      healthReporter: GrpcHealthReporter,
      healthService: DependenciesHealthService,
  ) extends BootstrapStageWithStorage[T, BootstrapStageOrLeaf[T], Unit](
        description = "generate-or-await-node-topology-tx",
        bootstrapStageCallback,
        storage,
        config.init.autoInit,
      ) {

    private val topologyManager: AuthorizedTopologyManager =
      new AuthorizedTopologyManager(
        nodeId,
        clock,
        crypto,
        authorizedStore,
        bootstrapStageCallback.timeouts,
        futureSupervisor,
        bootstrapStageCallback.loggerFactory,
      )
    addCloseable(topologyManager)
    adminServerRegistry
      .addServiceU(
        adminV30.TopologyManagerReadServiceGrpc
          .bindService(
            new GrpcTopologyManagerReadService(
              member(nodeId),
              sequencedTopologyStores :+ authorizedStore,
              crypto,
              lookupTopologyClient,
              processingTimeout = parameterConfig.processingTimeouts,
              bootstrapStageCallback.loggerFactory,
            ),
            executionContext,
          )
      )
    adminServerRegistry
      .addServiceU(
        adminV30.TopologyManagerWriteServiceGrpc
          .bindService(
            new GrpcTopologyManagerWriteService(
              sequencedTopologyManagers :+ topologyManager,
              crypto,
              bootstrapStageCallback.loggerFactory,
            ),
            executionContext,
          )
      )
    adminServerRegistry
      .addServiceU(
        adminV30.TopologyAggregationServiceGrpc.bindService(
          new GrpcTopologyAggregationService(
            sequencedTopologyStores.mapFilter(TopologyStoreId.select[TopologyStoreId.DomainStore]),
            ips,
            bootstrapStageCallback.loggerFactory,
          ),
          executionContext,
        )
      )

    private val topologyManagerObserver = new TopologyManagerObserver {
      override def addedNewTransactions(
          timestamp: CantonTimestamp,
          transactions: Seq[SignedTopologyTransaction[TopologyChangeOp, TopologyMapping]],
      )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
        logger.debug(
          s"Checking whether new topology transactions at $timestamp suffice for initializing the stage $description"
        )
        // Run the resumption check asynchronously so that
        // - Topology transactions added during initialization of this stage do not deadlock
        //   because all stages run on a sequential queue.
        // - Topology transactions added during the resumption do not deadlock
        //   because the topology processor runs all notifications and topology additions on a sequential queue.
        FutureUtil.doNotAwaitUnlessShutdown(
          resumeIfCompleteStage().value,
          s"Checking whether new topology transactions completed stage $description failed ",
        )
        FutureUnlessShutdown.unit
      }
    }

    override def start()(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, String, Unit] = {
      // Register the observer first so that it does not race with the removal when the stage has finished.
      topologyManager.addObserver(topologyManagerObserver)
      super.start()
    }

    override def waitingFor: Option[WaitingForExternalInput] = Some(WaitingForNodeTopology)

    override protected def stageCompleted(implicit
        traceContext: TraceContext
    ): Future[Option[Unit]] = {
      val myMember = member(nodeId)
      authorizedStore
        .findPositiveTransactions(
          CantonTimestamp.MaxValue,
          asOfInclusive = false,
          isProposal = false,
          types = Seq(OwnerToKeyMapping.code),
          filterUid = Some(Seq(nodeId)),
          filterNamespace = None,
        )
        .map { res =>
          val done = res.result
            .filterNot(_.transaction.isProposal)
            .map(_.mapping)
            .exists {
              case OwnerToKeyMapping(`myMember`, None, keys) =>
                // stage is clear if we have a general signing key and possibly also an encryption key
                // this tx can not exist without appropriate certificates, so don't need to check for them
                keys.exists(_.isSigning) && (myMember.code != ParticipantId.Code || keys
                  .exists(x => !x.isSigning))
              case _ => false
            }
          Option.when(done)(())
        }
    }

    override protected def buildNextStage(
        result: Unit
    ): EitherT[FutureUnlessShutdown, String, BootstrapStageOrLeaf[T]] = {
      topologyManager.removeObserver(topologyManagerObserver)
      EitherT.rightT(
        customNodeStages(
          storage,
          crypto,
          nodeId,
          topologyManager,
          healthReporter,
          healthService,
        )
      )
    }

    override protected def autoCompleteStage()
        : EitherT[FutureUnlessShutdown, String, Option[Unit]] = {
      for {
        namespaceKeyO <- crypto.cryptoPublicStore
          .signingKey(nodeId.fingerprint)
          .leftMap(_.toString)
        namespaceKey <- EitherT.fromEither[FutureUnlessShutdown](
          namespaceKeyO.toRight(
            s"Performing auto-init but can't find key ${nodeId.fingerprint} from previous step"
          )
        )
        // init topology manager
        nsd <- EitherT.fromEither[FutureUnlessShutdown](
          NamespaceDelegation.create(
            Namespace(namespaceKey.fingerprint),
            namespaceKey,
            isRootDelegation = true,
          )
        )
        _ <- authorizeStateUpdate(Seq(namespaceKey.fingerprint), nsd, ProtocolVersion.latest)
        // all nodes need a signing key
        signingKey <- CantonNodeBootstrapImpl
          .getOrCreateSigningKey(crypto)(s"$name-signing")
        // key owner id depends on the type of node
        ownerId = member(nodeId)
        // participants need also an encryption key
        keys <-
          if (ownerId.code == ParticipantId.Code) {
            for {
              encryptionKey <- CantonNodeBootstrapImpl
                .getOrCreateEncryptionKey(crypto)(
                  s"$name-encryption"
                )
            } yield NonEmpty.mk(Seq, signingKey, encryptionKey)
          } else {
            EitherT.rightT[FutureUnlessShutdown, String](NonEmpty.mk(Seq, signingKey))
          }
        // register the keys
        _ <- authorizeStateUpdate(
          Seq(namespaceKey.fingerprint, signingKey.fingerprint),
          OwnerToKeyMapping(ownerId, None, keys),
          ProtocolVersion.latest,
        )
      } yield Some(())
    }

    private def authorizeStateUpdate(
        keys: Seq[Fingerprint],
        mapping: TopologyMapping,
        protocolVersion: ProtocolVersion,
    )(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, String, Unit] = {
      topologyManager
        .proposeAndAuthorize(
          TopologyChangeOp.Replace,
          mapping,
          serial = None,
          keys,
          protocolVersion,
          expectFullAuthorization = true,
        )
        // TODO(#14048) error handling
        .leftMap(_.toString)
        .map(_ => ())
    }

  }

  override protected def onClosed(): Unit = {
    Lifecycle.close(clock, initQueue, adminServerRegistry, adminServer, startupStage)(
      logger
    )
    super.onClosed()
  }

}

object CantonNodeBootstrapImpl {

  def getOrCreateSigningKey(crypto: Crypto)(
      name: String
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, String, SigningPublicKey] =
    getOrCreateKey(
      "signing",
      crypto.cryptoPublicStore.findSigningKeyIdByName,
      name =>
        crypto
          .generateSigningKey(name = name)
          .leftMap(_.toString),
      crypto.cryptoPrivateStore.existsSigningKey,
      name,
    )

  def getOrCreateSigningKeyByFingerprint(crypto: Crypto)(
      fingerprint: Fingerprint
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, String, SigningPublicKey] =
    getKeyByFingerprint(
      "signing",
      crypto.cryptoPublicStore.findSigningKeyIdByFingerprint,
      crypto.cryptoPrivateStore.existsSigningKey,
      fingerprint,
    )

  def getOrCreateEncryptionKey(crypto: Crypto)(
      name: String
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, String, EncryptionPublicKey] =
    getOrCreateKey(
      "encryption",
      crypto.cryptoPublicStore.findEncryptionKeyIdByName,
      name => crypto.generateEncryptionKey(name = name).leftMap(_.toString),
      crypto.cryptoPrivateStore.existsDecryptionKey,
      name,
    )

  private def getKeyByFingerprint[P <: PublicKey](
      typ: String,
      findPubKeyIdByFingerprint: Fingerprint => EitherT[
        FutureUnlessShutdown,
        CryptoPublicStoreError,
        Option[P],
      ],
      existPrivateKeyByFp: Fingerprint => EitherT[
        FutureUnlessShutdown,
        CryptoPrivateStoreError,
        Boolean,
      ],
      fingerprint: Fingerprint,
  )(implicit ec: ExecutionContext): EitherT[FutureUnlessShutdown, String, P] = for {
    keyIdO <- findPubKeyIdByFingerprint(fingerprint)
      .leftMap(err =>
        s"Failure while looking for $typ fingerprint $fingerprint in public store: $err"
      )
    pubKey <- keyIdO.fold(
      EitherT.leftT[FutureUnlessShutdown, P](
        s"$typ key with fingerprint $fingerprint does not exist"
      )
    ) { keyWithFingerprint =>
      val fingerprint = keyWithFingerprint.fingerprint
      existPrivateKeyByFp(fingerprint)
        .leftMap(err =>
          s"Failure while looking for $typ key $fingerprint in private key store: $err"
        )
        .transform {
          case Right(true) => Right(keyWithFingerprint)
          case Right(false) =>
            Left(s"Broken private key store: Could not find $typ key $fingerprint")
          case Left(err) => Left(err)
        }
    }
  } yield pubKey

  private def getOrCreateKey[P <: PublicKey](
      typ: String,
      findPubKeyIdByName: KeyName => EitherT[FutureUnlessShutdown, CryptoPublicStoreError, Option[
        P
      ]],
      generateKey: Option[KeyName] => EitherT[FutureUnlessShutdown, String, P],
      existPrivateKeyByFp: Fingerprint => EitherT[
        FutureUnlessShutdown,
        CryptoPrivateStoreError,
        Boolean,
      ],
      name: String,
  )(implicit ec: ExecutionContext): EitherT[FutureUnlessShutdown, String, P] = for {
    keyName <- EitherT.fromEither[FutureUnlessShutdown](KeyName.create(name))
    keyIdO <- findPubKeyIdByName(keyName)
      .leftMap(err => s"Failure while looking for $typ key $name in public store: $err")
    pubKey <- keyIdO.fold(
      generateKey(Some(keyName))
        .leftMap(err => s"Failure while generating $typ key for $name: $err")
    ) { keyWithName =>
      val fingerprint = keyWithName.fingerprint
      existPrivateKeyByFp(fingerprint)
        .leftMap(err =>
          s"Failure while looking for $typ key $fingerprint of $name in private key store: $err"
        )
        .transform {
          case Right(true) => Right(keyWithName)
          case Right(false) =>
            Left(s"Broken private key store: Could not find $typ key $fingerprint of $name")
          case Left(err) => Left(err)
        }
    }
  } yield pubKey

}
