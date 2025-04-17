// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.environment

import better.files.File
import cats.data.{EitherT, OptionT}
import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.functorFilter.*
import cats.syntax.traverse.*
import com.daml.metrics.HealthMetrics
import com.daml.metrics.api.MetricHandle.Gauge.CloseableGauge
import com.daml.metrics.api.MetricHandle.LabeledMetricsFactory
import com.daml.metrics.api.MetricName
import com.daml.metrics.grpc.GrpcServerMetrics
import com.daml.nonempty.NonEmpty
import com.daml.tracing.DefaultOpenTelemetry
import com.digitalasset.canton.admin.health.v30.StatusServiceGrpc
import com.digitalasset.canton.auth.CantonAdminToken
import com.digitalasset.canton.concurrent.{
  ExecutionContextIdlenessExecutorService,
  FutureSupervisor,
}
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.InitConfigBase.NodeIdentifierConfig
import com.digitalasset.canton.config.{
  CryptoConfig,
  IdentityConfig,
  LocalNodeConfig,
  ProcessingTimeout,
  TestingConfigInternal,
}
import com.digitalasset.canton.connection.GrpcApiInfoService
import com.digitalasset.canton.connection.v30.ApiInfoServiceGrpc
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.admin.grpc.GrpcVaultService
import com.digitalasset.canton.crypto.admin.v30.VaultServiceGrpc
import com.digitalasset.canton.crypto.kms.KmsFactory
import com.digitalasset.canton.crypto.store.{CryptoPrivateStoreError, CryptoPrivateStoreFactory}
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
  LifeCycle,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.{DbStorageMetrics, DeclarativeApiMetrics}
import com.digitalasset.canton.networking.grpc.{
  CantonGrpcUtil,
  CantonMutableHandlerRegistry,
  CantonServerBuilder,
}
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
  IdentityProvidingServiceClient,
  SynchronizerTopologyClient,
}
import com.digitalasset.canton.topology.processing.{
  EffectiveTime,
  InitialTopologySnapshotValidator,
  SequencedTime,
}
import com.digitalasset.canton.topology.store.TopologyStoreId.{
  AuthorizedStore,
  SynchronizerStore,
  TemporaryStore,
}
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.topology.store.{
  InitializationStore,
  StoredTopologyTransaction,
  StoredTopologyTransactions,
  TopologyStore,
  TopologyStoreId,
}
import com.digitalasset.canton.topology.transaction.DelegationRestriction.{
  CanSignAllButNamespaceDelegations,
  CanSignAllMappings,
}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.PositiveSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.{
  DelegationRestriction,
  NamespaceDelegation,
  OwnerToKeyMapping,
  SignedTopologyTransaction,
  TopologyChangeOp,
  TopologyMapping,
}
import com.digitalasset.canton.tracing.{NoTracing, TraceContext, TracerProvider}
import com.digitalasset.canton.util.{
  BinaryFileUtil,
  FutureUnlessShutdownUtil,
  MonadUtil,
  SimpleExecutionQueue,
  SingleUseCell,
}
import com.digitalasset.canton.version.{ProtocolVersion, ReleaseProtocolVersion}
import com.digitalasset.canton.watchdog.WatchdogService
import io.grpc.ServerServiceDefinition
import io.grpc.protobuf.services.ProtoReflectionService
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.actor.ActorSystem

import java.io.IOException
import java.util.concurrent.{Executors, ScheduledExecutorService}
import scala.annotation.unused
import scala.concurrent.{ExecutionContext, Future}

/** When a canton node is created it first has to obtain an identity before most of its services can
  * be started. This process will begin when `start` is called and will try to perform as much as
  * permitted by configuration automatically. If external action is required before this process can
  * complete `start` will return successfully but `isInitialized` will still be false. When the node
  * is successfully initialized the underlying node will be available through `getNode`.
  */
trait CantonNodeBootstrap[+T <: CantonNode]
    extends FlagCloseable
    with HasCloseContext
    with NamedLogging {

  def name: InstanceName
  def clock: Clock
  def isInitialized: Boolean

  def start(): EitherT[Future, String, Unit]

  def getNode: Option[T]

  /** Access to the private and public store to support local key inspection commands */
  def crypto: Option[Crypto]
  def isActive: Boolean

  def metrics: BaseMetrics

  /** register trigger which can be fired if the declarative state change should be triggered */
  def registerDeclarativeChangeTrigger(trigger: () => Unit): Unit
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
  val declarativeApiMetrics: DeclarativeApiMetrics

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
    cryptoPrivateStoreFactory: CryptoPrivateStoreFactory,
    kmsFactory: KmsFactory,
    futureSupervisor: FutureSupervisor,
    loggerFactory: NamedLoggerFactory,
    writeHealthDumpToFile: HealthDumpFunction,
    configuredOpenTelemetry: ConfiguredOpenTelemetry,
    tracerProvider: TracerProvider,
)

/** CantonNodeBootstrapImpl insists that nodes have their own topology manager and that they have
  * the ability to auto-initialize their identity on their own.
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
    with NoTracing {

  override def name: InstanceName = arguments.name
  override def clock: Clock = arguments.clock
  def config: NodeConfig = arguments.config
  def parameters: ParameterConfig = arguments.parameterConfig
  override def timeouts: ProcessingTimeout = arguments.parameterConfig.processingTimeouts
  override def loggerFactory: NamedLoggerFactory = arguments.loggerFactory
  protected def futureSupervisor: FutureSupervisor = arguments.futureSupervisor
  protected def createAuthorizedTopologyManager(
      nodeId: UniqueIdentifier,
      crypto: Crypto,
      authorizedStore: TopologyStore[AuthorizedStore],
      @unused storage: Storage,
  ): AuthorizedTopologyManager =
    new AuthorizedTopologyManager(
      nodeId,
      clock,
      crypto,
      authorizedStore,
      exitOnFatalFailures = parameters.exitOnFatalFailures,
      bootstrapStageCallback.timeouts,
      futureSupervisor,
      bootstrapStageCallback.loggerFactory,
    )

  protected val cryptoConfig: CryptoConfig = config.crypto
  protected val tracerProvider: TracerProvider = arguments.tracerProvider
  protected implicit val tracer: Tracer = tracerProvider.tracer
  protected val initQueue: SimpleExecutionQueue = new SimpleExecutionQueue(
    s"init-queue-${arguments.name}",
    arguments.futureSupervisor,
    timeouts,
    loggerFactory,
    crashOnFailure = parameters.exitOnFatalFailures,
  )

  protected val declarativeChangeTrigger = new SingleUseCell[() => Unit]()
  protected def triggerDeclarativeChange(): Unit =
    declarativeChangeTrigger.get.foreach(trigger => trigger())
  override def registerDeclarativeChangeTrigger(trigger: () => Unit): Unit =
    declarativeChangeTrigger.putIfAbsent(trigger).discard

  // This absolutely must be a "def", because it is used during class initialization.
  protected def connectionPoolForParticipant: Boolean = false

  protected val ips = new IdentityProvidingServiceClient()

  private val adminApiConfig = config.adminApi
  protected def adminTokenConfig: Option[String]

  def getAdminToken: Option[String] = startupStage.getAdminToken

  protected def getNodeStatus: NodeStatus[T#Status] =
    getNode
      .map(_.status)
      .map(NodeStatus.Success(_))
      .getOrElse(NodeStatus.NotInitialized(isActive, waitingFor))

  private def waitingFor: Option[WaitingForExternalInput] = {
    def nextStage(stage: BootstrapStage[?, ?]): Option[BootstrapStage[?, ?]] =
      stage.next match {
        case Some(s: BootstrapStage[_, _]) => nextStage(s)
        case Some(_: RunningNode[_]) => None
        // BootstrapStageOrLeaf is not a sealed class, therefore we need to catch any other
        // possible subclass
        case Some(_) => None
        case None => Some(stage)
      }
    nextStage(startupStage).flatMap(_.waitingFor)
  }

  protected def registerHealthGauge(): CloseableGauge =
    arguments.metrics.healthMetrics
      .registerHealthGauge(
        name.toProtoPrimitive,
        () => getNode.exists(_.status.active),
      )

  protected def mkNodeHealthService(
      storage: Storage
  ): (DependenciesHealthService, LivenessHealthService)

  // Node specific status service need to be bound early
  protected def bindNodeStatusService(): ServerServiceDefinition

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
        executor,
        loggerFactory,
        parameters.loggingConfig.api,
        parameters.tracing,
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
      adminServerRegistry: CantonMutableHandlerRegistry,
      adminToken: CantonAdminToken,
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
    * this callback must be implemented by all node types, providing access to the synchronizer
    * topology stores which are only available in a later startup stage (sequencer and mediator
    * nodes) or in the node runtime itself (participant connected synchronizer)
    */
  protected def sequencedTopologyStores: Seq[TopologyStore[SynchronizerStore]]

  protected def sequencedTopologyManagers: Seq[SynchronizerTopologyManager]

  protected val bootstrapStageCallback = new BootstrapStage.Callback {
    override def loggerFactory: NamedLoggerFactory = CantonNodeBootstrapImpl.this.loggerFactory
    override def timeouts: ProcessingTimeout = CantonNodeBootstrapImpl.this.timeouts
    override def abortThisNodeOnStartupFailure(): Unit =
      if (parameters.exitOnFatalFailures) {
        FatalError.exitOnFatalError(s"startup of node $name failed", logger)
      } else {
        logger.error(s"Startup of node $name failed")
      }

    override val queue: SimpleExecutionQueue = initQueue
    override def ec: ExecutionContext = CantonNodeBootstrapImpl.this.executionContext
  }

  protected def lookupTopologyClient(storeId: TopologyStoreId): Option[SynchronizerTopologyClient]

  private val startupStage =
    new BootstrapStage[T, SetupCrypto](
      description = "Initialise storage",
      bootstrapStageCallback,
    ) {
      override protected def attempt()(implicit
          traceContext: TraceContext
      ): EitherT[FutureUnlessShutdown, String, Option[SetupCrypto]] =
        EitherT(
          FutureUnlessShutdown.lift(
            arguments.storageFactory
              .create(
                connectionPoolForParticipant,
                arguments.parameterConfig.loggingConfig.queryCost,
                arguments.clock,
                Some(scheduler),
                arguments.metrics.storageMetrics,
                arguments.parameterConfig.processingTimeouts,
                bootstrapStageCallback.loggerFactory,
              )
              .value
          )
        ).map { storage =>
          addCloseable(registerHealthGauge())
          // init health services once
          val (healthService, livenessService) = mkNodeHealthService(storage)
          addCloseable(healthService)
          addCloseable(livenessService)

          arguments.parameterConfig.watchdog
            .filter(_.enabled)
            .foreach { watchdogConfig =>
              val watchdog = WatchdogService.SysExitOnNotServing(
                watchdogConfig.checkInterval,
                watchdogConfig.killDelay,
                livenessService,
                bootstrap.loggerFactory,
                bootstrap.timeouts,
              )
              addCloseable(watchdog)
            }

          addCloseable(storage)
          Some(new SetupCrypto(storage, healthService, livenessService))
        }
    }

  private class SetupCrypto(
      val storage: Storage,
      healthService: DependenciesHealthService,
      livenessService: LivenessHealthService,
  ) extends BootstrapStage[T, SetupAdminApi](
        description = "Init crypto module",
        bootstrapStageCallback,
      )
      with HasCloseContext {

    override protected def attempt()(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, String, Option[SetupAdminApi]] = {
      // we check the memory configuration before starting the node
      MemoryConfigChecker.check(parameters.startupMemoryCheckConfig, logger)

      // crypto factory doesn't write to the db during startup, hence,
      // we won't have "isPassive" issues here
      performUnlessClosingEitherUSF("create-crypto")(
        Crypto
          .create(
            cryptoConfig,
            storage,
            arguments.cryptoPrivateStoreFactory,
            arguments.kmsFactory,
            ReleaseProtocolVersion.latest,
            arguments.parameterConfig.nonStandardConfig,
            arguments.futureSupervisor,
            arguments.clock,
            executionContext,
            bootstrapStageCallback.timeouts,
            bootstrapStageCallback.loggerFactory,
            tracerProvider,
          )
          .map { crypto =>
            addCloseable(crypto)
            Some(
              new SetupAdminApi(
                storage,
                crypto,
                healthService,
                livenessService,
              )
            )
          }
      )
    }
  }

  private class SetupAdminApi(
      val storage: Storage,
      val crypto: Crypto,
      healthService: DependenciesHealthService,
      livenessService: LivenessHealthService,
  ) extends BootstrapStage[T, SetupNodeId](
        description = "Stage Admin API",
        bootstrapStageCallback,
      )
      with HasCloseContext {

    private def createAdminServerRegistry(
        adminToken: CantonAdminToken
    ): Either[String, CantonMutableHandlerRegistry] = {
      // The admin-API services
      logger.info(s"Starting admin-api services on $adminApiConfig")
      val openTelemetry = new DefaultOpenTelemetry(tracerProvider.openTelemetry)
      val builder = CantonServerBuilder
        .forConfig(
          adminApiConfig,
          Some(adminToken),
          executionContext,
          bootstrapStageCallback.loggerFactory,
          arguments.parameterConfig.loggingConfig.api,
          arguments.parameterConfig.tracing,
          arguments.metrics.grpcMetrics,
          openTelemetry,
        )

      val registry = builder.mutableHandlerRegistry()

      val serverE = Either.catchOnly[IOException](
        builder.build
          .start()
      )
      serverE
        .map { server =>
          addCloseable(LifeCycle.toCloseableServer(server, logger, "AdminServer"))
          addCloseable(registry)
          registry
        }
        .leftMap(err => err.getMessage)
    }

    override protected def attempt()(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, String, Option[SetupNodeId]] = {
      // admin token is taken from the config or created per session
      val adminToken: CantonAdminToken = adminTokenConfig
        .fold(CantonAdminToken.create(crypto.pureCrypto))(token => CantonAdminToken(secret = token))
      createAdminServerRegistry(adminToken).map { adminServerRegistry =>
        val (healthReporter, grpcHealthServer, httpHealthServer) =
          mkHealthComponents(healthService, livenessService)
        grpcHealthServer.foreach(addCloseable)
        httpHealthServer.foreach(addCloseable)

        adminServerRegistry.addServiceU(bindNodeStatusService())

        adminServerRegistry.addServiceU(
          StatusServiceGrpc.bindService(
            new GrpcStatusService(
              arguments.writeHealthDumpToFile,
              arguments.parameterConfig.processingTimeouts,
              bootstrapStageCallback.loggerFactory,
            ),
            executionContext,
          )
        )
        adminServerRegistry
          .addServiceU(ProtoReflectionService.newInstance().bindService(), withLogging = false)
        adminServerRegistry.addServiceU(
          ApiInfoServiceGrpc.bindService(
            new GrpcApiInfoService(CantonGrpcUtil.ApiName.AdminApi),
            executionContext,
          )
        )

        adminServerRegistry.addServiceU(
          VaultServiceGrpc.bindService(
            new GrpcVaultService(
              crypto,
              arguments.parameterConfig.enablePreviewFeatures,
              bootstrapStageCallback.loggerFactory,
            ),
            executionContext,
          )
        )

        Some(
          new SetupNodeId(
            storage,
            crypto,
            adminServerRegistry,
            adminToken,
            healthReporter,
            healthService,
          )
        ): Option[SetupNodeId]
      }
    }.toEitherT[FutureUnlessShutdown]
  }

  private type SetupNodeIdResult =
    (UniqueIdentifier, Seq[PositiveSignedTopologyTransaction])
  private class SetupNodeId(
      storage: Storage,
      val crypto: Crypto,
      adminServerRegistry: CantonMutableHandlerRegistry,
      adminToken: CantonAdminToken,
      healthReporter: GrpcHealthReporter,
      healthService: DependenciesHealthService,
  ) extends BootstrapStageWithStorage[
        T,
        GenerateOrAwaitNodeTopologyTx,
        SetupNodeIdResult,
      ](
        description = "Init node id",
        bootstrap = bootstrapStageCallback,
        storage = storage,
        autoInit = !config.init.identity.isManual,
      )
      with HasCloseContext
      with GrpcIdentityInitializationService.Callback {
    override def getAdminToken: Option[String] = Some(adminToken.secret)

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
        ProtocolVersion.latest,
        bootstrapStageCallback.timeouts,
        bootstrapStageCallback.loggerFactory,
      )
    addCloseable(authorizedStore)

    adminServerRegistry
      .addServiceU(
        adminV30.IdentityInitializationServiceGrpc
          .bindService(
            new GrpcIdentityInitializationService(
              clock = clock,
              bootstrap = this,
              // if init is manual, then we expect the user to call the init_id endpoint of the initialization service
              nodeInitViaApi = config.init.identity.isManual,
              loggerFactory = bootstrapStageCallback.loggerFactory,
            ),
            executionContext,
          )
      )

    override def waitingFor: Option[WaitingForExternalInput] = Some(WaitingForId)

    override protected def stageCompleted(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Option[SetupNodeIdResult]] =
      initializationStore.uid.map(_.map { uid =>
        logger.info(s"Resuming with existing uid $uid")
        (uid, Seq.empty)
      })

    override protected def buildNextStage(
        result: SetupNodeIdResult
    ): EitherT[FutureUnlessShutdown, String, GenerateOrAwaitNodeTopologyTx] = {
      val (uid, transactions) = result
      EitherT.rightT(
        new GenerateOrAwaitNodeTopologyTx(
          uid,
          transactions,
          authorizedStore,
          storage,
          crypto,
          adminServerRegistry,
          adminToken,
          healthReporter,
          healthService,
        )
      )
    }

    override protected def autoCompleteStage(): EitherT[FutureUnlessShutdown, String, Option[
      SetupNodeIdResult
    ]] =
      (config.init.identity match {
        case IdentityConfig.External(identifier, namespace, certificates) =>
          initWithExternal(identifier, namespace, certificates).map(Some(_))
        case IdentityConfig.Auto(identifier) =>
          autoInitIdentifier(identifier).map(uid =>
            Some((uid, Seq.empty[PositiveSignedTopologyTransaction]))
          )
        case IdentityConfig.Manual =>
          // This should never be called, as the stage is not auto-completable
          EitherT.leftT("Manual initialization should never run auto-complete")
      })

    /** validate the certificates and the uid provided externally
      *
      * verify that they are sufficient for the given namespace of the node. this is the same code
      * path for both, the api init call and the declarative config
      *
      * @param transactions
      *   the list of namespace delegation transactions that delegate the namespace to this node the
      *   node needs to have access to a key which can create topology transactions on behalf of
      *   this node. if the list is empty, then the node needs to have access to the root key.
      */
    private def validateTransactionsAndComputeUid(
        identifier: String,
        namespace: Option[String],
        transactions: Seq[PositiveSignedTopologyTransaction],
    ): EitherT[FutureUnlessShutdown, String, UniqueIdentifier] = {
      val temporaryTopologyStore =
        new InMemoryTopologyStore(
          TemporaryStore.tryCreate(identifier),
          ProtocolVersion.latest,
          this.loggerFactory,
          this.timeouts,
        )

      val snapshotValidator = new InitialTopologySnapshotValidator(
        ProtocolVersion.latest,
        crypto.pureCrypto,
        temporaryTopologyStore,
        this.timeouts,
        this.loggerFactory,
      )

      for {
        // fails if one tx did not validate, they all must be valid
        _ <- snapshotValidator.validateAndApplyInitialTopologySnapshot(
          StoredTopologyTransactions(
            transactions.map(tx =>
              StoredTopologyTransaction(
                sequenced = SequencedTime(CantonTimestamp.Epoch),
                validFrom = EffectiveTime(CantonTimestamp.Epoch),
                validUntil = None,
                transaction = tx,
                rejectionReason = None,
              )
            )
          )
        )

        // find namespaces in certs
        namespacesFromCert <- EitherT.fromEither[FutureUnlessShutdown](
          transactions.map(_.mapping).traverse {
            case nd: NamespaceDelegation =>
              Right(nd.namespace)
            case mp => Left(s"Expected only namespace delegations, found $mp")
          }
        )
        // bail if there is more than one
        _ <- EitherT.cond[FutureUnlessShutdown](
          namespacesFromCert.toSet.sizeIs <= 1,
          (),
          s"Expected only one namespace in the certificates, found ${namespacesFromCert.toSet.size}",
        )
        // validate namespace. if both are provided, they need to match
        targetNamespace <- (
          namespace,
          namespacesFromCert.headOption,
        ) match {
          case (Some(cfg), Some(cert)) =>
            EitherT.cond[FutureUnlessShutdown](
              cfg == cert.unwrap,
              cert,
              s"Provided namespace '$cfg' does not match the certificate ${cert.unwrap} '",
            )
          case (Some(cfg), None) =>
            EitherT.fromEither[FutureUnlessShutdown](Fingerprint.fromString(cfg).map(Namespace(_)))
          case (None, Some(cert)) => EitherT.rightT[FutureUnlessShutdown, String](cert)
          case (None, None) =>
            EitherT.leftT[FutureUnlessShutdown, Namespace]("No namespace or certificate provided")
        }
        // build uid
        uid <- EitherT.fromEither[FutureUnlessShutdown](
          UniqueIdentifier.create(identifier, targetNamespace)
        )
        // verify that we have at least one signing key authorized by the topology transactions,
        // or we have access to the root key
        possibleKeys = transactions
          .map(_.mapping)
          .collect { case nd: NamespaceDelegation =>
            nd.target.fingerprint
          } :+ targetNamespace.fingerprint
        _ <- possibleKeys
          .findM(key => crypto.cryptoPrivateStore.existsSigningKey(key))
          .leftMap(_.toString)
          .subflatMap(
            _.toRight(
              "No signing key found locally for the provided namespace. The certificates are for: " + possibleKeys
                .map(_.unwrap)
                .mkString(", ")
            )
          )
      } yield uid
    }

    private def initWithExternal(
        identifier: String,
        namespace: Option[String],
        transactions: Seq[java.io.File],
    ): EitherT[FutureUnlessShutdown, String, SetupNodeIdResult] = {
      import com.digitalasset.canton.version.*
      import com.digitalasset.canton.topology.transaction.*
      for {
        parsedTransactions <- EitherT.fromEither[FutureUnlessShutdown](
          MonadUtil.sequentialTraverse(transactions) { file =>
            BinaryFileUtil
              .readByteStringFromFile(file.getPath)
              .flatMap { bytes =>
                SignedTopologyTransaction
                  .fromByteString(
                    // no validation of the protocol version, as the local topology manager is not tied to a specific version.
                    // this is consistent with the behaviour if you just add topology transactions into the authorized store
                    // (which is what we do here).
                    ProtocolVersionValidation.NoValidation,
                    ProtocolVersionValidation.NoValidation,
                    bytes,
                  )
                  .leftMap(_.message)
              }
              .flatMap(
                _.selectOp[TopologyChangeOp.Replace]
                  .toRight(
                    s"File $file does not contain a topology replacement transaction, but a remove"
                  )
              )
              .leftMap(str => s"Failed to parse certificate from file $file: $str")
          }
        )
        uid <- validateTransactionsAndComputeUid(identifier, namespace, parsedTransactions)
        _ <- EitherT.right[String](initializationStore.setUid(uid))
      } yield {
        logger.info(
          s"Initializing with external provided uid=$uid with ${parsedTransactions.length} namespace delegations"
        )
        (uid, parsedTransactions)
      }
    }

    private def autoInitIdentifier(
        identifier: NodeIdentifierConfig
    ): EitherT[FutureUnlessShutdown, String, UniqueIdentifier] =
      for {
        // create namespace key
        namespaceKey <- CantonNodeBootstrapImpl.getOrCreateSigningKey(crypto)(
          s"$name-${SigningKeyUsage.Namespace.identifier}",
          SigningKeyUsage.NamespaceOnly,
        )
        // create id
        identifierName = identifier.identifierName.getOrElse(name.unwrap)
        uid <- EitherT
          .fromEither[FutureUnlessShutdown](
            UniqueIdentifier
              .create(identifierName, namespaceKey.fingerprint)
          )
          .leftMap(err => s"Failed to convert name to identifier: $err")
        _ <- EitherT
          .right[String](initializationStore.setUid(uid))
      } yield {
        logger.info(s"Auto-initialized with uid=$uid")
        uid
      }

    override def initializeViaApi(
        identifier: String,
        namespace: String,
        transactions: Seq[PositiveSignedTopologyTransaction],
    )(implicit
        traceContext: TraceContext
    ): EitherT[Future, String, Unit] =
      validateTransactionsAndComputeUid(
        identifier,
        Option.when(namespace.nonEmpty)(namespace),
        transactions,
      )
        .flatMap { uid =>
          completeWithExternal(
            EitherT.right(
              initializationStore
                .setUid(uid)
                .failOnShutdownToAbortException("CantonNodeBootstrapImpl.initializeWithProvidedId")
                .map(_ => (uid, transactions))
            )
          )
        }
        .onShutdown(Left("Node has been shutdown"))

    override def getId: Option[UniqueIdentifier] = next.map(_.nodeId)
    override def isInitialized: Boolean = getId.isDefined
  }

  private class GenerateOrAwaitNodeTopologyTx(
      val nodeId: UniqueIdentifier,
      transactions: Seq[
        PositiveSignedTopologyTransaction
      ], // transactions that were added during init
      authorizedStore: TopologyStore[TopologyStoreId.AuthorizedStore],
      storage: Storage,
      crypto: Crypto,
      adminServerRegistry: CantonMutableHandlerRegistry,
      adminToken: CantonAdminToken,
      healthReporter: GrpcHealthReporter,
      healthService: DependenciesHealthService,
  ) extends BootstrapStageWithStorage[T, BootstrapStageOrLeaf[T], Unit](
        description = "generate-or-await-node-topology-tx",
        bootstrap = bootstrapStageCallback,
        storage = storage,
        autoInit = config.init.generateTopologyTransactionsAndKeys,
      ) {

    override def getAdminToken: Option[String] = Some(adminToken.secret)

    private val temporaryStoreRegistry =
      new TemporaryStoreRegistry(
        nodeId,
        clock,
        crypto,
        futureSupervisor,
        parameters.processingTimeouts,
        bootstrapStageCallback.loggerFactory,
      )
    addCloseable(temporaryStoreRegistry)

    private val topologyManager: AuthorizedTopologyManager =
      createAuthorizedTopologyManager(nodeId, crypto, authorizedStore, storage)
    addCloseable(topologyManager)
    adminServerRegistry
      .addServiceU(
        adminV30.TopologyManagerReadServiceGrpc
          .bindService(
            new GrpcTopologyManagerReadService(
              member(nodeId),
              temporaryStoreRegistry.stores() ++ sequencedTopologyStores :+ authorizedStore,
              crypto,
              lookupTopologyClient,
              processingTimeout = parameters.processingTimeouts,
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
              temporaryStoreRegistry.managers() ++ sequencedTopologyManagers :+ topologyManager,
              temporaryStoreRegistry,
              bootstrapStageCallback.loggerFactory,
            ),
            executionContext,
          )
      )
    adminServerRegistry
      .addServiceU(
        adminV30.TopologyAggregationServiceGrpc.bindService(
          new GrpcTopologyAggregationService(
            sequencedTopologyStores.mapFilter(
              TopologyStoreId.select[TopologyStoreId.SynchronizerStore]
            ),
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
        FutureUnlessShutdownUtil.doNotAwaitUnlessShutdown(
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
      // Add any topology transactions that were passed as part of the init process
      // This is not crash safe if we crash between storing the node-id and adding the transactions,
      // but a crash can recovered (use manual for anything).
      topologyManager
        .add(
          transactions,
          forceChanges = ForceFlags.none,
          expectFullAuthorization = true,
        )
        .leftMap(_.cause)
        .flatMap(_ => super.start())
    }

    override def waitingFor: Option[WaitingForExternalInput] = Some(WaitingForNodeTopology)

    override protected def stageCompleted(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Option[Unit]] = {
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
              case OwnerToKeyMapping(`myMember`, keys) =>
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
          adminServerRegistry,
          adminToken,
          nodeId,
          topologyManager,
          healthReporter,
          healthService,
        )
      )
    }

    /** Figure out the key we should be using to sign topology transactions
      *
      * We either use a delegated key (to which we have access) if we have certificates in our
      * store. Otherwise, we use the root key.
      *
      * If we have no certificates, we need to create a new root certificate. This is signalled
      * using the Boolean flag in the return value.
      */
    private def determineTopologySigningKeyAndNeedForRootCertificate()
        : EitherT[FutureUnlessShutdown, String, (Boolean, SigningPublicKey)] =
      EitherT
        .right(
          authorizedStore
            .findPositiveTransactions(
              CantonTimestamp.MaxValue,
              asOfInclusive = false,
              isProposal = false,
              types = Seq(NamespaceDelegation.code),
              filterUid = None,
              filterNamespace = Some(Seq(nodeId.namespace)),
            )
        )
        .flatMap { existing =>
          val possible = existing.collectOfMapping[NamespaceDelegation].result.map(_.mapping.target)
          if (possible.isEmpty) {
            crypto.cryptoPublicStore
              .signingKey(nodeId.fingerprint)
              .toRight(
                s"Performing auto-init but can't find key ${nodeId.fingerprint} from previous step"
              )
              .map(key => (true, key))
          } else {
            // reverse so we find the lowest permissible one
            possible.reverse
              .findM(key => crypto.cryptoPrivateStore.existsSigningKey(key.fingerprint))
              .leftMap(_.toString)
              .subflatMap {
                case None =>
                  Left(
                    "No matching signing key found in private crypto store. I was looking for:\n  " + possible
                      .mkString("\n  ")
                  )
                case Some(key) => Right((false, key))
              }
          }
        }

    private def createNsd(
        rootNamespaceFp: Fingerprint,
        targetKey: SigningPublicKey,
        delegationRestriction: DelegationRestriction,
    ): EitherT[FutureUnlessShutdown, String, Unit] = for {
      nsd <- EitherT.fromEither[FutureUnlessShutdown](
        NamespaceDelegation.create(
          Namespace(rootNamespaceFp),
          targetKey,
          delegationRestriction,
        )
      )
      _ <- authorizeStateUpdate(
        Seq(rootNamespaceFp),
        nsd,
        ProtocolVersion.latest,
      )
    } yield ()

    override protected def autoCompleteStage()
        : EitherT[FutureUnlessShutdown, String, Option[Unit]] =
      for {
        lookupKeyAndNeedRootCert <- determineTopologySigningKeyAndNeedForRootCertificate()
        (needRootCert, rootTopologySingingKey) = lookupKeyAndNeedRootCert
        // create root certificate if needed
        _ <-
          if (needRootCert)
            createNsd(
              rootTopologySingingKey.fingerprint,
              rootTopologySingingKey,
              CanSignAllMappings,
            )
          else EitherT.rightT[FutureUnlessShutdown, String](())
        // create intermediate certificate if desired
        topologySigningKey <-
          if (
            config.init.generateIntermediateKey && rootTopologySingingKey.fingerprint == nodeId.namespace.fingerprint
          ) {
            logger.info("Creating intermediate certificate for node")
            for {
              intermediateKey <- CantonNodeBootstrapImpl
                .getOrCreateSigningKey(crypto)(
                  s"$name-intermediate-${SigningKeyUsage.Namespace}",
                  SigningKeyUsage.NamespaceOnly,
                )
              _ <- createNsd(
                rootTopologySingingKey.fingerprint,
                intermediateKey,
                CanSignAllButNamespaceDelegations,
              )
            } yield intermediateKey
          } else EitherT.rightT[FutureUnlessShutdown, String](rootTopologySingingKey)

        // all nodes need two signing keys: (1) for sequencer authentication and (2) for protocol signing
        sequencerAuthKey <- CantonNodeBootstrapImpl
          .getOrCreateSigningKey(crypto)(
            s"$name-${SigningKeyUsage.SequencerAuthentication.identifier}",
            SigningKeyUsage.SequencerAuthenticationOnly,
          )
        signingKey <- CantonNodeBootstrapImpl
          .getOrCreateSigningKey(crypto)(
            s"$name-${SigningKeyUsage.Protocol.identifier}",
            SigningKeyUsage.ProtocolOnly,
          )
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
            } yield NonEmpty.mk(Seq, sequencerAuthKey, signingKey, encryptionKey)
          } else {
            EitherT.rightT[FutureUnlessShutdown, String](
              NonEmpty.mk(Seq, sequencerAuthKey, signingKey)
            )
          }
        // register the keys
        _ <- authorizeStateUpdate(
          Seq(
            topologySigningKey.fingerprint,
            sequencerAuthKey.fingerprint,
            signingKey.fingerprint,
          ),
          OwnerToKeyMapping(ownerId, keys),
          ProtocolVersion.latest,
        )
      } yield Some(())

    private def authorizeStateUpdate(
        keys: Seq[Fingerprint],
        mapping: TopologyMapping,
        protocolVersion: ProtocolVersion,
    )(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, String, Unit] =
      topologyManager
        .proposeAndAuthorize(
          TopologyChangeOp.Replace,
          mapping,
          serial = None,
          keys,
          protocolVersion,
          expectFullAuthorization = true,
          waitToBecomeEffective = None,
        )
        .leftMap(_.toString)
        .map(_ => ())

  }

  override protected def onClosed(): Unit = {
    LifeCycle.close(clock, initQueue, startupStage)(
      logger
    )
    super.onClosed()
  }

}

object CantonNodeBootstrapImpl {

  def getOrCreateSigningKey(crypto: Crypto)(
      name: String,
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, String, SigningPublicKey] =
    getOrCreateKey(
      "signing",
      crypto.cryptoPublicStore.findSigningKeyIdByName,
      name =>
        crypto
          .generateSigningKey(usage = usage, name = name)
          .leftMap(_.toString),
      crypto.cryptoPrivateStore.existsSigningKey,
      name,
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

  private def getOrCreateKey[P <: PublicKey](
      typ: String,
      findPubKeyIdByName: KeyName => OptionT[FutureUnlessShutdown, P],
      generateKey: Option[KeyName] => EitherT[FutureUnlessShutdown, String, P],
      existPrivateKeyByFp: Fingerprint => EitherT[
        FutureUnlessShutdown,
        CryptoPrivateStoreError,
        Boolean,
      ],
      name: String,
  )(implicit ec: ExecutionContext): EitherT[FutureUnlessShutdown, String, P] = for {
    keyName <- EitherT.fromEither[FutureUnlessShutdown](KeyName.create(name))
    keyIdO <- EitherT.right(findPubKeyIdByName(keyName).value)
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
