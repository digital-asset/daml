// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.environment

import better.files.File
import cats.Applicative
import cats.data.EitherT
import cats.syntax.either.*
import com.daml.metrics.HealthMetrics
import com.daml.metrics.api.MetricHandle.LabeledMetricsFactory
import com.daml.metrics.api.testing.InMemoryMetricsFactory
import com.daml.metrics.api.{MetricName, MetricsContext}
import com.daml.metrics.grpc.GrpcServerMetrics
import com.digitalasset.canton.*
import com.digitalasset.canton.auth.CantonAdminToken
import com.digitalasset.canton.concurrent.{
  ExecutionContextIdlenessExecutorService,
  FutureSupervisor,
}
import com.digitalasset.canton.config.*
import com.digitalasset.canton.config.StartupMemoryCheckConfig.ReportingLevel
import com.digitalasset.canton.crypto.store.CryptoPrivateStore.CommunityCryptoPrivateStoreFactory
import com.digitalasset.canton.crypto.{CommunityCryptoFactory, Crypto}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.health.{
  DependenciesHealthService,
  GrpcHealthReporter,
  LivenessHealthService,
}
import com.digitalasset.canton.lifecycle.{LifeCycle, ShutdownFailedException}
import com.digitalasset.canton.metrics.{
  CommonMockMetrics,
  DbStorageMetrics,
  DeclarativeApiMetrics,
  LedgerApiServerMetrics,
  OnDemandMetricsReader,
}
import com.digitalasset.canton.networking.grpc.CantonMutableHandlerRegistry
import com.digitalasset.canton.resource.{
  CommunityDbMigrationsFactory,
  CommunityStorageFactory,
  Storage,
}
import com.digitalasset.canton.sequencing.client.SequencerClientConfig
import com.digitalasset.canton.telemetry.ConfiguredOpenTelemetry
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.topology.client.SynchronizerTopologyClientWithInit
import com.digitalasset.canton.topology.store.{TopologyStore, TopologyStoreId}
import com.digitalasset.canton.topology.{
  AuthorizedTopologyManager,
  Member,
  SynchronizerTopologyManager,
  UniqueIdentifier,
}
import com.digitalasset.canton.tracing.TracingConfig
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.PekkoUtil
import com.digitalasset.canton.version.ProtocolVersion
import io.grpc.ServerServiceDefinition
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.trace.SdkTracerProvider
import org.scalatest.Outcome
import org.scalatest.wordspec.FixtureAnyWordSpec

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future, Promise}

class NodesTest extends FixtureAnyWordSpec with BaseTest with HasExecutionContext {
  val clock = new SimClock(loggerFactory = loggerFactory)
  trait TestNode extends CantonNode
  case class TestNodeConfig()
      extends LocalNodeConfig
      with ConfigDefaults[DefaultPorts, TestNodeConfig] {
    override val init: InitConfig = InitConfig()
    override val adminApi: AdminServerConfig =
      AdminServerConfig(internalPort = Some(UniquePortGenerator.next))
    override val storage: StorageConfig = StorageConfig.Memory()
    override val crypto: CommunityCryptoConfig = CommunityCryptoConfig()
    override val sequencerClient: SequencerClientConfig = SequencerClientConfig()
    override val nodeTypeName: String = "test-node"
    override def clientAdminApi = adminApi.clientConfig
    override def withDefaults(ports: DefaultPorts): TestNodeConfig = this
    override val monitoring: NodeMonitoringConfig = NodeMonitoringConfig()
    override val topology: TopologyConfig = TopologyConfig.NotUsed
    override def parameters: LocalNodeParametersConfig = new LocalNodeParametersConfig {
      override def batching: BatchingConfig = BatchingConfig()
      override def caching: CachingConfigs = CachingConfigs()
      override def alphaVersionSupport: Boolean = false
      override def watchdog: Option[WatchdogConfig] = None
      override def sessionSigningKeys: SessionSigningKeysConfig = SessionSigningKeysConfig.disabled
    }
  }

  case class TestNodeParameters(
      tracing: TracingConfig = TracingConfig(),
      delayLoggingThreshold: time.NonNegativeFiniteDuration = time.NonNegativeFiniteDuration.Zero,
      logQueryCost: Option[QueryCostMonitoringConfig] = None,
      loggingConfig: LoggingConfig = LoggingConfig(),
      enableAdditionalConsistencyChecks: Boolean = false,
      enablePreviewFeatures: Boolean = false,
      processingTimeouts: ProcessingTimeout = DefaultProcessingTimeouts.testing,
      sequencerClient: SequencerClientConfig = SequencerClientConfig(),
      cachingConfigs: CachingConfigs = CachingConfigs(),
      batchingConfig: BatchingConfig = BatchingConfig(),
      nonStandardConfig: Boolean = false,
      dbMigrateAndStart: Boolean = false,
      disableUpgradeValidation: Boolean = false,
      sessionSigningKeys: SessionSigningKeysConfig = SessionSigningKeysConfig.disabled,
      alphaVersionSupport: Boolean = false,
      betaVersionSupport: Boolean = false,
      dontWarnOnDeprecatedPV: Boolean = false,
      initialProtocolVersion: ProtocolVersion = testedProtocolVersion,
      exitOnFatalFailures: Boolean = true,
      watchdog: Option[WatchdogConfig] = None,
      startupMemoryCheckConfig: StartupMemoryCheckConfig = StartupMemoryCheckConfig(
        ReportingLevel.Warn
      ),
  ) extends CantonNodeParameters

  private val metricsFactory: LabeledMetricsFactory = new InMemoryMetricsFactory
  case class TestMetrics(
      prefix: MetricName = MetricName("test-metrics"),
      openTelemetryMetricsFactory: LabeledMetricsFactory = metricsFactory,
      grpcMetrics: GrpcServerMetrics = LedgerApiServerMetrics.ForTesting.grpc,
      healthMetrics: HealthMetrics = LedgerApiServerMetrics.ForTesting.health,
      storageMetrics: DbStorageMetrics = CommonMockMetrics.dbStorage,
  ) extends BaseMetrics {
    override val declarativeApiMetrics: DeclarativeApiMetrics =
      new DeclarativeApiMetrics(prefix, openTelemetryMetricsFactory)(MetricsContext.Empty)
  }

  def factoryArguments(config: TestNodeConfig) =
    NodeFactoryArguments[TestNodeConfig, CantonNodeParameters, TestMetrics](
      name = "test-node",
      config = config,
      parameters = new TestNodeParameters,
      clock = clock,
      metrics = TestMetrics(),
      testingConfig = TestingConfigInternal(),
      futureSupervisor = FutureSupervisor.Noop,
      loggerFactory = loggerFactory,
      writeHealthDumpToFile = (_: File) => Future.failed(new RuntimeException("Not implemented")),
      configuredOpenTelemetry = ConfiguredOpenTelemetry(
        OpenTelemetrySdk.builder().build(),
        SdkTracerProvider.builder(),
        OnDemandMetricsReader.NoOpOnDemandMetricsReader$,
      ),
      executionContext = parallelExecutionContext,
    )

  def arguments(config: TestNodeConfig) = factoryArguments(config)
    .toCantonNodeBootstrapCommonArguments(
      storageFactory = new CommunityStorageFactory(StorageConfig.Memory()),
      cryptoFactory = new CommunityCryptoFactory,
      cryptoPrivateStoreFactory = new CommunityCryptoPrivateStoreFactory,
    )
    .value

  private val actorSystem =
    PekkoUtil.createActorSystem(loggerFactory.threadName)(parallelExecutionContext)

  override def afterAll(): Unit = {
    LifeCycle.toCloseableActorSystem(actorSystem, logger, timeouts).close()
    super.afterAll()
  }

  class TestNodeBootstrap(config: TestNodeConfig)
      extends CantonNodeBootstrapImpl[TestNode, TestNodeConfig, CantonNodeParameters, TestMetrics](
        arguments(config)
      )(executorService, scheduledExecutor(), actorSystem) {
    implicit val parallelApplicative: Applicative[Future] = parallelApplicativeFuture(
      executionContext
    )

    override def metrics: BaseMetrics = TestNodeBootstrap.this.arguments.metrics

    override protected val adminTokenConfig: Option[String] = None

    override protected def customNodeStages(
        storage: Storage,
        crypto: Crypto,
        adminServerRegistry: CantonMutableHandlerRegistry,
        adminToken: CantonAdminToken,
        nodeId: UniqueIdentifier,
        manager: AuthorizedTopologyManager,
        healthReporter: GrpcHealthReporter,
        healthService: DependenciesHealthService,
    ): BootstrapStageOrLeaf[TestNode] = ???
    override protected def member(
        uid: UniqueIdentifier
    ): Member = ???
    override protected def mkNodeHealthService(
        storage: Storage
    ): (DependenciesHealthService, LivenessHealthService) =
      ???

    override protected def bindNodeStatusService(): ServerServiceDefinition = ???

    override def start(): EitherT[Future, String, Unit] =
      EitherT.pure[Future, String](())
    override protected def lookupTopologyClient(
        storeId: TopologyStoreId
    ): Option[SynchronizerTopologyClientWithInit] = ???

    override protected def sequencedTopologyStores
        : Seq[TopologyStore[TopologyStoreId.SynchronizerStore]] = Nil

    override protected def sequencedTopologyManagers: Seq[SynchronizerTopologyManager] = Nil

  }

  class TestNodeFactory {
    private class CreateResult(result: => TestNodeBootstrap) {
      def get = result
    }
    private val createResult = new AtomicReference[CreateResult](
      new CreateResult(new TestNodeBootstrap(TestNodeConfig()))
    )
    def setupCreate(result: => TestNodeBootstrap): Unit =
      createResult.set(new CreateResult(result))

    def create(): TestNodeBootstrap =
      createResult.get.get
  }

  class TestNodes(factory: TestNodeFactory, configs: Map[String, TestNodeConfig])
      extends ManagedNodes[TestNode, TestNodeConfig, CantonNodeParameters, TestNodeBootstrap](
        (_, _) => factory.create(),
        new CommunityDbMigrationsFactory(loggerFactory),
        timeouts,
        configs,
        _ => MockedNodeParameters.cantonNodeParameters(),
        startUpGroup = 0,
        NodesTest.this.loggerFactory,
      ) {
    protected val executionContext: ExecutionContextIdlenessExecutorService =
      NodesTest.this.executorService
  }

  class Env {
    val config = TestNodeConfig()
    val configs = Map(
      "n1" -> config
    )
    val nodeFactory = new TestNodeFactory
    val nodes = new TestNodes(nodeFactory, configs)
  }

  override type FixtureParam = Env

  override def withFixture(test: OneArgTest): Outcome = {
    val f: FixtureParam = new Env()
    try {
      withFixture(test.toNoArgTest(f))
    } finally {
      f.nodes.close()
    }
  }

  "starting a node" should {
    "return config not found error if using a bad id" in { f =>
      f.nodes.startAndWait("nope") shouldEqual Left(ConfigurationNotFound("nope"))
    }
    "not error if the node is already running when we try to start" in { f =>
      f.nodes.startAndWait("n1").map(_ => ()) shouldBe Either.unit // first create should work
      f.nodes.startAndWait("n1").map(_ => ()) shouldBe Either.unit // second is now a noop
    }
    "return an initialization failure if an exception is thrown during startup" in { f =>
      val exception = new RuntimeException("Nope!")
      f.nodeFactory.setupCreate(throw exception)
      the[RuntimeException] thrownBy Await.result(
        f.nodes.start("n1").value,
        10.seconds,
      ) shouldBe exception
    }
    "return a proper left if startup fails" in { f =>
      val node = new TestNodeBootstrap(f.config) {
        override def start(): EitherT[Future, String, Unit] = EitherT.leftT("HelloBello")
      }
      f.nodeFactory.setupCreate(node)
      f.nodes.startAndWait("n1") shouldBe Left(StartFailed("n1", "HelloBello"))
      node.isClosing shouldBe true
    }
  }
  "stopping a node" should {
    "return config not found error if using a bad id" in { f =>
      f.nodes.stopAndWait("nope") shouldEqual Left(ConfigurationNotFound("nope"))
    }
    "return successfully if the node is not running" in { f =>
      f.nodes.stopAndWait("n1") shouldBe Either.unit
    }
    "return an initialization failure if an exception is thrown during shutdown" in { f =>
      val anException = new RuntimeException("Nope!")
      val node = new TestNodeBootstrap(f.config) {
        override def onClosed() =
          throw anException
      }
      f.nodeFactory.setupCreate(node)

      f.nodes.startAndWait("n1") shouldBe Either.unit

      loggerFactory.assertThrowsAndLogs[ShutdownFailedException](
        f.nodes.stopAndWait("n1"),
        entry => {
          entry.warningMessage should fullyMatch regex "Closing .* failed! Reason:"
          entry.throwable.value shouldBe anException
        },
      )
    }
    "properly stop a running node" in { f =>
      f.nodeFactory.setupCreate(new TestNodeBootstrap(f.config))
      f.nodes.startAndWait("n1") shouldBe Either.unit
      f.nodes.isRunning("n1") shouldBe true
      f.nodes.stopAndWait("n1") shouldBe Either.unit
      f.nodes.isRunning("n1") shouldBe false
    }
  }

  private def startStopBehavior(f: Env, startupResult: Either[String, Unit]): Unit = {
    val startPromise = Promise[Either[String, Unit]]()
    val startReached = Promise[Unit]()
    val node = new TestNodeBootstrap(f.config) {
      override def start(): EitherT[Future, String, Unit] = {
        startReached.success(())
        EitherT(startPromise.future)
      }
    }
    f.nodeFactory.setupCreate(node)
    val start = f.nodes.start("n1")
    startReached.future.futureValue // wait until start happened
    val stop = f.nodes.stop("n1")
    // push start result
    startPromise.success(startupResult)
    // node should be properly closed and stop should succeed
    stop.value.futureValue shouldBe Either.unit
    node.isClosing shouldBe true
    // wait for start to be have completed all callbacks including removing n1 from nodes.
    start.value.futureValue.discard
    f.nodes.isRunning("n1") shouldBe false
    startupResult match {
      case Left(value) => start.value.futureValue shouldBe Left(StartFailed("n1", value))
      case Right(_) => start.value.futureValue.isRight shouldBe true
    }

  }

  "work when we are just starting" when {
    "start succeeded" in { f =>
      startStopBehavior(f, Either.unit)
    }
    "start failed" in { f =>
      startStopBehavior(f, Left("Stinky"))
    }
  }
}
