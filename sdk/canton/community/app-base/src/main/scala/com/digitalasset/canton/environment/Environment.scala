// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.environment

import better.files.File
import cats.data.EitherT
import cats.syntax.either.*
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.metrics.api.{HistogramInventory, MetricsInfoFilter}
import com.digitalasset.canton.concurrent.*
import com.digitalasset.canton.config.*
import com.digitalasset.canton.console.{
  ConsoleEnvironment,
  ConsoleOutput,
  GrpcAdminCommandRunner,
  HealthDumpGenerator,
  StandardConsoleOutput,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.environment.CantonNodeBootstrap.HealthDumpFunction
import com.digitalasset.canton.environment.Environment.*
import com.digitalasset.canton.lifecycle.LifeCycle
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.MetricsConfig.JvmMetrics
import com.digitalasset.canton.metrics.{CantonHistograms, MetricsRegistry}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.participant.*
import com.digitalasset.canton.resource.DbMigrationsFactory
import com.digitalasset.canton.synchronizer.mediator.{MediatorNodeBootstrap, MediatorNodeParameters}
import com.digitalasset.canton.synchronizer.metrics.MediatorMetrics
import com.digitalasset.canton.synchronizer.sequencing.SequencerNodeBootstrap
import com.digitalasset.canton.telemetry.{ConfiguredOpenTelemetry, OpenTelemetryFactory}
import com.digitalasset.canton.time.*
import com.digitalasset.canton.tracing.TraceContext.withNewTraceContext
import com.digitalasset.canton.tracing.{NoTracing, TraceContext, TracerProvider}
import com.digitalasset.canton.util.FutureInstances.parallelFuture
import com.digitalasset.canton.util.{MonadUtil, PekkoUtil, SingleUseCell}
import com.google.common.annotations.VisibleForTesting
import io.circe.Encoder
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.actor.ActorSystem
import org.slf4j.bridge.SLF4JBridgeHandler

import java.util.concurrent.ScheduledExecutorService
import scala.collection.mutable.ListBuffer
import scala.concurrent.{Future, blocking}
import scala.util.control.NonFatal

/** Holds all significant resources held by this process.
  */
trait Environment extends NamedLogging with AutoCloseable with NoTracing {

  type Config <: CantonConfig & ConfigDefaults[DefaultPorts, Config]
  type Console <: ConsoleEnvironment

  val config: Config
  val testingConfig: TestingConfigInternal

  val loggerFactory: NamedLoggerFactory

  implicit val scheduler: ScheduledExecutorService =
    Threading.singleThreadScheduledExecutor(
      loggerFactory.threadName + "-env-sched",
      noTracingLogger,
    )

  private val histogramInventory = new HistogramInventory()
  private val histograms = new CantonHistograms()(histogramInventory)
  private val baseFilter = new MetricsInfoFilter(
    config.monitoring.metrics.globalFilters,
    config.monitoring.metrics.qualifiers.toSet,
  )
  lazy val configuredOpenTelemetry: ConfiguredOpenTelemetry =
    OpenTelemetryFactory.initializeOpenTelemetry(
      initializeGlobalOpenTelemetry = testingConfig.initializeGlobalOpenTelemetry,
      testingSupportAdhocMetrics = testingConfig.supportAdhocMetrics,
      metricsEnabled = config.monitoring.metrics.reporters.nonEmpty,
      attachReporters = MetricsRegistry
        .registerReporters(config.monitoring.metrics, loggerFactory),
      config = config.monitoring.tracing.tracer,
      histogramInventory = histogramInventory,
      histogramFilter = baseFilter,
      histogramConfigs = config.monitoring.metrics.histograms,
      config.monitoring.metrics.cardinality.unwrap,
      loggerFactory,
    )

  config.monitoring.metrics.jvmMetrics
    .foreach(JvmMetrics.setup(_, configuredOpenTelemetry.openTelemetry))

  lazy val metricsRegistry: MetricsRegistry = new MetricsRegistry(
    configuredOpenTelemetry.openTelemetry.meterBuilder("canton").build(),
    testingConfig.metricsFactoryType,
    testingConfig.supportAdhocMetrics,
    histograms,
    baseFilter,
    loggerFactory,
  )

  protected def participantNodeFactory
      : ParticipantNodeBootstrap.Factory[Config#ParticipantConfigType, ParticipantNodeBootstrap]
  protected def migrationsFactory: DbMigrationsFactory

  def isEnterprise: Boolean

  def createConsole(
      consoleOutput: ConsoleOutput = StandardConsoleOutput
  ): Console = {
    val console = _createConsole(consoleOutput)
    healthDumpGenerator
      .putIfAbsent(createHealthDumpGenerator(console.grpcAdminCommandRunner))
      .discard
    console
  }

  protected def _createConsole(
      consoleOutput: ConsoleOutput = StandardConsoleOutput
  ): Console

  @VisibleForTesting
  protected def createHealthDumpGenerator(
      commandRunner: GrpcAdminCommandRunner
  ): HealthDumpGenerator =
    new HealthDumpGenerator(this, commandRunner)

  /* We can't reliably use the health administration instance of the console because:
   * 1) it's tied to the console environment, which we don't have access to yet when the environment is instantiated
   * 2) there might never be a console environment when running in daemon mode
   * Therefore we create an immutable lazy value for the health administration that can be set either with the console
   * health admin when/if it gets created, or with a headless health admin, whichever comes first.
   */
  private val healthDumpGenerator = new SingleUseCell[HealthDumpGenerator]

  // Function passed down to the node boostrap used to generate a health dump file
  val writeHealthDumpToFile: HealthDumpFunction = (file: File) =>
    Future {
      healthDumpGenerator
        .getOrElse {
          val tracerProvider =
            TracerProvider.Factory(configuredOpenTelemetry, "admin_command_runner")
          implicit val tracer: Tracer = tracerProvider.tracer

          val commandRunner =
            new GrpcAdminCommandRunner(
              this,
              config.parameters.timeouts.console,
              CantonGrpcUtil.ApiName.AdminApi,
            )
          val newGenerator = createHealthDumpGenerator(commandRunner)
          val previous = healthDumpGenerator.putIfAbsent(newGenerator)
          previous match {
            // If somehow the cell was set concurrently in the meantime, close the newly created command runner and use
            // the existing one
            case Some(value) =>
              commandRunner.close()
              value
            case None =>
              newGenerator
          }
        }
        .generateHealthDump(file)
    }

  installJavaUtilLoggingBridge()
  logger.debug(config.portDescription)

  private val numThreads = Threading.detectNumberOfThreads(noTracingLogger)
  implicit val executionContext: ExecutionContextIdlenessExecutorService =
    Threading.newExecutionContext(
      loggerFactory.threadName + "-env-ec",
      noTracingLogger,
      numThreads,
    )

  private val deadlockConfig = config.monitoring.deadlockDetection
  protected def timeouts: ProcessingTimeout = config.parameters.timeouts.processing

  protected val futureSupervisor =
    if (config.monitoring.logging.logSlowFutures)
      new FutureSupervisor.Impl(timeouts.slowFutureWarn)
    else FutureSupervisor.Noop

  private val monitorO = if (deadlockConfig.enabled) {
    val mon = new ExecutionContextMonitor(
      loggerFactory,
      deadlockConfig.interval.toInternal,
      deadlockConfig.warnInterval.toInternal,
      timeouts,
    )
    mon.monitor(executionContext)
    Some(mon)
  } else None

  implicit val actorSystem: ActorSystem = PekkoUtil.createActorSystem(loggerFactory.threadName)

  implicit val executionSequencerFactory: ExecutionSequencerFactory =
    PekkoUtil.createExecutionSequencerFactory(
      loggerFactory.threadName + "-admin-workflow-services",
      // don't log the number of threads twice, as we log it already when creating the first pool
      NamedLogging.noopNoTracingLogger,
    )

  // additional closeables
  private val userCloseables = ListBuffer[AutoCloseable]()

  /** Sim-clock if environment is using static time
    */
  val simClock: Option[DelegatingSimClock] = config.parameters.clock match {
    case ClockConfig.SimClock =>
      logger.info("Starting environment with sim-clock")
      Some(
        new DelegatingSimClock(
          () =>
            runningNodes.map(_.clock).collect { case c: SimClock =>
              c
            },
          loggerFactory = loggerFactory,
        )
      )
    case ClockConfig.WallClock(_) => None
    case ClockConfig.RemoteClock(_) => None
  }

  val clock: Clock = simClock.getOrElse(createClock(None))

  protected def createClock(nodeTypeAndName: Option[(String, String)]): Clock = {
    val clockLoggerFactory = nodeTypeAndName.fold(loggerFactory) { case (nodeType, name) =>
      loggerFactory.append(nodeType, name)
    }
    config.parameters.clock match {
      case ClockConfig.SimClock =>
        val parent = simClock.getOrElse(sys.error("This should not happen"))
        val clock = new SimClock(
          parent.start,
          clockLoggerFactory,
        )
        clock.advanceTo(parent.now)
        clock
      case ClockConfig.RemoteClock(clientConfig) =>
        RemoteClock(
          clientConfig,
          config.parameters.timeouts.processing,
          clockLoggerFactory,
        )
      case ClockConfig.WallClock(skewW) =>
        val skewMs = skewW.asJava.toMillis
        val tickTock =
          if (skewMs == 0) TickTock.Native
          else new TickTock.RandomSkew(Math.min(skewMs, Int.MaxValue).toInt)
        new WallClock(timeouts, clockLoggerFactory, tickTock)
    }
  }

  private val testingTimeService = new TestingTimeService(clock, () => simClocks)

  lazy val participants =
    new ParticipantNodes[ParticipantNodeBootstrap, ParticipantNode, Config#ParticipantConfigType](
      createParticipant,
      migrationsFactory,
      timeouts,
      config.participantsByString,
      config.participantNodeParametersByString,
      loggerFactory,
    )

  val sequencers = new SequencerNodes(
    createSequencer,
    migrationsFactory,
    timeouts,
    config.sequencersByString,
    config.sequencerNodeParametersByString,
    loggerFactory,
  )

  val mediators =
    new MediatorNodes(
      createMediator,
      migrationsFactory,
      timeouts,
      config.mediatorsByString,
      config.mediatorNodeParametersByString,
      loggerFactory,
    )

  // convenient grouping of all node collections for performing operations
  // intentionally defined in the order we'd like to start them
  protected def allNodes: List[Nodes[CantonNode, CantonNodeBootstrap[CantonNode]]] =
    List(sequencers, mediators, participants)
  private def runningNodes: Seq[CantonNodeBootstrap[CantonNode]] = allNodes.flatMap(_.running)

  private def autoConnectLocalNodes(): Either[StartupError, Unit] =
    // TODO(#14048) extend this to x-nodes
    Left(StartFailed("participants", "auto connect local nodes not yet implemented"))

  /** Try to startup all nodes in the configured environment and reconnect them to one another.
    * The first error will prevent further nodes from being started.
    * If an error is returned previously started nodes will not be stopped.
    */
  def startAndReconnect(autoConnectLocal: Boolean): Either[StartupError, Unit] =
    withNewTraceContext { implicit traceContext =>
      if (config.parameters.manualStart) {
        logger.info("Manual start requested.")
        Either.unit
      } else {
        logger.info("Automatically starting all instances")
        val startup = for {
          _ <- startAll()
          _ <- reconnectParticipants
          _ <- if (autoConnectLocal) autoConnectLocalNodes() else Either.unit
        } yield writePortsFile()
        // log results
        startup
          .bimap(
            error => logger.error(s"Failed to start ${error.name}: ${error.message}"),
            _ => logger.info("Successfully started all nodes"),
          )
          .discard
        startup
      }

    }

  private def writePortsFile()(implicit
      traceContext: TraceContext
  ): Unit = {
    final case class ParticipantApis(ledgerApi: Int, adminApi: Int)
    config.parameters.portsFile.foreach { portsFile =>
      val items = participants.running.map { node =>
        (
          node.name.unwrap,
          ParticipantApis(node.config.ledgerApi.port.unwrap, node.config.adminApi.port.unwrap),
        )
      }.toMap
      import io.circe.syntax.*
      implicit val encoder: Encoder[ParticipantApis] =
        Encoder.forProduct2("ledgerApi", "adminApi") { apis =>
          (apis.ledgerApi, apis.adminApi)
        }
      val out = items.asJson.spaces2
      try {
        better.files.File(portsFile).overwrite(out)
      } catch {
        case NonFatal(ex) =>
          logger.warn(s"Failed to write to port file $portsFile. Will ignore the error", ex)
      }
    }
  }

  private def reconnectParticipants(implicit
      traceContext: TraceContext
  ): Either[StartupError, Unit] = {
    def reconnect(
        instance: ParticipantNodeBootstrap
    ): EitherT[Future, StartupError, Unit] =
      instance.getNode match {
        case None =>
          // should not happen, but if it does, display at least a warning.
          if (instance.config.init.autoInit) {
            logger.error(
              s"Auto-initialisation failed or was too slow for ${instance.name}. Will not automatically re-connect to domains."
            )
          }
          EitherT.rightT(())
        case Some(node) =>
          node
            .reconnectSynchronizersIgnoreFailures()
            .leftMap(err => StartFailed(instance.name.unwrap, err.toString))
            .onShutdown(Left(StartFailed(instance.name.unwrap, "aborted due to shutdown")))

      }
    config.parameters.timeouts.processing.unbounded.await("reconnect-participants")(
      MonadUtil
        .parTraverseWithLimit_(config.parameters.getStartupParallelism(numThreads))(
          participants.running
        )(reconnect)
        .value
    )
  }

  /** Return current time of environment
    */
  def now: CantonTimestamp = clock.now

  private def allNodesWithGroup =
    allNodes.flatMap { nodeGroup =>
      nodeGroup.names().map(name => (name, nodeGroup))
    }

  /** Start all instances described in the configuration
    */
  def startAll()(implicit traceContext: TraceContext): Either[StartupError, Unit] =
    startNodes(allNodesWithGroup)

  def stopAll()(implicit traceContext: TraceContext): Either[ShutdownError, Unit] =
    stopNodes(allNodesWithGroup)

  def startNodes(
      nodes: Seq[(String, Nodes[CantonNode, CantonNodeBootstrap[CantonNode]])]
  )(implicit traceContext: TraceContext): Either[StartupError, Unit] =
    runOnNodesOrderedByStartupGroup(
      "startup-of-all-nodes",
      nodes,
      { case (name, nodes) => nodes.start(name) },
      reverse = false,
    )

  def stopNodes(
      nodes: Seq[(String, Nodes[CantonNode, CantonNodeBootstrap[CantonNode]])]
  )(implicit traceContext: TraceContext): Either[ShutdownError, Unit] =
    runOnNodesOrderedByStartupGroup(
      "stop-of-all-nodes",
      nodes,
      { case (name, nodes) => nodes.stop(name) },
      reverse = true,
    )

  /** run some task on nodes ordered by their startup group
    *
    * @param reverse if true, then the order will be reverted (e.g. for stop)
    */
  private def runOnNodesOrderedByStartupGroup[T, I](
      name: String,
      nodes: Seq[(String, Nodes[CantonNode, CantonNodeBootstrap[CantonNode]])],
      task: (String, Nodes[CantonNode, CantonNodeBootstrap[CantonNode]]) => EitherT[Future, T, I],
      reverse: Boolean,
  )(implicit traceContext: TraceContext): Either[T, Unit] =
    config.parameters.timeouts.processing.unbounded.await(name)(
      MonadUtil
        .sequentialTraverse_(
          nodes
            // parallelize startup by groups (mediator / topology manager need the sequencer to run when we startup)
            // as otherwise, they start to emit a few warnings which are ugly
            .groupBy { case (_, group) => group.startUpGroup }
            .toList
            .sortBy { case (group, _) => if (reverse) -group else group }
        ) { case (_, namesWithGroup) =>
          MonadUtil
            .parTraverseWithLimit_(config.parameters.getStartupParallelism(numThreads))(
              namesWithGroup.sortBy { case (name, _) =>
                name // sort by name to make the invocation order deterministic, hence also the result
              }
            ) { case (name, nodes) => task(name, nodes) }
        }
        .value
    )

  protected def createSequencer(
      name: String,
      sequencerConfig: Config#SequencerNodeConfigType,
  ): SequencerNodeBootstrap

  protected def createMediator(
      name: String,
      mediatorConfig: Config#MediatorNodeConfigType,
  ): MediatorNodeBootstrap

  protected def createParticipant(
      name: String,
      participantConfig: Config#ParticipantConfigType,
  ): ParticipantNodeBootstrap =
    participantNodeFactory
      .create(
        NodeFactoryArguments(
          name,
          participantConfig,
          // this is okay for x-nodes, as we've merged the two parameter sequences
          config.participantNodeParametersByString(name),
          createClock(Some(ParticipantNodeBootstrap.LoggerFactoryKeyName -> name)),
          metricsRegistry.forParticipant(name),
          testingConfig,
          futureSupervisor,
          loggerFactory.append(ParticipantNodeBootstrap.LoggerFactoryKeyName, name),
          writeHealthDumpToFile,
          configuredOpenTelemetry,
          executionContext,
        ),
        testingTimeService,
      )
      .valueOr(err => throw new RuntimeException(s"Failed to create participant bootstrap: $err"))

  protected def mediatorNodeFactoryArguments(
      name: String,
      mediatorConfig: Config#MediatorNodeConfigType,
  ): NodeFactoryArguments[
    Config#MediatorNodeConfigType,
    MediatorNodeParameters,
    MediatorMetrics,
  ] = NodeFactoryArguments(
    name,
    mediatorConfig,
    config.mediatorNodeParametersByString(name),
    createClock(Some(MediatorNodeBootstrap.LoggerFactoryKeyName -> name)),
    metricsRegistry.forMediator(name),
    testingConfig,
    futureSupervisor,
    loggerFactory.append(MediatorNodeBootstrap.LoggerFactoryKeyName, name),
    writeHealthDumpToFile,
    configuredOpenTelemetry,
    executionContext,
  )

  private def simClocks: Seq[SimClock] = {
    val clocks = clock +: (participants.running.map(_.clock) ++ sequencers.running.map(
      _.clock
    ) ++ mediators.running.map(_.clock))
    val simclocks = clocks.collect { case sc: SimClock => sc }
    if (simclocks.sizeCompare(clocks) < 0)
      logger.warn(s"Found non-sim clocks, testing time service will be broken.")
    simclocks
  }

  def addUserCloseable(closeable: AutoCloseable): Unit = userCloseables.append(closeable)

  override def close(): Unit = blocking(this.synchronized {
    val closeActorSystem: AutoCloseable =
      LifeCycle.toCloseableActorSystem(actorSystem, logger, timeouts)

    val closeExecutionContext: AutoCloseable =
      ExecutorServiceExtensions(executionContext)(logger, timeouts)
    val closeScheduler: AutoCloseable = ExecutorServiceExtensions(scheduler)(logger, timeouts)

    val closeHeadlessHealthAdministration: AutoCloseable =
      () => healthDumpGenerator.get.foreach(_.grpcAdminCommandRunner.close())

    // the allNodes list is ordered in ideal startup order, so reverse to shutdown
    val instances =
      monitorO.toList ++ userCloseables ++ allNodes.reverse :+ metricsRegistry :+ configuredOpenTelemetry :+ clock :+
        closeHeadlessHealthAdministration :+ executionSequencerFactory :+ closeActorSystem :+ closeExecutionContext :+
        closeScheduler
    logger.info("Closing environment...")
    LifeCycle.close((instances.toSeq)*)(logger)
  })
}

object Environment {

  /** Ensure all java.util.logging statements are routed to slf4j instead and can be configured with logback.
    * This should be paired with adding a LevelChangePropagator to the logback configuration to avoid the performance impact
    * of translating all JUL log statements (regardless of whether they are being used).
    * See for more details: https://logback.qos.ch/manual/configuration.html#LevelChangePropagator
    */
  def installJavaUtilLoggingBridge(): Unit =
    if (!SLF4JBridgeHandler.isInstalled) {
      // we want everything going to slf4j so remove any default loggers
      SLF4JBridgeHandler.removeHandlersForRootLogger()
      SLF4JBridgeHandler.install()
    }

}

trait EnvironmentFactory[E <: Environment] {
  def create(
      config: E#Config,
      loggerFactory: NamedLoggerFactory,
      testingConfigInternal: TestingConfigInternal = TestingConfigInternal(),
  ): E
}
