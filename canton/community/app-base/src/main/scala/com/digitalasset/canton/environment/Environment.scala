// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.environment

import cats.data.EitherT
import cats.syntax.either.*
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.concurrent.*
import com.digitalasset.canton.config.*
import com.digitalasset.canton.console.{
  ConsoleEnvironment,
  ConsoleGrpcAdminCommandRunner,
  ConsoleOutput,
  GrpcAdminCommandRunner,
  HealthDumpGenerator,
  StandardConsoleOutput,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.mediator.{MediatorNodeBootstrapX, MediatorNodeParameters}
import com.digitalasset.canton.domain.metrics.MediatorMetrics
import com.digitalasset.canton.domain.sequencing.SequencerNodeBootstrapX
import com.digitalasset.canton.environment.CantonNodeBootstrap.HealthDumpFunction
import com.digitalasset.canton.environment.Environment.*
import com.digitalasset.canton.environment.ParticipantNodes.ParticipantNodesX
import com.digitalasset.canton.lifecycle.Lifecycle
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.MetricsRegistry
import com.digitalasset.canton.participant.*
import com.digitalasset.canton.resource.DbMigrationsFactory
import com.digitalasset.canton.telemetry.{ConfiguredOpenTelemetry, OpenTelemetryFactory}
import com.digitalasset.canton.time.EnrichedDurations.*
import com.digitalasset.canton.time.*
import com.digitalasset.canton.tracing.TraceContext.withNewTraceContext
import com.digitalasset.canton.tracing.{NoTracing, TraceContext, TracerProvider}
import com.digitalasset.canton.util.FutureInstances.parallelFuture
import com.digitalasset.canton.util.{MonadUtil, PekkoUtil, SingleUseCell}
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

  type Config <: CantonConfig
  type Console <: ConsoleEnvironment

  val config: Config
  val testingConfig: TestingConfigInternal

  val loggerFactory: NamedLoggerFactory

  lazy val configuredOpenTelemetry: ConfiguredOpenTelemetry = {
    OpenTelemetryFactory.initializeOpenTelemetry(
      testingConfig.initializeGlobalOpenTelemetry,
      config.monitoring.metrics.reporters.nonEmpty,
      MetricsRegistry
        .registerReporters(config.monitoring.metrics, loggerFactory),
      config.monitoring.tracing.tracer,
      config.monitoring.metrics.histograms,
      loggerFactory,
    )
  }

  // public for buildDocs task to be able to construct a fake participant and domain to document available metrics via reflection

  lazy val metricsRegistry: MetricsRegistry = new MetricsRegistry(
    config.monitoring.metrics.reportJvmMetrics,
    configuredOpenTelemetry.openTelemetry.meterBuilder("canton").build(),
    testingConfig.metricsFactoryType,
  )

  protected def participantNodeFactoryX
      : ParticipantNodeBootstrap.Factory[Config#ParticipantConfigType, ParticipantNodeBootstrapX]
  protected def migrationsFactory: DbMigrationsFactory

  def isEnterprise: Boolean

  def createConsole(
      consoleOutput: ConsoleOutput = StandardConsoleOutput,
      createAdminCommandRunner: ConsoleEnvironment => ConsoleGrpcAdminCommandRunner =
        new ConsoleGrpcAdminCommandRunner(_),
  ): Console = {
    val console = _createConsole(consoleOutput, createAdminCommandRunner)
    healthDumpGenerator
      .putIfAbsent(createHealthDumpGenerator(console.grpcAdminCommandRunner))
      .discard
    console
  }

  protected def _createConsole(
      consoleOutput: ConsoleOutput = StandardConsoleOutput,
      createAdminCommandRunner: ConsoleEnvironment => ConsoleGrpcAdminCommandRunner =
        new ConsoleGrpcAdminCommandRunner(_),
  ): Console

  protected def createHealthDumpGenerator(
      commandRunner: GrpcAdminCommandRunner
  ): HealthDumpGenerator[_]

  /* We can't reliably use the health administration instance of the console because:
   * 1) it's tied to the console environment, which we don't have access to yet when the environment is instantiated
   * 2) there might never be a console environment when running in daemon mode
   * Therefore we create an immutable lazy value for the health administration that can be set either with the console
   * health admin when/if it gets created, or with a headless health admin, whichever comes first.
   */
  private val healthDumpGenerator = new SingleUseCell[HealthDumpGenerator[_]]

  // Function passed down to the node boostrap used to generate a health dump file
  val writeHealthDumpToFile: HealthDumpFunction = () =>
    Future {
      healthDumpGenerator
        .getOrElse {
          val tracerProvider =
            TracerProvider.Factory(configuredOpenTelemetry, "admin_command_runner")
          implicit val tracer: Tracer = tracerProvider.tracer

          val commandRunner = new GrpcAdminCommandRunner(this, config.parameters.timeouts.console)
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
        .generateHealthDump(
          better.files.File.newTemporaryFile(
            prefix = "canton-remote-health-dump"
          )
        )
    }

  installJavaUtilLoggingBridge()
  logger.debug(config.portDescription)

  implicit val scheduler: ScheduledExecutorService =
    Threading.singleThreadScheduledExecutor(
      loggerFactory.threadName + "-env-sched",
      noTracingLogger,
    )

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
    if (config.monitoring.logSlowFutures)
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
        new RemoteClock(
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

  lazy val participantsX =
    new ParticipantNodesX[Config#ParticipantConfigType](
      createParticipantX,
      migrationsFactory,
      timeouts,
      config.participantsByString,
      config.participantNodeParametersByString,
      loggerFactory,
    )

  val sequencersX = new SequencerNodesX(
    createSequencerX,
    migrationsFactory,
    timeouts,
    config.sequencersByString,
    config.sequencerNodeParametersByStringX,
    loggerFactory,
  )

  val mediatorsX =
    new MediatorNodesX(
      createMediatorX,
      migrationsFactory,
      timeouts,
      config.mediatorsByString,
      config.mediatorNodeParametersByStringX,
      loggerFactory,
    )

  // convenient grouping of all node collections for performing operations
  // intentionally defined in the order we'd like to start them
  protected def allNodes: List[Nodes[CantonNode, CantonNodeBootstrap[CantonNode]]] =
    List(sequencersX, mediatorsX, participantsX)
  private def runningNodes: Seq[CantonNodeBootstrap[CantonNode]] = allNodes.flatMap(_.running)

  private def autoConnectLocalNodes(): Either[StartupError, Unit] = {
    // TODO(#14048) extend this to x-nodes
    Left(StartFailed("participants", "auto connect local nodes not yet implemented"))
  }

  /** Try to startup all nodes in the configured environment and reconnect them to one another.
    * The first error will prevent further nodes from being started.
    * If an error is returned previously started nodes will not be stopped.
    */
  def startAndReconnect(autoConnectLocal: Boolean): Either[StartupError, Unit] =
    withNewTraceContext { implicit traceContext =>
      if (config.parameters.manualStart) {
        logger.info("Manual start requested.")
        Right(())
      } else {
        logger.info("Automatically starting all instances")
        val startup = for {
          _ <- startAll()
          _ <- reconnectParticipants
          _ <- if (autoConnectLocal) autoConnectLocalNodes() else Right(())
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
      val items = participantsX.running.map { node =>
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
          logger.warn(s"Failed to write to port file ${portsFile}. Will ignore the error", ex)
      }
    }
  }

  private def reconnectParticipants(implicit
      traceContext: TraceContext
  ): Either[StartupError, Unit] = {
    def reconnect(
        instance: CantonNodeBootstrap[ParticipantNodeCommon] & ParticipantNodeBootstrapCommon
    ): EitherT[Future, StartupError, Unit] = {
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
            .reconnectDomainsIgnoreFailures()
            .leftMap(err => StartFailed(instance.name.unwrap, err.toString))
            .onShutdown(Left(StartFailed(instance.name.unwrap, "aborted due to shutdown")))

      }
    }
    config.parameters.timeouts.processing.unbounded.await("reconnect-particiapnts")(
      MonadUtil
        .parTraverseWithLimit_(config.parameters.getStartupParallelism(numThreads))(
          participantsX.running
        )(reconnect)
        .value
    )
  }

  /** Return current time of environment
    */
  def now: CantonTimestamp = clock.now

  private def allNodesWithGroup = {
    allNodes.flatMap { nodeGroup =>
      nodeGroup.names().map(name => (name, nodeGroup))
    }
  }

  /** Start all instances described in the configuration
    */
  def startAll()(implicit traceContext: TraceContext): Either[StartupError, Unit] =
    startNodes(allNodesWithGroup)

  def stopAll()(implicit traceContext: TraceContext): Either[ShutdownError, Unit] =
    stopNodes(allNodesWithGroup)

  def startNodes(
      nodes: Seq[(String, Nodes[CantonNode, CantonNodeBootstrap[CantonNode]])]
  )(implicit traceContext: TraceContext): Either[StartupError, Unit] = {
    runOnNodesOrderedByStartupGroup(
      "startup-of-all-nodes",
      nodes,
      { case (name, nodes) => nodes.start(name) },
      reverse = false,
    )
  }

  def stopNodes(
      nodes: Seq[(String, Nodes[CantonNode, CantonNodeBootstrap[CantonNode]])]
  )(implicit traceContext: TraceContext): Either[ShutdownError, Unit] = {
    runOnNodesOrderedByStartupGroup(
      "stop-of-all-nodes",
      nodes,
      { case (name, nodes) => nodes.stop(name) },
      reverse = true,
    )
  }

  /** run some task on nodes ordered by their startup group
    *
    * @param reverse if true, then the order will be reverted (e.g. for stop)
    */
  private def runOnNodesOrderedByStartupGroup[T, I](
      name: String,
      nodes: Seq[(String, Nodes[CantonNode, CantonNodeBootstrap[CantonNode]])],
      task: (String, Nodes[CantonNode, CantonNodeBootstrap[CantonNode]]) => EitherT[Future, T, I],
      reverse: Boolean,
  )(implicit traceContext: TraceContext): Either[T, Unit] = {
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
  }

  protected def createSequencerX(
      name: String,
      sequencerConfig: Config#SequencerNodeXConfigType,
  ): SequencerNodeBootstrapX

  protected def createMediatorX(
      name: String,
      mediatorConfig: Config#MediatorNodeXConfigType,
  ): MediatorNodeBootstrapX

  protected def createParticipantX(
      name: String,
      participantConfig: Config#ParticipantConfigType,
  ): ParticipantNodeBootstrapX = {
    participantNodeFactoryX
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
        ),
        testingTimeService,
      )
      .valueOr(err => throw new RuntimeException(s"Failed to create participant bootstrap: $err"))
  }

  protected def mediatorNodeFactoryArguments(
      name: String,
      mediatorConfig: Config#MediatorNodeXConfigType,
  ): NodeFactoryArguments[
    Config#MediatorNodeXConfigType,
    MediatorNodeParameters,
    MediatorMetrics,
  ] = NodeFactoryArguments(
    name,
    mediatorConfig,
    config.mediatorNodeParametersByStringX(name),
    createClock(Some(MediatorNodeBootstrapX.LoggerFactoryKeyName -> name)),
    metricsRegistry.forMediator(name),
    testingConfig,
    futureSupervisor,
    loggerFactory.append(MediatorNodeBootstrapX.LoggerFactoryKeyName, name),
    writeHealthDumpToFile,
    configuredOpenTelemetry,
  )

  private def simClocks: Seq[SimClock] = {
    val clocks = clock +: (participantsX.running.map(_.clock) ++ sequencersX.running.map(
      _.clock
    ) ++ mediatorsX.running.map(_.clock))
    val simclocks = clocks.collect { case sc: SimClock => sc }
    if (simclocks.sizeCompare(clocks) < 0)
      logger.warn(s"Found non-sim clocks, testing time service will be broken.")
    simclocks
  }

  def addUserCloseable(closeable: AutoCloseable): Unit = userCloseables.append(closeable)

  override def close(): Unit = blocking(this.synchronized {
    val closeActorSystem: AutoCloseable =
      Lifecycle.toCloseableActorSystem(actorSystem, logger, timeouts)

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
    Lifecycle.close((instances.toSeq): _*)(logger)
  })
}

object Environment {

  /** Ensure all java.util.logging statements are routed to slf4j instead and can be configured with logback.
    * This should be paired with adding a LevelChangePropagator to the logback configuration to avoid the performance impact
    * of translating all JUL log statements (regardless of whether they are being used).
    * See for more details: https://logback.qos.ch/manual/configuration.html#LevelChangePropagator
    */
  def installJavaUtilLoggingBridge(): Unit = {
    if (!SLF4JBridgeHandler.isInstalled) {
      // we want everything going to slf4j so remove any default loggers
      SLF4JBridgeHandler.removeHandlersForRootLogger()
      SLF4JBridgeHandler.install()
    }
  }

}

trait EnvironmentFactory[E <: Environment] {
  def create(
      config: E#Config,
      loggerFactory: NamedLoggerFactory,
      testingConfigInternal: TestingConfigInternal = TestingConfigInternal(),
  ): E
}
