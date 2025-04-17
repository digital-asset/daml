// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.environment

import cats.data.EitherT
import cats.instances.future.*
import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.{Applicative, Id}
import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.config.{DbConfig, LocalNodeConfig, ProcessingTimeout, StorageConfig}
import com.digitalasset.canton.console.GrpcAdminCommandRunner
import com.digitalasset.canton.console.declarative.{DeclarativeApiHandle, DeclarativeApiManager}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.*
import com.digitalasset.canton.participant.config.ParticipantNodeConfig
import com.digitalasset.canton.resource.DbStorage.RetryConfig
import com.digitalasset.canton.resource.{DbMigrations, DbMigrationsFactory}
import com.digitalasset.canton.synchronizer.mediator.{
  MediatorNode,
  MediatorNodeBootstrap,
  MediatorNodeConfig,
  MediatorNodeParameters,
}
import com.digitalasset.canton.synchronizer.sequencer.config.{
  SequencerNodeConfig,
  SequencerNodeParameters,
}
import com.digitalasset.canton.synchronizer.sequencer.{SequencerNode, SequencerNodeBootstrap}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Thereafter.syntax.*

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future, Promise, blocking}

/** Group of CantonNodes of the same type (mediators, participants, sequencers). */
trait Nodes[+Node <: CantonNode, +NodeBootstrap <: CantonNodeBootstrap[Node]]
    extends FlagCloseable {

  type InstanceName = String

  /** Returns the startup group (nodes in the same group will start together)
    *
    * Mediators automatically connect to a synchronizer. Participants require an external call to
    * reconnectSynchronizers. Therefore, we can start participant and sequencer nodes together, but
    * we have to wait for the sequencers to be up before we can kick off mediators.
    */
  def startUpGroup: Int

  /** Returns the names of all known nodes */
  def names(): Seq[InstanceName]

  /** Start an individual node by name */
  def start(name: InstanceName)(implicit
      traceContext: TraceContext
  ): EitherT[Future, StartupError, Unit]

  def startAndWait(name: InstanceName)(implicit
      traceContext: TraceContext
  ): Either[StartupError, Unit]

  /** Is the named node running? */
  def isRunning(name: InstanceName): Boolean

  /** Get the single running node */
  def getRunning(name: InstanceName): Option[NodeBootstrap]

  /** Get the node while it is still being started. This is mostly useful during testing to access
    * the node in earlier stages of its initialization phase.
    */
  def getStarting(name: InstanceName): Option[NodeBootstrap]

  /** Stop the named node */
  def stop(name: InstanceName)(implicit
      traceContext: TraceContext
  ): EitherT[Future, ShutdownError, Unit]

  def stopAndWait(name: InstanceName)(implicit
      traceContext: TraceContext
  ): Either[ShutdownError, Unit]

  /** Get nodes that are currently running */
  def running: Seq[NodeBootstrap]

  /** Independently run any pending database migrations for the named node */
  def migrateDatabase(name: InstanceName): Either[StartupError, Unit]

  /** Independently repair the Flyway schema history table for the named node to reset Flyway
    * migration checksums etc
    */
  def repairDatabaseMigration(name: InstanceName): Either[StartupError, Unit]
}

private sealed trait ManagedNodeStage[T]

private final case class PreparingDatabase[T](
    promise: Promise[Either[StartupError, T]]
) extends ManagedNodeStage[T]

private final case class StartingUp[T](
    promise: Promise[Either[StartupError, T]],
    node: T,
) extends ManagedNodeStage[T]

private final case class Running[T, C](node: T, declarativeHandle: Option[DeclarativeApiHandle[C]])
    extends ManagedNodeStage[T]

/** Nodes group that can start nodes with the provided configuration and factory */
class ManagedNodes[
    Node <: CantonNode,
    NodeConfig <: LocalNodeConfig,
    NodeParameters <: CantonNodeParameters,
    NodeBootstrap <: CantonNodeBootstrap[Node],
](
    create: (String, NodeConfig) => NodeBootstrap,
    migrationsFactory: DbMigrationsFactory,
    override protected val timeouts: ProcessingTimeout,
    configs: => Map[String, NodeConfig],
    parametersFor: String => CantonNodeParameters,
    override val startUpGroup: Int,
    protected val loggerFactory: NamedLoggerFactory,
    protected val declarativeManager: Option[DeclarativeApiManager[NodeConfig]] = None,
)(implicit ec: ExecutionContext)
    extends Nodes[Node, NodeBootstrap]
    with NamedLogging
    with HasCloseContext
    with FlagCloseableAsync {

  private val nodes = TrieMap[InstanceName, ManagedNodeStage[NodeBootstrap]]()
  override def names(): Seq[InstanceName] = configs.keys.toSeq

  override def running: Seq[NodeBootstrap] = nodes.values.toSeq.collect { case Running(node, _) =>
    node
  }
  def startAndWait(name: InstanceName)(implicit
      traceContext: TraceContext
  ): Either[StartupError, Unit] =
    timeouts.unbounded.await(s"Starting node $name")(start(name).value)

  override def start(
      name: InstanceName
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, StartupError, Unit] =
    EitherT
      .fromEither[Future](
        configs
          .get(name)
          .toRight(ConfigurationNotFound(name): StartupError)
      )
      .flatMap(startNode(name, _).map(_ => ()))

  def pokeDeclarativeApis(
      newConfig: Either[Unit, Boolean]
  )(implicit traceContext: TraceContext): Unit = newConfig match {
    case Right(true) =>
      val currentConfig = configs
      nodes.foreach {
        case (name, Running(_, Some(handle))) =>
          currentConfig
            .get(name)
            .foreach { config =>
              handle.newConfig(config)
            }
        case _ =>
      }
    case Right(false) =>
      nodes.foreach {
        case (_, Running(_, Some(handle))) =>
          handle.poke()
        case _ =>
      }
    case Left(()) =>
      nodes.foreach {
        case (_, Running(_, Some(handle))) =>
          handle.invalidConfig()
        case _ =>
      }
  }

  private def startDeclarativeApi(
      instance: NodeBootstrap,
      name: InstanceName,
      config: NodeConfig,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, Option[DeclarativeApiHandle[NodeConfig]]] =
    declarativeManager
      .map { manager =>
        manager
          .started(
            name,
            config,
            instance,
          )
          .map(r => Some(r): Option[DeclarativeApiHandle[NodeConfig]])
      }
      .getOrElse(EitherT.rightT(None))

  private def startNode(
      name: InstanceName,
      config: NodeConfig,
  )(implicit traceContext: TraceContext): EitherT[Future, StartupError, NodeBootstrap] = if (
    isClosing
  )
    EitherT.leftT(ShutdownDuringStartup(name, "Won't start during shutdown"))
  else {
    // Does not run concurrently with itself as ensured by putIfAbsent below
    def runStartup(
        promise: Promise[Either[StartupError, NodeBootstrap]]
    ): EitherT[Future, StartupError, NodeBootstrap] = {
      val params = parametersFor(name)
      val startup = for {
        // start migration
        _ <- EitherT(Future(checkMigration(name, config.storage, params)))
        instance = {
          val instance = create(name, config)
          nodes.put(name, StartingUp(promise, instance)).discard
          instance
        }
        declarativeHandler <-
          instance
            .start()
            .flatMap(_ => startDeclarativeApi(instance, name, config))
            .leftMap { error =>
              instance.close() // clean up resources allocated during instance creation (e.g., db)
              StartFailed(name, error): StartupError
            }
      } yield {
        // register the running instance
        nodes.put(name, Running(instance, declarativeHandler)).discard
        instance
      }
      // remove node upon failure
      val withCleanup = startup.thereafterSuccessOrFailure(_ => (), nodes.remove(name).discard)
      promise.completeWith(withCleanup.value)
      withCleanup
    }

    val promise = Promise[Either[StartupError, NodeBootstrap]]()
    nodes.putIfAbsent(name, PreparingDatabase(promise)) match {
      case None => runStartup(promise) // startup will run async
      case Some(PreparingDatabase(promise)) => EitherT(promise.future)
      case Some(StartingUp(promise, _)) => EitherT(promise.future)
      case Some(Running(node, _)) => EitherT.rightT(node)
    }
  }

  private def configAndParams(
      name: InstanceName
  ): Either[StartupError, (NodeConfig, CantonNodeParameters)] =
    for {
      config <- configs.get(name).toRight(ConfigurationNotFound(name): StartupError)
      _ <- checkNotRunning(name)
      params = parametersFor(name)
    } yield (config, params)

  override def migrateDatabase(name: InstanceName): Either[StartupError, Unit] = blocking(
    synchronized {
      for {
        cAndP <- configAndParams(name)
        (config, params) = cAndP
        _ <- runMigration(name, config.storage, params.alphaVersionSupport)
      } yield ()
    }
  )

  override def repairDatabaseMigration(name: InstanceName): Either[StartupError, Unit] = blocking(
    synchronized {
      for {
        cAndP <- configAndParams(name)
        (config, params) = cAndP
        _ <- runRepairMigration(name, config.storage, params.alphaVersionSupport)
      } yield ()
    }
  )

  override def isRunning(name: InstanceName): Boolean = nodes.contains(name)

  override def getRunning(name: InstanceName): Option[NodeBootstrap] = nodes.get(name).collect {
    case Running(node, _) => node
  }

  override def getStarting(name: InstanceName): Option[NodeBootstrap] = nodes.get(name).collect {
    case StartingUp(_, node) => node
  }

  override def stop(
      name: InstanceName
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, ShutdownError, Unit] = {
    val node = nodes.get(name)
    for {
      _ <-
        // if node is not running and config does not exist, indicate that something is wrong
        if (node.isEmpty) {
          EitherT.fromEither[Future](
            configs.get(name).toRight[ShutdownError](ConfigurationNotFound(name))
          )
        } else {
          // we don't check this if the node is running as if someone actually changed the config and
          // removed the node, we still want to be able to shut it down
          EitherT.rightT(())
        }
      _ <- nodes.get(name).traverse_(stopStage(name))
    } yield ()
  }

  override def stopAndWait(name: InstanceName)(implicit
      traceContext: TraceContext
  ): Either[ShutdownError, Unit] =
    timeouts.unbounded.await(s"stopping node $name")(stop(name).value)

  private def stopStage(name: InstanceName)(
      stage: ManagedNodeStage[NodeBootstrap]
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[Future, ShutdownError, Unit] =
    EitherT(stage match {
      // wait for the node to complete startup
      case PreparingDatabase(promise) => promise.future
      case StartingUp(promise, _) => promise.future
      case Running(node, _) => Future.successful(Right(node))
    }).transform {
      case Left(_) =>
        // we can remap a startup failure to a success here, as we don't want the
        // startup failure to propagate into a shutdown failure
        Either.unit
      case Right(node) =>
        nodes.remove(name).foreach {
          // if there were other processes messing with the node, we won't shut down
          case Running(current, _) if node == current =>
            LifeCycle.close(node)(logger)
          case _ =>
            logger.info(s"Node $name has already disappeared.")
        }
        Either.unit
    }

  override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = {
    val runningInstances = nodes.toList
    import TraceContext.Implicits.Empty.*
    runningInstances.map { case (name, stage) =>
      AsyncCloseable(s"node-$name", stopStage(name)(stage).value, timeouts.closing)
    }
  }

  private def runIfUsingDatabase[F[_]](storageConfig: StorageConfig)(
      fn: DbConfig => F[Either[StartupError, Unit]]
  )(implicit F: Applicative[F]): F[Either[StartupError, Unit]] = storageConfig match {
    case dbConfig: DbConfig => fn(dbConfig)
    case _ => F.pure(Either.unit)
  }

  // if database is fresh, we will migrate it. Otherwise, we will check if there is any pending migrations,
  // which need to be triggered manually.
  private def checkMigration(
      name: InstanceName,
      storageConfig: StorageConfig,
      params: CantonNodeParameters,
  ): Either[StartupError, Unit] =
    runIfUsingDatabase[Id](storageConfig) { dbConfig =>
      val migrations = migrationsFactory.create(dbConfig, name, params.alphaVersionSupport)
      import TraceContext.Implicits.Empty.*
      logger.info(s"Setting up database schemas for $name")

      def errorMapping(err: DbMigrations.Error): StartupError =
        err match {
          case DbMigrations.PendingMigrationError(msg) => PendingDatabaseMigration(name, msg)
          case err: DbMigrations.FlywayError => FailedDatabaseMigration(name, err)
          case err: DbMigrations.DatabaseError => FailedDatabaseMigration(name, err)
          case err: DbMigrations.DatabaseVersionError => FailedDatabaseVersionChecks(name, err)
          case err: DbMigrations.DatabaseConfigError => FailedDatabaseConfigChecks(name, err)
        }
      val retryConfig =
        if (storageConfig.parameters.failFastOnStartup) RetryConfig.failFast
        else RetryConfig.forever

      val result = migrations
        .checkAndMigrate(params, retryConfig)
        .leftMap(errorMapping)

      result.value.onShutdown(
        Left(ShutdownDuringStartup(name, "DB migration check interrupted due to shutdown"))
      )
    }

  private def checkNotRunning(name: InstanceName): Either[StartupError, Unit] =
    Either.cond(!isRunning(name), (), AlreadyRunning(name))

  private def runMigration(
      name: InstanceName,
      storageConfig: StorageConfig,
      alphaVersionSupport: Boolean,
  ): Either[StartupError, Unit] =
    runIfUsingDatabase[Id](storageConfig) { dbConfig =>
      migrationsFactory
        .create(dbConfig, name, alphaVersionSupport)
        .migrateDatabase()
        .leftMap(FailedDatabaseMigration(name, _))
        .value
        .onShutdown(Left(ShutdownDuringStartup(name, "DB migration interrupted due to shutdown")))
    }

  private def runRepairMigration(
      name: InstanceName,
      storageConfig: StorageConfig,
      alphaVersionSupport: Boolean,
  ): Either[StartupError, Unit] =
    runIfUsingDatabase[Id](storageConfig) { dbConfig =>
      migrationsFactory
        .create(dbConfig, name, alphaVersionSupport)
        .repairFlywayMigration()
        .leftMap(FailedDatabaseRepairMigration(name, _))
        .value
        .onShutdown(
          Left(ShutdownDuringStartup(name, "DB repair migration interrupted due to shutdown"))
        )
    }
}

class ParticipantNodes[B <: CantonNodeBootstrap[N], N <: CantonNode](
    create: (String, ParticipantNodeConfig) => B, // (nodeName, config) => bootstrap
    migrationsFactory: DbMigrationsFactory,
    timeouts: ProcessingTimeout,
    configs: => Map[String, ParticipantNodeConfig],
    parametersFor: String => ParticipantNodeParameters,
    runnerFactory: String => GrpcAdminCommandRunner,
    enableAlphaStateViaConfig: Boolean,
    loggerFactory: NamedLoggerFactory,
)(implicit
    protected val executionContext: ExecutionContextIdlenessExecutorService
) extends ManagedNodes[N, ParticipantNodeConfig, ParticipantNodeParameters, B](
      create,
      migrationsFactory,
      timeouts,
      configs,
      parametersFor,
      startUpGroup = 2,
      loggerFactory,
      Option.when(enableAlphaStateViaConfig)(
        DeclarativeApiManager
          .forParticipants(runnerFactory, timeouts.dynamicStateConsistencyTimeout, loggerFactory)
      ),
    ) {}

class SequencerNodes(
    create: (String, SequencerNodeConfig) => SequencerNodeBootstrap,
    migrationsFactory: DbMigrationsFactory,
    timeouts: ProcessingTimeout,
    configs: => Map[String, SequencerNodeConfig],
    parameters: String => SequencerNodeParameters,
    loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends ManagedNodes[
      SequencerNode,
      SequencerNodeConfig,
      SequencerNodeParameters,
      SequencerNodeBootstrap,
    ](
      create,
      migrationsFactory,
      timeouts,
      configs,
      parameters,
      startUpGroup = 0,
      loggerFactory,
    )

class MediatorNodes(
    create: (String, MediatorNodeConfig) => MediatorNodeBootstrap,
    migrationsFactory: DbMigrationsFactory,
    timeouts: ProcessingTimeout,
    configs: => Map[String, MediatorNodeConfig],
    parameters: String => MediatorNodeParameters,
    loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends ManagedNodes[
      MediatorNode,
      MediatorNodeConfig,
      MediatorNodeParameters,
      MediatorNodeBootstrap,
    ](
      create,
      migrationsFactory,
      timeouts,
      configs,
      parameters,
      startUpGroup = 1,
      loggerFactory,
    )
