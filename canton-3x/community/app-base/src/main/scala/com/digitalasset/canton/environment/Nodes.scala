// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.environment

import cats.data.EitherT
import cats.instances.future.*
import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.{Applicative, Id}
import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.config.{DbConfig, LocalNodeConfig, ProcessingTimeout, StorageConfig}
import com.digitalasset.canton.domain.config.DomainConfig
import com.digitalasset.canton.domain.mediator.{
  MediatorNodeBootstrapX,
  MediatorNodeConfigCommon,
  MediatorNodeParameters,
  MediatorNodeX,
}
import com.digitalasset.canton.domain.{Domain, DomainNodeBootstrap, DomainNodeParameters}
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.*
import com.digitalasset.canton.participant.config.LocalParticipantConfig
import com.digitalasset.canton.participant.ledger.api.CantonLedgerApiServerWrapper
import com.digitalasset.canton.participant.ledger.api.CantonLedgerApiServerWrapper.MigrateSchemaConfig
import com.digitalasset.canton.resource.DbStorage.RetryConfig
import com.digitalasset.canton.resource.{DbMigrations, DbMigrationsFactory}
import com.digitalasset.canton.tracing.TraceContext

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future, Promise, blocking}
import scala.util.{Failure, Success}

/** Group of CantonNodes of the same type (domains, participants, sequencers). */
trait Nodes[+Node <: CantonNode, +NodeBootstrap <: CantonNodeBootstrap[Node]]
    extends FlagCloseable {

  type InstanceName = String

  /** Returns the startup group (nodes in the same group will start together)
    *
    * Mediator & Topology manager automatically connect to a domain. Participants
    * require an external call to reconnectDomains. Therefore, we can start participants, sequencer and domain
    * nodes together, but we have to wait for the sequencers to be up before we can kick off mediators & topology managers.
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

  /** Get the node while it is still being started. This is mostly useful during testing to access the node in earlier
    * stages of its initialization phase.
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

  /** Independently repair the Flyway schema history table for the named node to reset Flyway migration checksums etc */
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

private final case class Running[T](node: T) extends ManagedNodeStage[T]

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
    configs: Map[String, NodeConfig],
    parametersFor: String => CantonNodeParameters,
    override val startUpGroup: Int,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends Nodes[Node, NodeBootstrap]
    with NamedLogging
    with HasCloseContext
    with FlagCloseableAsync {

  private val nodes = TrieMap[InstanceName, ManagedNodeStage[NodeBootstrap]]()
  override lazy val names: Seq[InstanceName] = configs.keys.toSeq

  override def running: Seq[NodeBootstrap] = nodes.values.toSeq.collect { case Running(node) =>
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

  private def startNode(
      name: InstanceName,
      config: NodeConfig,
  ): EitherT[Future, StartupError, NodeBootstrap] = if (isClosing)
    EitherT.leftT(ShutdownDuringStartup(name, "Won't start during shutdown"))
  else {
    def runStartup(
        promise: Promise[Either[StartupError, NodeBootstrap]]
    ): EitherT[Future, StartupError, NodeBootstrap] = {
      val params = parametersFor(name)
      val startup = for {
        // start migration
        _ <- EitherT(Future { checkMigration(name, config.storage, params) })
        instance = {
          val instance = create(name, config)
          nodes.put(name, StartingUp(promise, instance)).discard
          instance
        }
        _ <-
          instance.start().leftMap { error =>
            instance.close() // clean up resources allocated during instance creation (e.g., db)
            StartFailed(name, error): StartupError
          }
      } yield {
        // register the running instance
        nodes.put(name, Running(instance)).discard
        instance
      }
      import com.digitalasset.canton.util.Thereafter.syntax.*
      promise.completeWith(startup.value)
      // remove node upon failure
      startup.thereafter {
        case Success(Right(_)) => ()
        case Success(Left(_)) =>
          nodes.remove(name).discard
        case Failure(_) =>
          nodes.remove(name).discard
      }
    }

    blocking(synchronized {
      nodes.get(name) match {
        case Some(PreparingDatabase(promise)) => EitherT(promise.future)
        case Some(StartingUp(promise, _)) => EitherT(promise.future)
        case Some(Running(node)) => EitherT.rightT(node)
        case None =>
          val promise = Promise[Either[StartupError, NodeBootstrap]]()
          nodes
            .put(name, PreparingDatabase(promise))
            .discard // discard is okay as this is running in the sync block
          runStartup(promise) // startup will run async
      }
    })

  }

  private def configAndParams(
      name: InstanceName
  ): Either[StartupError, (NodeConfig, CantonNodeParameters)] = {
    for {
      config <- configs.get(name).toRight(ConfigurationNotFound(name): StartupError)
      _ <- checkNotRunning(name)
      params = parametersFor(name)
    } yield (config, params)
  }

  override def migrateDatabase(name: InstanceName): Either[StartupError, Unit] = blocking(
    synchronized {
      for {
        cAndP <- configAndParams(name)
        (config, params) = cAndP
        _ <- runMigration(name, config.storage, params.devVersionSupport)
      } yield ()
    }
  )

  override def repairDatabaseMigration(name: InstanceName): Either[StartupError, Unit] = blocking(
    synchronized {
      for {
        cAndP <- configAndParams(name)
        (config, params) = cAndP
        _ <- runRepairMigration(name, config.storage, params.devVersionSupport)
      } yield ()
    }
  )

  override def isRunning(name: InstanceName): Boolean = nodes.contains(name)

  override def getRunning(name: InstanceName): Option[NodeBootstrap] = nodes.get(name).collect {
    case Running(node) => node
  }

  override def getStarting(name: InstanceName): Option[NodeBootstrap] = nodes.get(name).collect {
    case StartingUp(_, node) => node
  }

  override def stop(
      name: InstanceName
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, ShutdownError, Unit] =
    for {
      _ <- EitherT.fromEither[Future](
        configs.get(name).toRight[ShutdownError](ConfigurationNotFound(name))
      )
      _ <- nodes.get(name).traverse_(stopStage(name))
    } yield ()

  override def stopAndWait(name: InstanceName)(implicit
      traceContext: TraceContext
  ): Either[ShutdownError, Unit] =
    timeouts.unbounded.await(s"stopping node $name")(stop(name).value)

  private def stopStage(name: InstanceName)(
      stage: ManagedNodeStage[NodeBootstrap]
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[Future, ShutdownError, Unit] = {
    EitherT(stage match {
      // wait for the node to complete startup
      case PreparingDatabase(promise) => promise.future
      case StartingUp(promise, _) => promise.future
      case Running(node) => Future.successful(Right(node))
    }).transform {
      case Left(_) =>
        // we can remap a startup failure to a success here, as we don't want the
        // startup failure to propagate into a shutdown failure
        Right(())
      case Right(node) =>
        nodes.remove(name).foreach {
          // if there were other processes messing with the node, we won't shutdown
          case Running(current) if node == current =>
            Lifecycle.close(node)(logger)
          case _ =>
            logger.info(s"Node $name has already disappeared.")
        }
        Right(())
    }
  }

  override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = {
    val runningInstances = nodes.toList
    import TraceContext.Implicits.Empty.*
    runningInstances.map { case (name, stage) =>
      AsyncCloseable(s"node-$name", stopStage(name)(stage).value, timeouts.closing)
    }
  }

  protected def runIfUsingDatabase[F[_]](storageConfig: StorageConfig)(
      fn: DbConfig => F[Either[StartupError, Unit]]
  )(implicit F: Applicative[F]): F[Either[StartupError, Unit]] = storageConfig match {
    case dbConfig: DbConfig => fn(dbConfig)
    case _ => F.pure(Right(()))
  }

  // if database is fresh, we will migrate it. Otherwise, we will check if there is any pending migrations,
  // which need to be triggered manually.
  private def checkMigration(
      name: InstanceName,
      storageConfig: StorageConfig,
      params: CantonNodeParameters,
  ): Either[StartupError, Unit] =
    runIfUsingDatabase[Id](storageConfig) { dbConfig =>
      val migrations = migrationsFactory.create(dbConfig, name, params.devVersionSupport)
      import TraceContext.Implicits.Empty.*
      logger.info(s"Setting up database schemas for $name")

      def errorMapping(err: DbMigrations.Error): StartupError = {
        err match {
          case DbMigrations.PendingMigrationError(msg) => PendingDatabaseMigration(name, msg)
          case err: DbMigrations.FlywayError => FailedDatabaseMigration(name, err)
          case err: DbMigrations.DatabaseError => FailedDatabaseMigration(name, err)
          case err: DbMigrations.DatabaseVersionError => FailedDatabaseVersionChecks(name, err)
          case err: DbMigrations.DatabaseConfigError => FailedDatabaseConfigChecks(name, err)
        }
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
    if (isRunning(name)) Left(AlreadyRunning(name))
    else Right(())

  private def runMigration(
      name: InstanceName,
      storageConfig: StorageConfig,
      devVersionSupport: Boolean,
  ): Either[StartupError, Unit] =
    runIfUsingDatabase[Id](storageConfig) { dbConfig =>
      migrationsFactory
        .create(dbConfig, name, devVersionSupport)
        .migrateDatabase()
        .leftMap(FailedDatabaseMigration(name, _))
        .value
        .onShutdown(Left(ShutdownDuringStartup(name, "DB migration interrupted due to shutdown")))
    }

  private def runRepairMigration(
      name: InstanceName,
      storageConfig: StorageConfig,
      devVersionSupport: Boolean,
  ): Either[StartupError, Unit] =
    runIfUsingDatabase[Id](storageConfig) { dbConfig =>
      migrationsFactory
        .create(dbConfig, name, devVersionSupport)
        .repairFlywayMigration()
        .leftMap(FailedDatabaseRepairMigration(name, _))
        .value
        .onShutdown(
          Left(ShutdownDuringStartup(name, "DB repair migration interrupted due to shutdown"))
        )
    }
}

class ParticipantNodes[B <: CantonNodeBootstrap[N], N <: CantonNode, PC <: LocalParticipantConfig](
    create: (String, PC) => B, // (nodeName, config) => bootstrap
    migrationsFactory: DbMigrationsFactory,
    timeouts: ProcessingTimeout,
    configs: Map[String, PC],
    parametersFor: String => ParticipantNodeParameters,
    loggerFactory: NamedLoggerFactory,
)(implicit
    protected val executionContext: ExecutionContextIdlenessExecutorService
) extends ManagedNodes[N, PC, ParticipantNodeParameters, B](
      create,
      migrationsFactory,
      timeouts,
      configs,
      parametersFor,
      startUpGroup = 0,
      loggerFactory,
    ) {
  private def migrateIndexerDatabase(name: InstanceName): Either[StartupError, Unit] = {
    import TraceContext.Implicits.Empty.*

    for {
      config <- configs.get(name).toRight(ConfigurationNotFound(name))
      parameters = parametersFor(name)
      _ = parameters.processingTimeouts.unbounded.await("migrate indexer database") {
        runIfUsingDatabase[Future](config.storage) { dbConfig =>
          CantonLedgerApiServerWrapper
            .migrateSchema(
              MigrateSchemaConfig(
                dbConfig,
                config.ledgerApi.additionalMigrationPaths,
              ),
              loggerFactory,
            )
            .map(_.asRight)
        }
      }
    } yield ()
  }

  override def migrateDatabase(name: InstanceName): Either[StartupError, Unit] =
    for {
      _ <- super.migrateDatabase(name)
      _ <- migrateIndexerDatabase(name)
    } yield ()
}

object ParticipantNodes {
  type ParticipantNodesOld[PC <: LocalParticipantConfig] =
    ParticipantNodes[ParticipantNodeBootstrap, ParticipantNode, PC]
  type ParticipantNodesX[PC <: LocalParticipantConfig] =
    ParticipantNodes[ParticipantNodeBootstrapX, ParticipantNodeX, PC]
}

class DomainNodes[DC <: DomainConfig](
    create: (String, DC) => DomainNodeBootstrap,
    migrationsFactory: DbMigrationsFactory,
    timeouts: ProcessingTimeout,
    configs: Map[String, DC],
    parameters: String => DomainNodeParameters,
    loggerFactory: NamedLoggerFactory,
)(implicit
    protected val executionContext: ExecutionContextIdlenessExecutorService
) extends ManagedNodes[Domain, DC, DomainNodeParameters, DomainNodeBootstrap](
      create,
      migrationsFactory,
      timeouts,
      configs,
      parameters,
      startUpGroup = 0,
      loggerFactory,
    )

class MediatorNodesX[MNC <: MediatorNodeConfigCommon](
    create: (String, MNC) => MediatorNodeBootstrapX,
    migrationsFactory: DbMigrationsFactory,
    timeouts: ProcessingTimeout,
    configs: Map[String, MNC],
    parameters: String => MediatorNodeParameters,
    loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends ManagedNodes[
      MediatorNodeX,
      MNC,
      MediatorNodeParameters,
      MediatorNodeBootstrapX,
    ](
      create,
      migrationsFactory,
      timeouts,
      configs,
      parameters,
      startUpGroup = 1,
      loggerFactory,
    )
