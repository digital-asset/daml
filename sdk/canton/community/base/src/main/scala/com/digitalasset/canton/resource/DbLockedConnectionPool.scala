// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.resource

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.functorFilter.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.concurrent.{DirectExecutionContext, FutureSupervisor}
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{DbConfig, DbLockedConnectionPoolConfig, ProcessingTimeout}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  NamedLoggerFactory,
  NamedLogging,
  TracedLogger,
}
import com.digitalasset.canton.resource.DbStorage.PassiveInstanceException
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.retry.NoExceptionRetryPolicy
import com.digitalasset.canton.util.{
  EitherTUtil,
  FutureUnlessShutdownUtil,
  SimpleExecutionQueue,
  retry,
}
import com.google.common.annotations.VisibleForTesting
import org.slf4j.event.Level
import slick.jdbc.JdbcBackend.Database
import slick.jdbc.{DataSourceJdbcDataSource, JdbcDataSource}
import slick.util.{AsyncExecutor, AsyncExecutorWithMetrics, AsyncExecutorWithShutdown}

import java.sql.{Connection, SQLTransientException}
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}

/** A pool of [[DbLockedConnection]] for writes guarded by a main lock. It implements the
  * [[slick.jdbc.JdbcDataSource]] trait to be used by slick.
  *
  * The pool is considered active iff the main connection is active, i.e., the connection is healthy
  * and the lock acquired. If the main connection becomes inactive, the pool of connections are
  * ramped down and the pool periodically attempts to become active again. Once a pool becomes
  * active, it waits for the other pool to ramp down by acquiring exclusive access to the pool's
  * shared lock.
  */
class DbLockedConnectionPool private (
    ds: JdbcDataSource,
    private[resource] val mainConnection: DbLockedConnection,
    private val mainExecutor: AsyncExecutorWithShutdown,
    val config: DbLockedConnectionPoolConfig,
    private[resource] val poolSize: PositiveInt,
    createPoolConnection: (String, DbLockMode) => DbLockedConnection,
    clock: Clock,
    exitOnFatalFailures: Boolean,
    futureSupervisor: FutureSupervisor,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends JdbcDataSource
    with FlagCloseable
    with HasCloseContext
    with NamedLogging
    with StateMachine[DbLockedConnectionPool.State] {

  import DbLockedConnectionPool.*

  private val execQueue =
    new SimpleExecutionQueue(
      "db-locked-connection-pool-queue",
      futureSupervisor,
      timeouts,
      loggerFactory,
      crashOnFailure = exitOnFatalFailures,
    )

  // Start out as passive and try to become active
  override protected val stateRef: AtomicReference[DbLockedConnectionPool.State] =
    new AtomicReference(State.Passive)

  private def becomeActive()(implicit traceContext: TraceContext): Future[Unit] = {
    logger.debug("Becoming active")

    // Create an exclusive connection and lock for the pool and wait until active to ensure that the other connection
    // pool has scaled down all its connections.
    val poolExclusive = createPoolConnection("pool-wait", DbLockMode.Exclusive)
    retry
      .Pause(
        logger,
        this,
        retry.Forever,
        500.millis,
        "wait for exclusive access to connection pool",
        retryLogLevel = Some(Level.DEBUG),
      )(
        poolExclusive.get match {
          // While we're waiting to acquire the pool in exclusive mode, it's possible that we lose the main connection lock
          // to another instance (for whatever reason). If that happens we can stop retrying here.
          case Left(_) if !mainConnection.isActive =>
            // Close the exclusive pool connection before failing the retry
            poolExclusive.close()
            Future.failed(
              PassiveInstanceException(
                s"Main connection lock was lost while trying to acquire write pool locks"
              )
            )
          case other => Future.successful(other)
        },
        NoExceptionRetryPolicy,
      )
      .map {
        case Left(err) =>
          // This can only happen when the retry is aborted due to shutdown, because otherwise we retry forever
          logger.info(s"Failed to get exclusive access to the pool's lock, staying passive: $err")
          poolExclusive.close()

        case Right(_conn) =>
          logger.debug("Obtained exclusive access to the pool's lock")

          // Close the exclusive access to the pool and ramp up the connections in the pool using a shared lock.
          poolExclusive.close()

          logger.debug(s"Ramping up connection pool with ${poolSize.value} connections")

          val pool = Range(0, poolSize.value).map { idx =>
            createPoolConnection(s"pool-$idx", DbLockMode.Shared)
          }

          // Additional check that the main lock is still acquired when the first shared lock of the pool has been acquired.
          val result = timeouts.network.await("check main connection health when becoming active") {
            // Break up the configurable timeout into smaller chunks of 200ms
            val waitInMs = 200L
            val maxRetries =
              Math.min(config.activeTimeout.asJava.toMillis / waitInMs, Int.MaxValue).toInt
            val checkResult = for {
              firstConnection <- pool.headOption
                .toRight("Pool is empty")
                .toEitherT[FutureUnlessShutdown]
              // Wait until one connection is active and has acquired the lock
              _ <- DbLockedConnection
                .awaitActive(firstConnection, maxRetries, waitInMs, logger)
                .leftMap(err => s"Connection did not become active: $err")
                .mapK(FutureUnlessShutdown.liftK)
              // Force a check that the main connection is valid and the lock is still acquired
              isActive <- mainConnection.isActiveNow().mapK(FutureUnlessShutdown.outcomeK)
              _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
                isActive,
                "Lost main connection lock while becoming active",
              )
            } yield ()

            checkResult.value.unwrap
          }

          result match {
            case UnlessShutdown.Outcome(Left(err)) =>
              logger.info(s"Failed to ramp up pool, staying passive: $err")
              pool.foreach(_.close())

            case UnlessShutdown.Outcome(Right(())) =>
              transitionOrFail(State.Passive, State.Active(pool))
              logger.debug(s"Connection pool now active")

            case UnlessShutdown.AbortedDueToShutdown =>
              logger.info(s"Becoming active was interrupted due to shutdown")
              pool.foreach(_.close())
          }
      }
  }

  private def becomePassive()(implicit traceContext: TraceContext): Unit = {
    logger.debug("Becoming passive")

    def getActiveState(state: State): Option[State.Active] = state match {
      case active: State.Active => Some(active)
      case State.Passive => None
    }

    logger.trace(s"Closing pool connections")
    LifeCycle.close(
      (transitionOrFail(
        classOf[State.Active].getSimpleName,
        getActiveState(_),
        State.Passive,
      ).pool)*
    )(logger)
  }

  private def checkPoolHealth(
      pool: Seq[DbLockedConnection]
  )(implicit traceContext: TraceContext): Unit = {
    val invalidConns = pool.filterNot(_.isActive)
    if (invalidConns.nonEmpty) {
      logger.info(s"#${invalidConns.size}/${poolSize.value} connections unhealthy")
      invalidConns.foreach { c =>
        logger.debug(s"Connection $c unhealthy")
      }
    } else {
      logger.trace("Connection pool is healthy")
    }
  }

  @VisibleForTesting
  private[resource] def getPool: Option[Seq[DbLockedConnection]] = stateRef.get() match {
    case State.Active(pool) => Some(pool)
    case State.Passive => None
  }

  private def checkHealth()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    def checkHealthInternal(): Future[Unit] = {
      val result = stateRef.get() match {
        case State.Active(pool) =>
          if (mainConnection.isActive) {
            logger.trace(s"Connection pool remains active")
            checkPoolHealth(pool)
            Future.unit
          } else {
            becomePassive()
            Future.unit
          }
        case State.Passive =>
          if (!mainConnection.isActive) {
            logger.trace(s"Connection pool remains passive")
            Future.unit
          } else {
            becomeActive()
          }
      }

      // Run health check scheduling on the direct execution context to avoid rejected execution exceptions
      result.map(_ => scheduleHealthCheck(clock.now))(DirectExecutionContext(logger))
    }

    // Run the health check and becoming active/passive through the queue to ensure there's only one attempt at a time.
    execQueue.execute(checkHealthInternal(), "check-health")
  }

  private def runScheduledHealthCheck(ts: CantonTimestamp): Unit =
    TraceContext.withNewTraceContext("db_scheduled_health_check") { implicit traceContext =>
      synchronizeWithClosingSync(functionFullName) {
        logger.trace(s"Checking connection pool health at $ts")
        FutureUnlessShutdownUtil.doNotAwaitUnlessShutdown(
          checkHealth(),
          "failed connection pool health check",
        )
      }.onShutdown {
        logger.debug("Shutting down, stop connection pool health check")
      }
    }

  private def scheduleHealthCheck(now: CantonTimestamp): Unit =
    clock.scheduleAt(runScheduledHealthCheck, now.add(config.healthCheckPeriod.asJava)).discard

  private def findActiveConnection(pool: Seq[DbLockedConnection]): Option[KeepAliveConnection] = {
    val availableConnectionOpt = pool.mapFilter(_.get.toOption).find(_.markInUse())

    if (availableConnectionOpt.isEmpty) {
      logger.debug(s"Did not find any available connection in the pool. Connection states: ${pool
          .map(_.get.map(c => if (c.inUse.get()) "In use" else "Not in use"))
          .mkString(", ")}")(TraceContext.empty)
    }
    availableConnectionOpt
  }

  // Run and wait for the initial health check
  TraceContext.withNewTraceContext("db_initial_health_check") { implicit traceContext =>
    timeouts.default.await("initial health check") {
      checkHealth().onShutdown(())
    }
  }

  /** The pool is active and the main connection is active */
  def isActive: Boolean = stateRef.get() match {
    case _: State.Active if mainConnection.isActive => true
    case _ => false
  }

  def isPassive: Boolean = stateRef.get() match {
    case _: State.Active => false
    case State.Passive => true
  }

  def setPassive()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = {

    def setPassiveInternal(): EitherT[FutureUnlessShutdown, String, Unit] = {
      logger.info("Setting active connection pool to passive")

      // Closes the main connection and thus release its lock, which will trigger becomePassive on the next health check
      mainConnection.setPassive()
    }

    // Run the set passive in the execution queue to not infer with health checks
    execQueue.executeEUS(setPassiveInternal(), "set passive")
  }

  override def createConnection(): Connection =
    stateRef.get() match {
      case State.Active(pool) =>
        findActiveConnection(pool).getOrElse(throw NoActiveConnectionAvailable())
      case State.Passive =>
        throw PassiveInstanceException(s"Connection pool is not active")
    }

  override val maxConnections: Option[Int] = Some(poolSize.value)

  override def onClosed(): Unit =
    stateRef.get() match {
      case State.Active(pool) =>
        LifeCycle.close(execQueue +: pool :+ mainConnection :+ mainExecutor :+ ds: _*)(logger)
      case State.Passive =>
        LifeCycle.close(execQueue, mainConnection, mainExecutor, ds)(logger)
    }

}

object DbLockedConnectionPool {

  final case class NoActiveConnectionAvailable()
      extends SQLTransientException("No active and free KeepAliveConnection available")

  sealed trait State extends Product with Serializable
  object State {
    final case class Active(pool: Seq[DbLockedConnection]) extends State
    case object Passive extends State
  }

  def createDatabaseFromPool(
      connectionPool: DbLockedConnectionPool,
      executor: AsyncExecutor,
  ): Database =
    Database.forSource(
      connectionPool,
      executor,
    )

  def create(
      profile: DbStorage.Profile with DbStorage.DbLockSupport,
      dbConfig: DbConfig,
      config: DbLockedConnectionPoolConfig,
      poolSize: PositiveInt,
      mainLockCounter: DbLockCounter,
      poolLockCounter: DbLockCounter,
      clock: Clock,
      timeouts: ProcessingTimeout,
      exitOnFatalFailures: Boolean,
      futureSupervisor: FutureSupervisor,
      loggerFactory: NamedLoggerFactory,
      writeExecutor: AsyncExecutorWithShutdown,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
      closeContext: CloseContext,
  ): Either[DbLockedConnectionPoolError, DbLockedConnectionPool] = {

    val logger = loggerFactory.getLogger(DbLockedConnectionPool.getClass)
    val tracedLogger = TracedLogger.apply(logger)

    tracedLogger.debug(s"Creating write connection pool with ${poolSize.value} connections")

    def createConnection(
        ds: DataSourceJdbcDataSource,
        connId: String,
        lockId: DbLockId,
        mode: DbLockMode,
        isMainConnection: Boolean,
        executor: AsyncExecutorWithShutdown,
    ): DbLockedConnection =
      DbLockedConnection.create(
        profile,
        ds,
        lockId,
        mode,
        config.connection,
        isMainConnection,
        timeouts,
        exitOnFatalFailures,
        clock,
        // We create multiple connections, differentiate them using a connection id
        loggerFactory.append(
          "connId",
          connId,
        ),
        futureSupervisor,
        executor,
        logLockOwnersOnLockAcquisitionAttempt = false,
      )

    for {
      ds <- DbLockedConnection
        .createDataSource(
          dbConfig.config,
          poolSize.value + 1, // We obtain the main connection and poolSize connections from the data source
          config.connection.connectionTimeout.toInternal,
        )(ErrorLoggingContext.fromTracedLogger(tracedLogger))
        .leftMap(DbLockedConnectionPoolError.FailedToCreateDataSource.apply)
      mainLockId <- DbLockId
        .allocate(dbConfig, mainLockCounter, loggerFactory)
        .leftMap(DbLockedConnectionPoolError.FailedToAllocateLockId.apply)
      poolLockId <- DbLockId
        .allocate(dbConfig, poolLockCounter, loggerFactory)
        .leftMap(DbLockedConnectionPoolError.FailedToAllocateLockId.apply)

      // main connection gets its own executor
      mainExecutor = AsyncExecutorWithMetrics.createSingleThreaded("PoolMain", logger)
      mainConnection = createConnection(
        ds,
        "pool-main",
        mainLockId,
        DbLockMode.Exclusive,
        isMainConnection = true,
        mainExecutor,
      )

      // Wait until main connection is either active or passive before returning the connection pool
      _ = DbLockedConnection.awaitInitialized(mainConnection, 50, 200, tracedLogger)
    } yield {
      new DbLockedConnectionPool(
        ds,
        mainConnection,
        mainExecutor,
        config,
        poolSize,
        (connId, lockMode) =>
          // lock connection gets sent through the write pool executor
          createConnection(
            ds,
            connId,
            poolLockId,
            lockMode,
            isMainConnection = false,
            writeExecutor,
          ),
        clock,
        exitOnFatalFailures = exitOnFatalFailures,
        futureSupervisor,
        timeouts,
        loggerFactory,
      )
    }
  }

}

sealed trait DbLockedConnectionPoolError extends Product with Serializable
object DbLockedConnectionPoolError {
  final case class FailedToCreateDataSource(error: String) extends DbLockedConnectionPoolError
  final case class FailedToAllocateLockId(error: DbLockError) extends DbLockedConnectionPoolError
}
