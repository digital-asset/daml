// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.resource

import cats.data.EitherT
import cats.syntax.either.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{
  DbConfig,
  DbLockedConnectionPoolConfig,
  ProcessingTimeout,
  QueryCostMonitoringConfig,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.health.ComponentHealthState
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.DbStorageMetrics
import com.digitalasset.canton.resource.DbStorage.DbAction.{All, ReadTransactional}
import com.digitalasset.canton.resource.DbStorageMulti.passiveInstanceHealthState
import com.digitalasset.canton.time.{Clock, PositiveFiniteDuration, WallClock}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureUnlessShutdownUtil
import com.digitalasset.canton.util.Thereafter.syntax.*
import slick.jdbc.JdbcBackend.Database
import slick.util.{AsyncExecutor, AsyncExecutorWithMetrics, QueryCostTrackerImpl}

import java.sql.SQLTransientConnectionException
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.concurrent.ExecutionContext

/** DB Storage implementation that allows multiple processes to access the underlying database and
  * uses a pool of write connections which are guarded by an exclusive main lock to ensure a single
  * writer instance.
  *
  * Periodically checks the activeness of the write connection pool and if the activeness changes
  * executes the `onActive` or `onPassive` callbacks.
  */
final class DbStorageMulti private (
    override val profile: DbStorage.Profile with DbStorage.DbLockSupport,
    generalDb: Database,
    private[resource] val writeConnectionPool: DbLockedConnectionPool,
    val dbConfig: DbConfig,
    onActive: () => FutureUnlessShutdown[Unit],
    onPassive: () => FutureUnlessShutdown[Option[CloseContext]],
    checkPeriod: PositiveFiniteDuration,
    clock: Clock,
    closeClock: Boolean,
    logQueryCost: Option[QueryCostMonitoringConfig],
    override val metrics: DbStorageMetrics,
    override protected val timeouts: ProcessingTimeout,
    override val threadsAvailableForWriting: PositiveInt,
    override protected val loggerFactory: NamedLoggerFactory,
    initialCloseContext: Option[CloseContext],
    writeDbExecutor: AsyncExecutor,
)(override protected implicit val ec: ExecutionContext)
    extends DbStorage
    with NamedLogging
    with HasCloseContext {

  protected val logOperations: Boolean = logQueryCost.exists(_.logOperations)

  private val active: AtomicBoolean = new AtomicBoolean(writeConnectionPool.isActive)

  private val sessionCloseContext = new AtomicReference[Option[CloseContext]](initialCloseContext)

  override def initialHealthState: ComponentHealthState =
    if (active.get()) ComponentHealthState.Ok()
    else passiveInstanceHealthState

  private def checkHealth(now: CantonTimestamp): Unit =
    TraceContext.withNewTraceContext("db_health") { implicit traceContext =>
      synchronizeWithClosingSync(functionFullName) {
        logger.trace(s"Checking storage health at $now")

        val connectionPoolActive = writeConnectionPool.isActive
        val activeOrPassive = if (connectionPoolActive) "active" else "passive"
        if (active.compareAndSet(!connectionPoolActive, connectionPoolActive)) {
          logger.debug(
            s"Write connection pool is now $activeOrPassive. Beginning state transition."
          )
          // We have a transition of the activeness
          val transitionReplicaState =
            if (connectionPoolActive)
              onActive()
                .thereafter(_ => reportHealthState(ComponentHealthState.Ok()))
            else
              onPassive()
                .map(sessionCloseContext.set)
                .thereafter(_ => reportHealthState(passiveInstanceHealthState))

          FutureUnlessShutdownUtil.doNotAwaitUnlessShutdown(
            // Schedule next health check after transition is completed
            transitionReplicaState.thereafter(_ => scheduleHealthCheck(clock.now)),
            failureMessage = s"Failed to transition replica state",
            // if the transition failed we revert the new activeness state
            onFailure = _ => active.set(!connectionPoolActive),
          )
        } else {
          // Immediately schedule next health check
          logger.trace(
            s"Write connection pool is still $activeOrPassive. Scheduling next health check."
          )
          scheduleHealthCheck(clock.now)
        }
      }.onShutdown {
        logger.debug(s"Shutting down, stop storage health check")
      }
    }

  private def scheduleHealthCheck(now: CantonTimestamp)(implicit tc: TraceContext): Unit = {
    val nextCheckTime = now.add(checkPeriod.unwrap)
    logger.trace(s"Scheduling the next health check at $nextCheckTime")
    FutureUnlessShutdownUtil.doNotAwaitUnlessShutdown(
      clock.scheduleAt(
        checkHealth,
        nextCheckTime,
      ),
      "failed to schedule next health check",
      closeContext = Some(closeContext),
    )
  }

  // Run the initial health check
  checkHealth(clock.now)

  private val writeDb: Database = DbLockedConnectionPool.createDatabaseFromPool(
    writeConnectionPool,
    writeDbExecutor,
  )

  private def runIfSessionIsOpen[A](
      action: String,
      operationName: String,
      maxRetries: Int,
  )(
      f: => FutureUnlessShutdown[A]
  )(implicit traceContext: TraceContext, closeContext: CloseContext): FutureUnlessShutdown[A] = {
    val sessionContext = sessionCloseContext.get
    sessionContext
      .map { sessionCC =>
        if (sessionCC.context.isClosing) {
          FutureUnlessShutdown.abortedDueToShutdown
        } else {
          CloseContext.withCombinedContext(closeContext, sessionCC, timeouts, logger) { cc =>
            run(action, operationName, maxRetries) {
              f
            }(traceContext, cc)
          }
        }
      }
      .getOrElse {
        run(action, operationName, maxRetries) {
          f
        }
      }
      .recover {
        // If the session close context is closed, DB queries won't be retried but may end up bubbling up SQL errors that would
        // normally be retried. Catch them here and replace them with a more appropriate AbortedDueToShutdown.
        case e: SQLTransientConnectionException if sessionContext.exists(_.context.isClosing) =>
          logger.debug(
            "Caught a transient DB error while session close context is closing. Masking it with AbortedDueToShutdown",
            e,
          )
          UnlessShutdown.AbortedDueToShutdown
      }
  }

  override protected[canton] def runRead[A](
      action: ReadTransactional[A],
      operationName: String,
      maxRetries: Int,
  )(implicit traceContext: TraceContext, closeContext: CloseContext): FutureUnlessShutdown[A] =
    runIfSessionIsOpen("reading", operationName, maxRetries)(
      FutureUnlessShutdown.outcomeF(generalDb.run(action))
    )

  override protected[canton] def runWrite[A](
      action: All[A],
      operationName: String,
      maxRetries: Int,
  )(implicit traceContext: TraceContext, closeContext: CloseContext): FutureUnlessShutdown[A] =
    runIfSessionIsOpen("writing", operationName, maxRetries)(
      FutureUnlessShutdown.outcomeF(writeDb.run(action))
    )

  override def isActive: Boolean = writeConnectionPool.isActive

  override def onClosed(): Unit = {
    // Closing first the pool and then the executor, otherwise we may get rejected execution exceptions for the pool's connection health checks.
    // Slick by default closes first the executor and then the source, which does not work here.
    val clockCloseable = if (closeClock) Seq(clock) else Seq.empty
    val otherCloseables = Seq(generalDb, writeConnectionPool, writeDbExecutor)
    LifeCycle.close((clockCloseable ++ otherCloseables)*)(logger)
  }

  def setPassive()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    writeConnectionPool.setPassive()

  def setSessionCloseContext(sessionContext: Option[CloseContext]): Unit =
    sessionCloseContext.set(sessionContext)
}

object DbStorageMulti {
  private val passiveInstanceHealthState = ComponentHealthState.failed("instance is passive")

  /** Creates a new multi-process aware DbStorage.
    *
    * The caller should check if the returned instance is active or not, and act correspondingly,
    * because onActive/onPassive are not invoked during creation.
    *
    * @param customClock
    *   allows for injecting a custom clock in tests. The caller is responsible for closing the
    *   custom clock.
    */
  def create(
      dbConfig: DbConfig,
      writeConnectionPoolConfig: DbLockedConnectionPoolConfig,
      readPoolSize: PositiveInt,
      writePoolSize: PositiveInt,
      mainLockCounter: DbLockCounter,
      poolLockCounter: DbLockCounter,
      onActive: () => FutureUnlessShutdown[Unit],
      onPassive: () => FutureUnlessShutdown[Option[CloseContext]],
      metrics: DbStorageMetrics,
      logQueryCost: Option[QueryCostMonitoringConfig],
      customClock: Option[Clock],
      scheduler: Option[ScheduledExecutorService],
      timeouts: ProcessingTimeout,
      exitOnFatalFailures: Boolean,
      futureSupervisor: FutureSupervisor,
      loggerFactory: NamedLoggerFactory,
      initialCloseContext: Option[CloseContext] = None,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
      closeContext: CloseContext,
  ): EitherT[UnlessShutdown, String, DbStorageMulti] = {
    val logger = loggerFactory.getTracedLogger(getClass)

    // By default, ensure that storage runs with wallclock for its health checks
    val clock: Clock = customClock.getOrElse(new WallClock(timeouts, loggerFactory))

    logger.info(s"Creating storage, num-reads: $readPoolSize, num-writes: $writePoolSize")
    for {
      generalDb <- DbStorage
        .createDatabase(
          dbConfig,
          readPoolSize,
          Some(metrics.general),
          logQueryCost,
          scheduler,
        )(loggerFactory)

      profile <- DbLock.isSupported(DbStorage.profile(dbConfig)).toEitherT[UnlessShutdown]

      writeExecutor = {
        import slick.util.ConfigExtensionMethods.*
        val logger = loggerFactory.getLogger(DbStorageMulti.getClass)
        val tracker = new QueryCostTrackerImpl(
          logQueryCost,
          metrics.write,
          scheduler,
          warnOnSlowQueryO = dbConfig.parameters.warnOnSlowQuery.map(_.toInternal),
          warnInterval = dbConfig.parameters.warnOnSlowQueryInterval.toInternal,
          writePoolSize.value,
          logger,
        )
        new AsyncExecutorWithMetrics(
          name = "db-lock-pool-ec",
          minThreads = writePoolSize.value,
          maxThreads = writePoolSize.value,
          queueSize = dbConfig.config.getIntOr("queueSize", 1000),
          logger,
          tracker,
          maxConnections = writePoolSize.value,
        )
      }

      writeConnectionPool <- DbLockedConnectionPool
        .create(
          profile,
          dbConfig,
          writeConnectionPoolConfig,
          writePoolSize,
          mainLockCounter,
          poolLockCounter,
          clock,
          timeouts,
          exitOnFatalFailures = exitOnFatalFailures,
          futureSupervisor,
          loggerFactory,
          writeExecutor,
        )
        .leftMap(err => s"Failed to create write connection pool: $err")
        .toEitherT[UnlessShutdown]

      sharedStorage = new DbStorageMulti(
        profile,
        generalDb,
        writeConnectionPool,
        dbConfig,
        onActive,
        onPassive,
        writeConnectionPoolConfig.healthCheckPeriod.toInternal,
        clock,
        closeClock = customClock.isEmpty,
        logQueryCost,
        metrics,
        timeouts,
        writePoolSize,
        loggerFactory,
        initialCloseContext,
        writeExecutor,
      )
    } yield sharedStorage
  }

}
