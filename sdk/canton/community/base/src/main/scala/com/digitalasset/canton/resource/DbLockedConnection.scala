// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.resource

import cats.data.EitherT
import cats.syntax.either.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.checked
import com.digitalasset.canton.concurrent.{FutureSupervisor, Threading}
import com.digitalasset.canton.config.{
  DbLockedConnectionConfig,
  PositiveFiniteDuration as PositiveFiniteDurationConfig,
  ProcessingTimeout,
}
import com.digitalasset.canton.crypto.PseudoRandom
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  NamedLoggerFactory,
  NamedLogging,
  TracedLogger,
}
import com.digitalasset.canton.resource.DbStorage.Profile
import com.digitalasset.canton.time.{Clock, PositiveFiniteDuration}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.*
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.retry.{AllExceptionRetryPolicy, RetryEither, Success}
import com.google.common.annotations.VisibleForTesting
import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.event.Level
import slick.dbio.DBIO
import slick.jdbc.{DataSourceJdbcDataSource, JdbcDataSource}
import slick.util.{AsyncExecutorWithShutdown, ClassLoaderUtil}

import java.io.EOFException
import java.sql.{Connection, SQLException}
import java.time.Duration
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future, TimeoutException, blocking}
import scala.jdk.CollectionConverters.*
import scala.util.control.Exception.handling
import scala.util.{Failure, Try}

/** Maintains the combination of a persistent DB connection and a DB lock acquired on that
  * connection.
  *
  * If the connection is closed or becomes invalid, it will try to rebuild the connection and
  * re-acquire the lock.
  */
class DbLockedConnection private (
    createConnection: () => EitherT[Future, String, KeepAliveConnection],
    createLock: KeepAliveConnection => DbLock,
    config: DbLockedConnectionConfig,
    checkReadOnly: KeepAliveConnection => EitherT[Future, String, Boolean],
    private[resource] val lockId: DbLockId,
    exitOnFatalFailures: Boolean,
    clock: Clock,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
    futureSupervisor: FutureSupervisor,
    writeExecutor: AsyncExecutorWithShutdown,
    logLockOwnersOnLockAcquisitionAttempt: Boolean,
)(implicit ec: ExecutionContext)
    extends NamedLogging
    with FlagCloseable
    with HasCloseContext
    with StateMachine[DbLockedConnection.State] {

  import DbLockedConnection.*

  private val execQueue =
    new SimpleExecutionQueue(
      "db-locked-connection-queue",
      futureSupervisor,
      timeouts,
      loggerFactory,
      crashOnFailure = exitOnFatalFailures,
    )

  override protected val stateRef: AtomicReference[State] = new AtomicReference(
    State.Init
  )

  @VisibleForTesting
  private[resource] def state: State = stateRef.get()

  private def withLockOwners(
      lock: DbLock
  )(f: Vector[Long] => Unit)(implicit traceContext: TraceContext): Future[Unit] =
    lock
      .getLockOwners()
      .value
      .transform {
        case util.Success(Right(lockOwners)) =>
          util.Success(f(lockOwners))
        case util.Success(Left(err)) =>
          util.Success(logger.debug(s"Failed to get lock owners for lock ${lock.lockId}: $err"))
        case Failure(err) =>
          util.Success(logger.debug(s"Failed to get lock owners for lock ${lock.lockId}", err))
      }

  /** Rebuild the DB connection until successfully reconnected or shutdown.
    *
    * Retries indefinitely to connect to the database.
    */
  private def rebuildConnection()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[KeepAliveConnection] =
    retry
      .Backoff(
        logger,
        this,
        retry.Forever,
        100.millis,
        10.seconds,
        operationName = functionFullName,
        retryLogLevel = Some(Level.INFO),
      )
      .unlessShutdown(
        createConnection().mapK(FutureUnlessShutdown.outcomeK).value,
        AllExceptionRetryPolicy,
      )
      // Due to the infinite retry we must get a connection here
      .map(_.valueOr(err => ErrorUtil.invalidState(s"Failed to establish DB connection: $err")))

  /** Attempt to acquire the DB lock with customizable retry config
    */
  private def becomeActiveWithRetries(
      connection: KeepAliveConnection,
      lock: DbLock,
      maxRetries: Int,
      retryInterval: PositiveFiniteDurationConfig,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Either[DbLockError, Boolean]] = {
    implicit val success: Success[Either[DbLockError, Boolean]] = Success.apply {
      case Right(false) =>
        // Lock acquisition was not successful, retry
        false
      case Left(DbLockError.LockAcquireRejected(_)) =>
        // Lock acquisition failed due to contention in slick, retry
        false
      case Left(err) =>
        // Failed with an error (e.g., connection problem, and not contention), stop the retry
        true
      case Right(true) =>
        // Lock acquisition was successful, stop the retry
        true
    }

    FutureUnlessShutdownUtil.logOnFailureUnlessShutdown(
      retry
        .Pause(
          logger,
          this,
          maxRetries,
          retryInterval.underlying,
          operationName = functionFullName,
          retryLogLevel = Some(Level.DEBUG),
        )
        .unlessShutdown(
          synchronizeWithClosingF(functionFullName) {
            lock.tryAcquire().value.thereafterF {
              case scala.util.Success(Right(true)) if logLockOwnersOnLockAcquisitionAttempt =>
                withLockOwners(lock)(lockOwners =>
                  logger.debug(
                    s"Lock successfully acquired, the following PIDs now own it: ${lockOwners.mkString(", ")}"
                  )
                )
              case scala.util.Success(Right(false)) if logLockOwnersOnLockAcquisitionAttempt =>
                // When the lock could not be acquired, check who already owns the lock
                withLockOwners(lock)(lockOwners =>
                  logger.debug(
                    s"Failed to acquire lock, the following PIDs already own it: ${lockOwners.mkString(", ")}"
                  )
                )
              case _ =>
                // In case of connection/lock acquisition errors, do not check lock owners
                Future.unit
            }
          },
          AllExceptionRetryPolicy,
        )
        .tapOnShutdown {
          logger.debug(s"Aborting to become active due to shutdown, closing connection and lock..")
          closeLockedConnection(connection, lock, Level.DEBUG)
        },
      "failed to acquire lock and become active",
      level = Level.INFO,
    )

  }

  // For the initial attempt to become active, the retry interval period is shortened
  // Regardless of the outcome
  private def initialBecomeActive(
      connection: KeepAliveConnection,
      lock: DbLock,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val initialAttempt = becomeActiveWithRetries(
      connection,
      lock,
      config.initialAcquisitionMaxRetries,
      config.initialAcquisitionInterval,
    )

    initialAttempt.transformIntoSuccess {
      case util.Success(UnlessShutdown.AbortedDueToShutdown) =>
        UnlessShutdown.AbortedDueToShutdown
      case util.Success(UnlessShutdown.Outcome(Right(true))) =>
        // We already became active on the first try
        UnlessShutdown.Outcome(())
      case _ =>
        // The initial attempt failed or we did not become active, try again in the background
        UnlessShutdown.Outcome(becomeActive(connection, lock))
    }
  }

  /** Attempt to acquire the DB lock until successfully acquired, a failure occurred, or shutdown.
    *
    * Retries indefinitely if the acquisition fails.
    */
  private def becomeActive(
      connection: KeepAliveConnection,
      lock: DbLock,
  )(implicit traceContext: TraceContext): Unit = {

    logger.debug(s"Trying to become active..")

    val becomeActiveF = becomeActiveWithRetries(
      connection,
      lock,
      retry.Forever,
      config.passiveCheckPeriod,
    ).map {
      case Left(err) => logger.info(s"Failed to become active: $err")
      case Right(false) => logger.debug("Failed to acquire lock and become active")
      case Right(true) => logger.trace("Successfully became active")
    }

    FutureUtil.doNotAwait(
      becomeActiveF.unwrap,
      "failed to acquire lock and become active",
      level = Level.INFO,
    )
  }

  private def connectAndBecomeActive(init: Boolean)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, DbLockError, State.Connected] =
    for {
      // Rebuild the database connection
      newConn <- EitherT.right(rebuildConnection())
      _ = logger.debug("Succeeded to reconnect to database")

      _ = logger.debug(s"Creating new DB lock with $newConn")
      newLock = createLock(newConn)

      // During initialization, wait for the lock acquisition to complete before returning to avoid racing the node initialization
      _ <-
        if (init)
          EitherT
            .liftF[FutureUnlessShutdown, DbLockError, Unit](
              initialBecomeActive(newConn, newLock)
            )
        // Otherwise run it in the background
        else
          EitherT.pure[FutureUnlessShutdown, DbLockError](
            becomeActive(newConn, newLock)
          )
    } yield State.Connected(newConn, newLock)

  private def buildLockedConnection(fromState: State)(implicit traceContext: TraceContext): Unit = {

    transitionOrFail(
      fromState,
      State.Recovering,
    )

    val initial = fromState == State.Init

    val result = for {

      // Retry to connect to the database and become active. If the lock acquisition fails with an error, we rebuild the connection
      connected <- retry
        .Backoff(
          logger,
          this,
          retry.Forever,
          500.millis,
          10.second,
          operationName = functionFullName,
        )
        .unlessShutdown(connectAndBecomeActive(initial).value, AllExceptionRetryPolicy)
        .map(_.valueOr(err => ErrorUtil.invalidState(s"Failed to connect and become active: $err")))
        .tapOnShutdown(logger.debug("Stopped recovery due to shutdown"))

      _ <- FutureUnlessShutdown.lift(
        synchronizeWithClosingSync("transitioning to connected state")(
          transitionOrFail(State.Recovering, connected)
        )
      )
      _ = logger.info("Successfully rebuilt connection")

      // Schedule next health check from the current time when we have recovered
      _ = scheduleCheckConnection(clock.now)
    } yield {
      logger.debug("Successfully finished locked connection rebuild")
    }

    // We don't need to wait for the future to complete here, the next connection check is only scheduled when the recovery is completed
    FutureUtil.doNotAwait(result.unwrap, "Failed to recover locked connection")
  }

  private def closeLockedConnection(connection: KeepAliveConnection, lock: DbLock, logLevel: Level)(
      implicit traceContext: TraceContext
  ): Unit = {
    // Stop the health check of the lock, this does not release the lock if acquired.
    lock.close()
    // Close the database connection. If the lock was acquired it is released due to the connection closing.
    connection.closeUnderlying(logLevel)
  }

  private def runLockedConnectionCheck(now: CantonTimestamp): Unit = {
    import TraceContext.Implicits.Empty.*

    FutureUnlessShutdownUtil.doNotAwaitUnlessShutdown(
      synchronizeWithClosing("check-locked-connection")(checkHealth(now)),
      "check-locked-connection",
      closeContext = Some(closeContext),
    )
  }

  private def scheduleCheckConnection(now: CantonTimestamp): Unit = {
    // Add a jitter of 20% of the health check period to the scheduled connection check time to scatter the checks, such that they don't run at the same time
    val period = config.healthCheckPeriod.asJava
    val jitter = Duration.ofMillis(PseudoRandom.randomLong((period.toMillis * 0.2).toLong))

    val checkAt = now.add(config.healthCheckPeriod.asJava).add(jitter)

    logger.trace(s"At $now schedule next health check for $checkAt")(TraceContext.empty)

    clock
      .scheduleAt(runLockedConnectionCheck, checkAt)
      .discard
  }

  private def checkHealth(now: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    def checkLockedConnection(now: CantonTimestamp): Unit = {
      val state = stateRef.get()
      state match {
        case State.Init =>
          logger.debug("Building initial DB connection and lock")
          buildLockedConnection(State.Init)

        case State.SetPassive =>
          logger.debug("Connection was closed to become passive, trying to become active again")
          buildLockedConnection(State.SetPassive)

        case State.Connected(connection, lock) =>
          logger.trace(s"Checking if DB locked connection $connection and $lock is healthy at $now")

          // Check the health of the DB connection and that the lock was not lost
          val isValid =
            checkConnection(
              connection,
              checkReadOnly,
              config.connectionTimeout.toInternal,
              timeouts,
              logger,
            ) && !lock.isLost

          if (!isValid) {
            // Connection and/or lock was lost thus setting the connected state to lost
            val prevState = stateRef.getAndUpdate {
              case _: State.Connected => State.Lost
              case s => s
            }

            prevState match {
              case _: State.Connected if writeExecutor.isShuttingDown =>
                logger.debug(
                  s"Locked connection was lost but write executor is shutting down. Skipping re-connection."
                )
              case _: State.Connected =>
                logger.warn(s"Locked connection was lost, trying to rebuild")
                logger.debug(s"Trying to close defunct connection $connection before recovery")
                // Try to close the defunct connection and lock before rebuilding the pair
                closeLockedConnection(connection, lock, Level.DEBUG)
                buildLockedConnection(State.Lost)
              case State.Disconnecting | State.Disconnected =>
                logger.debug(s"Skipping connection recovery due to closing")
              case State.Init | State.Lost | State.Recovering | State.SetPassive =>
                ErrorUtil.invalidState(s"Invalid state for recovery: $prevState")
            }
          } else {
            logger.trace("Locked connection is healthy")
            scheduleCheckConnection(clock.now)
          }

        case State.Disconnecting | State.Disconnected =>
          if (isClosing)
            logger.debug(s"Skipping connection check due to closing")
          else
            ErrorUtil.invalidState(s"Closing state $state for connection but closing flag not set")

        case State.Lost | State.Recovering =>
          ErrorUtil.invalidState(s"Invalid state for connection check: $state")
      }
    }

    execQueue.execute(Future(checkLockedConnection(now)), "check-health")
  }

  /** Returns true if the connection is valid and the lock is held.
    *
    * It actually performs the checks instead of returning a cached result from the periodic health
    * checks.
    */
  private[resource] def isActiveNow()(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, Boolean] =
    stateRef.get() match {
      case State.Connected(connection, lock) =>
        for {
          hasLock <- lock.hasLock.leftMap(err => s"Failed to check lock: $err")
          isValid = checkConnection(
            connection,
            checkReadOnly,
            config.connectionTimeout.toInternal,
            timeouts,
            logger,
          )
        } yield hasLock && isValid
      case _ => EitherT.rightT(false)
    }

  // Run the initial connection check
  runLockedConnectionCheck(clock.now)

  /** Returns a connection only if the connection is available and the lock has been acquired.
    *
    * The consumer should get a connection when needed and not cache the returned connection to
    * ensure that the connection is healthy when used.
    */
  def get: Either[DbLockedConnectionError, KeepAliveConnection] =
    stateRef.get() match {
      case State.Connected(connection, lock) =>
        if (lock.isAcquired) Right(connection)
        else Left(DbLockedConnectionError.DbLockNotAcquired)
      case state => Left(DbLockedConnectionError.DbConnectionNotAvailable(state))
    }

  def isActive: Boolean = get.isRight

  /** Returns true if node became passive and another replica became active. Returns false if no
    * other replica took the lock and the node should remain active.
    */
  def setPassive()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    def setPassiveInternal() =
      stateRef.get() match {
        case State.Connected(connection, lock) if lock.isAcquired =>
          logger.info(
            s"Closing the connection $connection to release the lock $lock to become passive"
          )

          // Close the main connection to release the lock
          closeLockedConnection(connection, lock, Level.INFO)

          transitionOrFail[State.Connected](State.SetPassive)

          logger.info(s"Waiting for the other replica to try to become active")

          // Wait for twice the passive check period to give the other replica a chance to acquire the lock
          Threading.sleep((config.passiveCheckPeriod * 2).asJava.toMillis)

          Either.unit
        case s =>
          Left(s"Connection is not connected or active: $s")
      }

    // Run health check and set passive through the execution queue to ensure they don't interfere
    execQueue.executeEUS(EitherT.fromEither(setPassiveInternal()), "set-passive")
  }

  override def onClosed(): Unit = {
    def getConnectedOrRecovering(
        state: State
    ): Option[Either[State.Recovering.type, State.Connected]] = state match {
      case c: State.Connected => Some(Right(c))
      case State.Recovering => Some(Left(State.Recovering))
      case _ => None
    }

    def closeConnectionWarn(warnState: State)(implicit traceContext: TraceContext): Unit =
      logger.debug(s"Not closing connection as connection is $warnState")

    TraceContext.withNewTraceContext("close_locked_connection") { implicit traceContext =>
      logger.debug(s"Closing DB-locked connection")

      transitionEither[Unit, State.Recovering.type, State.Connected](
        getConnectedOrRecovering(_),
        State.Disconnecting,
        closeConnectionWarn,
      )
        .foreach {
          case Right(connected) =>
            closeLockedConnection(connected.connection, connected.lock, Level.INFO)
            transitionOrFail(State.Disconnecting, State.Disconnected)
            logger.debug(s"Closed DB-locked connection: ${connected.connection}")
          case Left(State.Recovering) =>
            transitionOrFail(State.Disconnecting, State.Disconnected)
            logger.debug(s"Recovering connection is now disconnected")
        }
    }
  }

}

object DbLockedConnection {

  sealed trait State extends Product with Serializable with PrettyPrinting
  object State {

    // The initial state
    case object Init extends State {
      override protected def pretty: Pretty[Init.type] = prettyOfObject[Init.type]
    }

    // The healthy state with a connection and db lock
    final case class Connected(connection: KeepAliveConnection, lock: DbLock) extends State {
      override protected def pretty: Pretty[Connected] =
        prettyOfClass(param("connection", _.connection), param("lock", _.lock))
    }

    // The database and connection is explicitly being closed
    case object Disconnecting extends State {
      override protected def pretty: Pretty[Disconnecting.type] = prettyOfObject[Disconnecting.type]

    }
    case object Disconnected extends State {
      override protected def pretty: Pretty[Disconnected.type] = prettyOfObject[Disconnected.type]
    }

    // The connection of the database has been lost and needs to be recovered
    case object Lost extends State {
      override protected def pretty: Pretty[Lost.type] = prettyOfObject[Lost.type]
    }
    case object Recovering extends State {
      override protected def pretty: Pretty[Recovering.type] = prettyOfObject[Recovering.type]
    }

    // The connection was gracefully closed to become passive
    case object SetPassive extends State {
      override protected def pretty: Pretty[SetPassive.type] = prettyOfObject[SetPassive.type]
    }
  }

  /** Returns true if the connection is valid. */
  @SuppressWarnings(Array("org.wartremover.warts.TryPartial"))
  private def checkConnection(
      connection: KeepAliveConnection,
      checkReadOnly: KeepAliveConnection => EitherT[Future, String, Boolean],
      connectionTimeout: PositiveFiniteDuration,
      timeouts: ProcessingTimeout,
      logger: TracedLogger,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
      errorLoggingContext: ErrorLoggingContext,
  ): Boolean =
    if (connection.markInUse()) {

      logger.trace(s"Checking if connection $connection is valid")

      val isValid =
        try {
          blocking {
            // isValid only throws when the provided timeout is < 0
            checked {
              connection.isValid(connectionTimeout.toSecondsTruncated(logger).unwrap)
            }
          }
        } finally {
          // Although isValid should not throw any exception with a timeout >= 0, make sure that we will always mark the connection as free again
          connection.markFree()
        }

      if (isValid) {
        logger.trace(s"Connection $connection is valid, checking if connection is read-only")

        // If the connection is valid, further check that the connection is not read-only
        val isReadOnly = Try {
          timeouts.network.await("connection check read-only") {
            checkReadOnly(connection).valueOr { err =>
              logger.debug(s"Failed to check if connection is read-only: $err")
              // Assume connection is NOT read-only, e.g., due to contention
              false
            }
          }
        }.recover { case _: TimeoutException =>
          logger.debug(s"Read-only check timed out, assuming connection is not read-only")
          false
        }.get // Currently exceptions of the read-only check are returned as Left's and the timeout exception explicitly handled above

        if (isReadOnly)
          logger.info(s"Connection $connection is read-only")
        else
          logger.trace(s"Connection $connection is not read-only")

        !isReadOnly
      } else {
        logger.info(s"Connection $connection is NOT valid")
        false
      }
    } else {
      logger.trace(s"Skip connection $connection check because the connection is in use")
      true
    }

  private[resource] def awaitConnection(
      connection: DbLockedConnection,
      maxRetries: Int,
      waitInMs: Long,
      stop: Option[DbLockedConnectionError => Boolean],
      logger: TracedLogger,
  )(implicit
      traceContext: TraceContext,
      closeContext: CloseContext,
  ): EitherT[UnlessShutdown, DbLockedConnectionError, Connection] =
    RetryEither.retry[DbLockedConnectionError, Connection](
      maxRetries,
      waitInMs,
      functionFullName,
      stop,
      retryLogLevel = Level.DEBUG,
      failLogLevel = Level.INFO,
    ) {
      connection.get
    }(ErrorLoggingContext.fromTracedLogger(logger), closeContext)

  private[resource] def awaitActive(
      connection: DbLockedConnection,
      maxRetries: Int,
      waitInMs: Long,
      logger: TracedLogger,
  )(implicit
      traceContext: TraceContext,
      closeContext: CloseContext,
  ): EitherT[UnlessShutdown, DbLockedConnectionError, Unit] =
    awaitConnection(connection, maxRetries, waitInMs, None, logger).map(_ => ())

  /** Awaits for the locked connection to be either active or passive. */
  private[resource] def awaitInitialized(
      connection: DbLockedConnection,
      maxRetries: Int,
      waitInMs: Long,
      logger: TracedLogger,
  )(implicit traceContext: TraceContext, closeContext: CloseContext): Unit = {
    def successOnPassive(error: DbLockedConnectionError): Boolean = error match {
      case DbLockedConnectionError.DbLockNotAcquired => true
      case _: DbLockedConnectionError.DbConnectionNotAvailable => false
    }

    awaitConnection(connection, maxRetries, waitInMs, Some(successOnPassive), logger).discard
  }

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private[resource] def createDataSource(
      baseDbConfig: Config,
      poolSize: Int,
      connectionTimeout: PositiveFiniteDuration,
  )(implicit errorLoggingContext: ErrorLoggingContext): Either[String, DataSourceJdbcDataSource] = {
    import slick.util.ConfigExtensionMethods.*

    val logger = errorLoggingContext.logger

    for {
      // Sanity check that the user has not configured connection pooling
      _ <- Either.cond(
        !baseDbConfig
          .getStringOpt("dataSourceClassName")
          .orElse(baseDbConfig.getStringOpt("dataSourceClass"))
          .contains("org.postgresql.ds.PGConnectionPoolDataSource"),
        (),
        s"The data source should not be configured with connection pooling, use `org.postgresql.ds.PGSimpleDataSource` instead",
      )

      dbConfig = ConfigFactory
        .parseMap(
          Map[String, Any](
            "keepAliveConnection" -> false, // We use our own keep alive connections
            "maxConnections" -> poolSize,
            "connectionPool" -> "disabled", // Explicitly disable HikariCP to have full control over the connections
          ).asJava
        )
        .withFallback(baseDbConfig)

      ds = JdbcDataSource.forConfig(
        c = dbConfig,
        driver = null,
        name = "",
        classLoader = ClassLoaderUtil.defaultClassLoader,
      )
    } yield {
      // Setup the data source
      ds match {
        case jdbcDS: DataSourceJdbcDataSource =>
          jdbcDS.ds.setLoginTimeout(
            connectionTimeout.toSecondsTruncated(logger)(errorLoggingContext.traceContext).unwrap
          )
          jdbcDS
        case invalidDS =>
          ErrorUtil.invalidState(s"Got invalid datasource from configuration: $invalidDS")
      }
    }
  }

  private[resource] def create(
      profile: DbStorage.Profile with DbStorage.DbLockSupport,
      ds: DataSourceJdbcDataSource, // We don't use a HikariCP data source to have full control over the connections
      lockId: DbLockId,
      lockMode: DbLockMode,
      connectionConfig: DbLockedConnectionConfig,
      isMainConnection: Boolean,
      timeouts: ProcessingTimeout,
      exitOnFatalFailures: Boolean,
      clock: Clock,
      loggerFactory: NamedLoggerFactory,
      futureSupervisor: FutureSupervisor,
      writeExecutor: AsyncExecutorWithShutdown,
      logLockOwnersOnLockAcquisitionAttempt: Boolean,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): DbLockedConnection = {

    val logger = loggerFactory.getLogger(DbLockedConnection.getClass)
    val tracedLogger = TracedLogger(logger)

    def initConnection(connection: KeepAliveConnection): EitherT[Future, String, Unit] = for {
      _ <- Either
        .catchOnly[SQLException](connection.setAutoCommit(true))
        .leftMap(err => show"Failed to set autocommit to true: $err")
        .toEitherT[Future]
      _ <- profile match {
        case _: Profile.Postgres =>
          import slick.jdbc.canton.ActionBasedSQLInterpolation.Implicits.*
          val db = KeepAliveConnection.createDatabaseFromConnection(
            connection,
            logger,
            writeExecutor,
          )

          // Set an explicit network timeout on the main connection as we do not run any (long) user queries on that connection
          if (isMainConnection)
            connection.setNetworkTimeout(
              Threading.directExecutionContext(logger),
              connectionConfig.healthCheckTimeout.duration.toMillis.toInt,
            )

          val keepAliveIdle =
            connectionConfig.keepAliveIdle.toInternal.toSecondsTruncated(tracedLogger).unwrap
          val keepAliveInterval =
            connectionConfig.keepAliveInterval.toInternal.toSecondsTruncated(tracedLogger).unwrap
          val keepAliveCount = connectionConfig.keepAliveCount
          val setKeepAliveSettings = DBIO
            .seq(
              sqlu"SET tcp_keepalives_idle TO #$keepAliveIdle",
              sqlu"SET tcp_keepalives_interval TO #$keepAliveInterval",
              sqlu"SET tcp_keepalives_count TO #$keepAliveCount",
            )

          EitherTUtil.fromFuture(
            db.run(setKeepAliveSettings),
            err => show"Failed to initialize new connection: $err",
          )
      }
    } yield ()

    def createConnection(): EitherT[Future, String, KeepAliveConnection] = {
      implicit val errorLoggingContext: ErrorLoggingContext =
        ErrorLoggingContext.fromTracedLogger(tracedLogger)

      for {
        newConn <- EitherT {
          Future {
            blocking {
              handling(classOf[EOFException], classOf[SQLException])
                .by(Left(_))
                .apply(Right(ds.createConnection()))
                .map(conn => new KeepAliveConnection(conn))
                .leftMap(err => show"Failed to create connection: $err")
            }
          }
        }

        // Ensure the new connection is valid
        _ <- EitherTUtil
          .condUnitET[Future](
            checkConnection(
              newConn,
              checkReadOnly,
              connectionConfig.connectionTimeout.toInternal,
              timeouts,
              tracedLogger,
            ), {
              // Attempt to close the new invalid connection
              newConn.closeUnderlying(Level.DEBUG)(errorLoggingContext)
              "New connection was not valid"
            },
          )

        // Initialize the new connection
        _ <- initConnection(newConn)
      } yield newConn
    }

    def createLock(connection: KeepAliveConnection): DbLock =
      DbLock
        .create(
          profile,
          connection,
          connectionConfig.lock,
          lockId,
          lockMode,
          timeouts,
          clock,
          loggerFactory,
          writeExecutor,
        )

    def checkReadOnly(connection: KeepAliveConnection): EitherT[Future, String, Boolean] =
      profile match {
        case Profile.Postgres(jdbc) =>
          import slick.jdbc.canton.ActionBasedSQLInterpolation.Implicits.*
          val db = KeepAliveConnection.createDatabaseFromConnection(
            connection,
            logger,
            writeExecutor,
          )

          import profile.DbStorageAPI.jdbcActionExtensionMethods

          val readOnlyQuery =
            sql"show transaction_read_only"
              .as[String]
              .headOption
              .withStatementParameters(statementInit =
                _.setQueryTimeout(
                  connectionConfig.healthCheckTimeout.toInternal
                    .toSecondsTruncated(tracedLogger)
                    .unwrap
                )
              )

          EitherTUtil
            .fromFuture(
              db.run(readOnlyQuery),
              err => show"Failed to check new connection for read-only: $err",
            )
            .map(_.map(_.toLowerCase).contains("on"))
      }

    if (ds.keepAliveConnection) {
      logger.warn(s"DataSource should not be configured with keep-alive")
    }

    new DbLockedConnection(
      createConnection _,
      createLock,
      connectionConfig,
      checkReadOnly,
      lockId,
      exitOnFatalFailures,
      clock,
      timeouts,
      loggerFactory,
      futureSupervisor,
      writeExecutor,
      logLockOwnersOnLockAcquisitionAttempt,
    )
  }

}

sealed trait DbLockedConnectionError extends Product with Serializable with PrettyPrinting
object DbLockedConnectionError {
  case object DbLockNotAcquired extends DbLockedConnectionError {
    override protected def pretty: Pretty[DbLockNotAcquired.type] =
      prettyOfObject[DbLockNotAcquired.type]
  }
  final case class DbConnectionNotAvailable(state: DbLockedConnection.State)
      extends DbLockedConnectionError {
    override protected def pretty: Pretty[DbConnectionNotAvailable] = prettyOfClass(
      unnamedParam(_.state)
    )
  }
}
