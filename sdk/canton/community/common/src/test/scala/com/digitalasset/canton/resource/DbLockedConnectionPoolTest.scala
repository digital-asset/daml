// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.resource

import cats.syntax.either.*
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{DbLockedConnectionPoolConfig, DefaultProcessingTimeouts}
import com.digitalasset.canton.logging.{NamedLoggerFactory, SuppressionRule}
import com.digitalasset.canton.resource.DbLockedConnection.State
import com.digitalasset.canton.store.db.*
import com.digitalasset.canton.time.{Clock, SimClock, WallClock}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, CloseableTest, HasExecutorService}
import org.scalatest.wordspec.FixtureAsyncWordSpec
import org.scalatest.{Assertion, FutureOutcome}
import org.slf4j.event.Level
import slick.util.{AsyncExecutorWithMetrics, AsyncExecutorWithShutdown}

import java.sql.SQLTransientException
import java.util.concurrent.Executors
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future, Promise}

trait DbLockedConnectionPoolTest
    extends FixtureAsyncWordSpec
    with BaseTest
    with HasExecutorService
    with CloseableTest {

  protected def setup: DbStorageSetup

  private lazy val executor = AsyncExecutorWithMetrics.createSingleThreaded(
    "DbLockedConnectionPoolTest",
    noTracingLogger,
  )

  case class FixtureParam(
      pool: DbLockedConnectionPool,
      config: DbLockedConnectionPoolConfig,
      mainLockCounter: DbLockCounter,
      poolLockCounter: DbLockCounter,
      poolSize: PositiveInt,
      clock: SimClock,
      loggerFactory: NamedLoggerFactory,
  )

  protected def createConnectionPool(
      config: DbLockedConnectionPoolConfig,
      poolSize: PositiveInt,
      clock: Clock,
      poolId: String,
      mainLockCounter: DbLockCounter,
      poolLockCounter: DbLockCounter,
      loggerFactory: NamedLoggerFactory,
      writeExecutor: Option[AsyncExecutorWithShutdown] = None,
  ): DbLockedConnectionPool =
    DbLockedConnectionPool
      .create(
        DbLock
          .isSupported(setup.storage.profile)
          .valueOrFail(s"Storage profile `${setup.storage.profile}` does not support DB locks"),
        setup.storage.dbConfig,
        config,
        poolSize,
        mainLockCounter,
        poolLockCounter,
        clock,
        DefaultProcessingTimeouts.testing,
        exitOnFatalFailures = true,
        futureSupervisor,
        loggerFactory.append("poolId", poolId),
        writeExecutor.getOrElse(executor),
      )(executorService, TraceContext.empty, closeContext)
      .valueOrFail("Failed to create connection pool")

  protected def awaitActive(
      pool: DbLockedConnectionPool,
      config: DbLockedConnectionPoolConfig,
      clock: SimClock,
  ): Assertion =
    eventually(timeUntilSuccess = 60.seconds) {
      clock.advance(config.healthCheckPeriod.asJava)
      val active = pool.isActive
      logger.debug(s"Checking if pool is active... $active")
      assert(active)
    }

  protected def awaitActive(f: FixtureParam): Assertion = awaitActive(f.pool, f.config, f.clock)

  def withFixture(test: OneArgAsyncTest): FutureOutcome = {
    val fixtureLoggerFactory =
      loggerFactory.append("case", test.pos.map(_.lineNumber.toString).getOrElse("unknown"))
    val clock = new SimClock(loggerFactory = fixtureLoggerFactory)
    val config = DbLockedConnectionPoolConfig()
    val poolSize =
      PositiveInt.tryCreate(Math.max(Threading.detectNumberOfThreads(noTracingLogger).value / 4, 1))
    val mainLockCounter = UniqueDbLockCounter.get()
    val poolLockCounter = UniqueDbLockCounter.get()
    val pool = createConnectionPool(
      config,
      poolSize,
      clock,
      "fixture",
      mainLockCounter,
      poolLockCounter,
      fixtureLoggerFactory,
    )
    complete {
      withFixture(
        test.toNoArgAsyncTest(
          FixtureParam(
            pool,
            config,
            mainLockCounter,
            poolLockCounter,
            poolSize,
            clock,
            fixtureLoggerFactory,
          )
        )
      )
    } lastly {
      pool.close()
      clock.close()
    }
  }

  override def afterAll(): Unit =
    try {
      executor.close()
      setup.close()
    } finally super.afterAll()

  "DbLockedConnectionPool" can {

    "create a connection pool and become active" in { f =>
      awaitActive(f)
    }

    "create two connection pools and one becomes active" in { f =>
      awaitActive(f)

      logger.debug(s"Creating a second connection pool")
      val pool2 = createConnectionPool(
        f.config,
        f.poolSize,
        f.clock,
        "pool2",
        f.mainLockCounter,
        f.poolLockCounter,
        f.loggerFactory,
      )
      assert(!pool2.isActive)

      logger.debug(s"Closing first (active) connection pool")
      f.pool.close()
      logger.debug(s"First (active) connection pool is being closed")

      awaitActive(pool2, f.config, f.clock)
      pool2.close()

      succeed
    }

    "when provided, use write executor to schedule queries" in { f =>
      // Close the fixture pool and create a new one with a non empty write executor
      awaitActive(f)
      logger.debug(s"Closing active connection pool")
      f.pool.close()
      val executor = Executors.newSingleThreadExecutor()
      val lockExecutionContext = ExecutionContext.fromExecutor(executor)
      val asyncExecutor = new AsyncExecutorWithShutdown {
        override def executionContext: ExecutionContext = lockExecutionContext
        override def close(): Unit = executor.shutdown()
        override def isShuttingDown: Boolean = false
      }

      val pool2 = createConnectionPool(
        f.config,
        f.poolSize,
        f.clock,
        "pool2",
        f.mainLockCounter,
        f.poolLockCounter,
        f.loggerFactory,
        Some(asyncExecutor),
      )
      awaitActive(pool2, f.config, f.clock)

      val connections = pool2.getPool.value

      val p = Promise[Unit]()
      val mockFuture = Future(timeouts.default.await("Mock query")(p.future))(lockExecutionContext)
      val hasLockFuture = connections.head.state match {
        case State.Connected(_, lock) => lock.hasLock.value
        case _ => fail("Expected connected connection")
      }

      // The lock check can't complete because the mock query is running on the single threaded write executor
      always()(hasLockFuture.isCompleted shouldBe false)
      p.success(())

      timeouts.default.await("Wait for mock query")(mockFuture)
      timeouts.default.await("Wait for lock")(hasLockFuture)

      pool2.close()

      succeed
    }

    "fail-over on main connection close" in { f =>
      awaitActive(f)

      val clock2 = new WallClock(DefaultProcessingTimeouts.testing, loggerFactory)
      val pool2 = createConnectionPool(
        f.config,
        f.poolSize,
        clock2,
        "pool2",
        f.mainLockCounter,
        f.poolLockCounter,
        f.loggerFactory,
      )
      assert(!pool2.isActive)

      val mainConnection =
        f.pool.mainConnection.get.valueOrFail("Cannot get main connection of active pool")

      logger.debug("Close main connection of active pool")
      mainConnection
        .closeUnderlying(Level.WARN)

      logger.debug("Wait until passive pool acquires main connection lock")
      eventually() {
        assert(pool2.mainConnection.isActive)
      }

      def expectedWarns(lockId: DbLockId) = Seq(
        s"Failed to check database lock status for $lockId, assuming lost",
        s"Lock $lockId was lost",
        "Locked connection was lost, trying to rebuild",
      )

      logger.debug("Wait until former active pool becomes passive")
      loggerFactory.assertLogsSeqString(
        SuppressionRule.LevelAndAbove(Level.WARN),
        expectedWarns(f.pool.mainConnection.lockId),
      ) {
        eventually() {
          // Continuously advance time to make sure that the lock, connection, and pool checks have ran and picked up the updated health from sub-resources
          f.clock.advance(f.config.healthCheckPeriod.asJava)

          // Ensure the pool went passive
          assert(f.pool.isPassive)

          // Ensure the main connection got replaced
          f.pool.mainConnection.state match {
            case State.Connected(connection, lock)
                if connection != mainConnection && !lock.isAcquired =>
              succeed
            case _ => fail("connection is not yet passive")
          }
        }
      }

      logger.debug("Wait until new pool becomes active")
      eventually() {
        // Eventually the second pool is active
        assert(pool2.isActive)
      }

      pool2.close()
      clock2.close()

      succeed
    }

    "connection pool must mark connections as in-use" in { f =>
      awaitActive(f)

      val conns = Range(0, f.poolSize.unwrap).map { _ =>
        // Not all the connections in the pool might have been ready, so retry here
        eventually() {
          Either
            .catchOnly[SQLTransientException](f.pool.createConnection())
            .valueOrFail("Could not create connection")
        }
      }

      // We are out of connections
      assertThrows[SQLTransientException] {
        f.pool.createConnection()
      }

      // Free one of the initial connections
      conns.headOption.foreach(_.close())

      // Creating a new connection should succeed now
      f.pool.createConnection()

      succeed
    }
  }
}

class DbLockedConnectionPoolTestPostgres extends DbLockedConnectionPoolTest {

  override protected lazy val setup: PostgresDbStorageSetup =
    DbStorageSetup.postgres(loggerFactory)(executorService)
}
