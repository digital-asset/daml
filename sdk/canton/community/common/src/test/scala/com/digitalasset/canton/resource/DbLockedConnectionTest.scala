// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.resource

import com.digitalasset.canton.config.{DbLockedConnectionConfig, DefaultProcessingTimeouts}
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.store.db.*
import com.digitalasset.canton.time.{Clock, PositiveFiniteDuration, SimClock}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, CloseableTest, HasExecutorService}
import org.scalatest.FutureOutcome
import org.scalatest.wordspec.FixtureAsyncWordSpec
import org.slf4j.event.Level
import slick.util.AsyncExecutorWithMetrics

trait DbLockedConnectionTest
    extends FixtureAsyncWordSpec
    with BaseTest
    with HasExecutorService
    with CloseableTest {

  protected def setup: DbStorageSetup

  private lazy val ds =
    DbLockedConnection
      .createDataSource(
        setup.storage.dbConfig.config,
        poolSize = 1,
        connectionTimeout = PositiveFiniteDuration.tryOfSeconds(10),
      )
      .valueOrFail("Failed to create datasource")

  private lazy val executor = AsyncExecutorWithMetrics.createSingleThreaded(
    "DbLockedConnectionTest",
    noTracingLogger,
  )

  case class FixtureParam(
      connection: DbLockedConnection,
      lockId: DbLockId,
      clock: SimClock,
      config: DbLockedConnectionConfig,
  )

  // Throws an exception if the connection is not healthy/available
  protected def checkHealth(connection: DbLockedConnection): Unit =
    connection.get.valueOrFail(s"Connection $connection not available")

  protected def awaitHealthy(connection: DbLockedConnection): Unit =
    eventually() {
      checkHealth(connection)
    }

  protected def createConnection(
      connId: String,
      lockId: DbLockId,
      lockMode: DbLockMode,
      clock: Clock,
      config: DbLockedConnectionConfig,
  ): DbLockedConnection =
    DbLockedConnection
      .create(
        DbLock
          .isSupported(setup.storage.profile)
          .valueOrFail("Storage profile does not support DB locks"),
        ds,
        lockId,
        lockMode,
        config.copy(passiveCheckPeriod = PositiveFiniteDuration.tryOfSeconds(1).toConfig),
        isMainConnection = true,
        DefaultProcessingTimeouts.testing,
        exitOnFatalFailures = false,
        clock,
        loggerFactory.append("connId", connId),
        futureSupervisor,
        executor,
        logLockOwnersOnLockAcquisitionAttempt = true,
      )(executorService, TraceContext.empty)

  protected def allocateLockId(lockCounter: DbLockCounter): DbLockId

  protected def freshLockId(): DbLockId = allocateLockId(UniqueDbLockCounter.get())

  def withFixture(test: OneArgAsyncTest): FutureOutcome = {
    val clock = new SimClock(loggerFactory = loggerFactory)
    val lockId = allocateLockId(UniqueDbLockCounter.get())
    val config = DbLockedConnectionConfig()
    val lockedConnection = createConnection("fixture", lockId, DbLockMode.Exclusive, clock, config)
    complete {
      withFixture(test.toNoArgAsyncTest(FixtureParam(lockedConnection, lockId, clock, config)))
    } lastly {
      lockedConnection.close()
    }
  }

  override def afterAll(): Unit =
    try {
      ds.close()
      executor.close()
      setup.close()
    } finally super.afterAll()

  "DbLockedConnection" can {

    "create a connection and acquire an exclusive lock" in { f =>
      awaitHealthy(f.connection)
      succeed
    }

    "create multiple connections with a shared lock" in { f =>
      val lockId = freshLockId()

      val connections = Range(0, 10).map { idx =>
        createConnection(s"pool-$idx", lockId, DbLockMode.Shared, f.clock, f.config)
      }

      // Eventually all connections should become healthy
      connections.foreach(awaitHealthy)

      connections.foreach(_.close())

      succeed
    }

    "create multiple connections for an exclusive lock id, only one is active" in { f =>
      awaitHealthy(f.connection)

      // Create a second connection for the same lock id and exclusive mode
      val conn = createConnection(s"conflict", f.lockId, DbLockMode.Exclusive, f.clock, f.config)

      DbLockedConnection.awaitInitialized(conn, 50, 200, logger)
      assert(!conn.isActive)

      // Trigger a health check and ensure the initial connection is active, the second is still passive
      f.clock.advance(f.config.healthCheckPeriod.asJava)
      assert(f.connection.isActive)
      assert(!conn.isActive)

      // Close the active connection
      f.connection.close()
      f.clock.advance(
        (f.config.healthCheckPeriod * 2).asJava
      )
      // Observe the second connection becoming active instead
      eventually() {
        assert(conn.isActive)
      }

      // Create a third connection
      val conn2 = createConnection(s"conflict", f.lockId, DbLockMode.Exclusive, f.clock, f.config)
      DbLockedConnection.awaitInitialized(conn2, 50, 200, logger)
      assert(!conn2.isActive)

      // Close the second connection
      conn.close()
      f.clock.advance(
        (f.config.healthCheckPeriod * 2).asJava
      )

      // Observe the third becomes active
      eventually() {
        assert(conn2.isActive)
      }

      conn2.close()
      succeed
    }

    "have a lock owner" in { f =>
      var pid: Option[Long] = None

      awaitHealthy(f.connection)

      val conn = loggerFactory.assertEventuallyLogsSeq(SuppressionRule.Level(Level.DEBUG))(
        {
          val conn =
            createConnection(s"lock-owners", f.lockId, DbLockMode.Exclusive, f.clock, f.config)
          DbLockedConnection.awaitInitialized(conn, 50, 200, logger)
          conn
        },
        entries => {
          val lockAcquiredByEntry =
            entries.find(_.debugMessage.contains("Failed to acquire lock"))
          pid = lockAcquiredByEntry.flatMap { logEntry =>
            """.*own it: ([0-9]*)""".r
              .findFirstMatchIn(logEntry.debugMessage)
              .map(_.group(1).toLong)
          }
          pid shouldBe defined
        },
      )

      var newPid: Option[Long] = None
      loggerFactory.assertEventuallyLogsSeq(SuppressionRule.Level(Level.DEBUG))(
        {
          // Now we close the fixture connection, and we should see `conn` logging that it acquired the lock
          // along with the pid it got assigned
          f.connection.close()
          eventually() {
            assert(conn.isActive)
          }
        },
        entries => {
          val lockAcquiredByEntry =
            entries.find(_.debugMessage.contains("Lock successfully acquired"))
          newPid = lockAcquiredByEntry.flatMap { logEntry =>
            """.*own it: ([0-9]*)""".r
              .findFirstMatchIn(logEntry.debugMessage)
              .map(_.group(1).toLong)
          }
          newPid shouldBe defined
        },
      )
      conn.close()
      succeed
    }

    "recover when connection is closed" in { f =>
      awaitHealthy(f.connection)

      f.connection.get.valueOrFail("Connection not available") match {
        case keepAliveConnection: KeepAliveConnection =>
          logger.debug(s"Cutting DB connection $keepAliveConnection")
          keepAliveConnection.underlying.close()
        case _ => fail("Connection not of type KeepAliveConnection")
      }

      def expectedWarns(lockId: DbLockId) = Seq(
        s"Failed to check database lock status for $lockId, assuming lost",
        s"Lock $lockId was lost",
        "Locked connection was lost, trying to rebuild",
      )

      val prevState = f.connection.state
      val prevConnectedState = prevState match {
        case connected: DbLockedConnection.State.Connected => connected
        case s => fail(s"Locked connection must be connected, but state is $s")
      }

      loggerFactory.assertLogsSeqString(
        SuppressionRule.LevelAndAbove(Level.WARN),
        expectedWarns(prevConnectedState.lock.lockId),
      ) {
        eventually() {
          // Add 1 second to the health check period to account for schedule jitter
          // Continuously advance time to make sure that both the DB lock check has run and a follow-up connection check
          f.clock.advance(f.config.healthCheckPeriod.plusSeconds(1).asJava)
          checkHealth(f.connection)
          f.connection.state shouldNot be(prevState)
        }
      }

    }
  }

}

class DbLockedConnectionTestPostgres extends DbLockedConnectionTest {

  override protected lazy val setup: PostgresDbStorageSetup =
    DbStorageSetup.postgres(loggerFactory)(executorService)

  override protected def allocateLockId(lockCounter: DbLockCounter): DbLockId =
    PostgresDbLock
      .allocateLockId(setup.config, lockCounter)(loggerFactory)
      .valueOrFail(s"Failed to allocate lock id for $lockCounter")
}
