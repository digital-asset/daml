// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.resource

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{
  DbLockedConnectionConfig,
  DbLockedConnectionPoolConfig,
  DefaultProcessingTimeouts,
  PositiveFiniteDuration,
}
import com.digitalasset.canton.lifecycle.{
  CloseContext,
  FlagCloseable,
  FutureUnlessShutdown,
  HasCloseContext,
}
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.metrics.CommonMockMetrics
import com.digitalasset.canton.resource.DbLockedConnection.State
import com.digitalasset.canton.resource.DbStorage.PassiveInstanceException
import com.digitalasset.canton.store.db.*
import com.digitalasset.canton.time.{Clock, SimClock}
import com.digitalasset.canton.util.FutureUtil
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.FixtureAsyncWordSpec
import org.scalatest.{Assertion, BeforeAndAfterAll, FutureOutcome, Suite}
import org.slf4j.event.Level
import slick.dbio.{Effect, NoStream}
import slick.sql.SqlAction

import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.duration.*

trait DbStorageMultiTestBase extends FlagCloseable with HasCloseContext with BeforeAndAfterAll {
  self: Suite with BaseTest with HasExecutionContext =>
  protected def setup: DbStorageSetup

  trait Fixture {
    def storage: DbStorage
  }

  protected val tableName: String

  protected val storageTimeout = 60.seconds

  def createTable(f: Fixture): FutureUnlessShutdown[Unit] = createTable(f.storage)

  def createTable(storage: DbStorage): FutureUnlessShutdown[Unit] = {
    import storage.api.*
    storage.update_(sqlu"create table #$tableName (foo varchar)", functionFullName)
  }

  def dropTable(f: Fixture): FutureUnlessShutdown[Unit] = dropTable(f.storage)

  def dropTable(storage: DbStorage): FutureUnlessShutdown[Unit] = {
    import storage.api.*
    storage.update_(sqlu"drop table #$tableName", functionFullName)
  }

  def writeToTable(f: Fixture, str: String): FutureUnlessShutdown[Unit] =
    writeToTable(f.storage, str)

  def writeToTableQuery(storage: DbStorage, str: String): SqlAction[Int, NoStream, Effect.Write] = {
    import storage.api.*
    sqlu"insert into #$tableName(foo) values ($str)"
  }

  def writeToTable(storage: DbStorage, str: String): FutureUnlessShutdown[Unit] =
    storage.update_(writeToTableQuery(storage, str), functionFullName)

  def writeAndVerify(f: Fixture): FutureUnlessShutdown[Assertion] = writeAndVerify(f.storage)

  def writeAndVerify(storage: DbStorage): FutureUnlessShutdown[Assertion] = {
    import storage.api.*

    val testString = "foobar"
    for {
      _ <- writeToTable(storage, testString)
      result <- storage.query(
        sql"select foo from #$tableName".as[String].headOption,
        functionFullName,
      )
    } yield result.value shouldEqual testString
  }

  def verifyActiveStorage(storage: DbStorage): FutureUnlessShutdown[Assertion] =
    (for {
      _ <- createTable(storage)
      result <- writeAndVerify(storage)
      _ <- dropTable(storage)
    } yield result)

  override def afterAll(): Unit = {
    super.afterAll()
    close()
  }
}

trait DbStorageMultiTest
    extends FixtureAsyncWordSpec
    with BaseTest
    with HasExecutionContext
    with DbStorageMultiTestBase {

  override protected lazy val tableName = "db_storage_multi_pooled_test"

  case class FixtureParam(
      storage: DbStorageMulti,
      clock: SimClock,
      onPassiveCalled: AtomicBoolean,
      mainLockCounter: DbLockCounter,
      poolLockCounter: DbLockCounter,
  ) extends Fixture

  protected def setup: DbStorageSetup

  protected lazy val connectionConfig: DbLockedConnectionConfig = DbLockedConnectionConfig(
    healthCheckPeriod = PositiveFiniteDuration.ofSeconds(1),
    passiveCheckPeriod = PositiveFiniteDuration.ofSeconds(5),
  )

  protected lazy val connectionPoolConfig: DbLockedConnectionPoolConfig =
    DbLockedConnectionPoolConfig(
      connection = connectionConfig,
      healthCheckPeriod = PositiveFiniteDuration.ofSeconds(1),
    )

  protected def createStorage(
      customClock: Option[Clock] = None,
      onActive: () => FutureUnlessShutdown[Unit] = () => FutureUnlessShutdown.unit,
      onPassive: () => FutureUnlessShutdown[Option[CloseContext]] = () =>
        FutureUnlessShutdown.pure(None),
      name: String,
      mainLockCounter: DbLockCounter,
      poolLockCounter: DbLockCounter,
  ): DbStorageMulti = {
    val writePoolSize = PositiveInt.tryCreate(8)
    val readPoolSize = setup.config.numReadConnectionsCanton(
      forParticipant = false,
      withWriteConnectionPool = true,
      withMainConnection = false,
    )
    DbStorageMulti
      .create(
        setup.config,
        connectionPoolConfig,
        readPoolSize,
        writePoolSize,
        mainLockCounter,
        poolLockCounter,
        onActive,
        onPassive,
        CommonMockMetrics.dbStorage,
        None,
        customClock,
        None,
        DefaultProcessingTimeouts.testing,
        exitOnFatalFailures = true,
        futureSupervisor,
        loggerFactory.append("storageId", name),
      )
      .valueOrFailShutdown("create DB storage")
  }

  protected def setBoolean(ref: AtomicBoolean, active: Boolean): () => FutureUnlessShutdown[Unit] =
    () => FutureUnlessShutdown.pure(ref.set(active))

  def withFixture(test: OneArgAsyncTest): FutureOutcome = {
    val clock = new SimClock(loggerFactory = loggerFactory)
    val onPassiveCalled = new AtomicBoolean(false)
    val mainLockCounter = UniqueDbLockCounter.get()
    val poolLockCounter = UniqueDbLockCounter.get()
    val storage =
      createStorage(
        name = "fixture",
        onPassive = () => {
          setBoolean(onPassiveCalled, active = true)().map(_ => None)
        },
        customClock = Some(clock),
        mainLockCounter = mainLockCounter,
        poolLockCounter = poolLockCounter,
      )
    val fixture = FixtureParam(storage, clock, onPassiveCalled, mainLockCounter, poolLockCounter)

    complete {
      withFixture(test.toNoArgAsyncTest(fixture))
    } lastly {
      storage.close()
    }
  }

  def terminateConnection(storage: DbStorageMulti): Unit = {
    val conn = storage.writeConnectionPool.mainConnection.get.valueOrFail("Storage not connected")
    logger.debug(s"Terminate connection $conn")
    // Attempt to close the DB connection of the active replica to trigger a fail-over
    conn.underlying.close()
  }

  def triggerDisconnectCheck(f: FixtureParam): Unit = {

    // When the connection is cut, the db lock, single connection db, and multi storage will each warn
    def expectedWarns(lockId: DbLockId): List[String] = List(
      s"Failed to check database lock status for $lockId, assuming lost",
      s"Lock $lockId was lost",
      "Locked connection was lost, trying to rebuild",
    )

    def getMainLockOrFail: DbLock = f.storage.writeConnectionPool.mainConnection.state match {
      case State.Connected(_connection, lock) => lock
      case s => fail(s"Connection pool not connected: $s")
    }

    val previousLock = getMainLockOrFail

    loggerFactory.assertLogsSeqString(
      SuppressionRule.LevelAndAbove(Level.WARN),
      expectedWarns(previousLock.lockId),
    ) {
      eventually(storageTimeout) {
        // Advance the time to trigger a connection check
        f.clock.advance(connectionPoolConfig.healthCheckPeriod.asJava)

        // Wait until the lock has been replaced with a new lock or set to None
        val lock = getMainLockOrFail
        assert(lock != previousLock)

        assert(!f.storage.isActive)
      }
    }
  }

  protected def createStorageForFailover(
      replicaActive: AtomicBoolean,
      mainLockCounter: DbLockCounter,
      poolLockCounter: DbLockCounter,
  ): DbStorageMulti = {
    val storage = createStorage(
      onActive = setBoolean(replicaActive, active = true),
      onPassive = () => {
        setBoolean(replicaActive, active = false)().map(_ => None)
      },
      name = "failover",
      mainLockCounter = mainLockCounter,
      poolLockCounter = poolLockCounter,
    )

    storage
  }

  protected def withNewStorage[T](
      newStorage: DbStorageMulti
  )(fn: DbStorageMulti => FutureUnlessShutdown[T]): FutureUnlessShutdown[T] =
    FutureUtil
      .logOnFailureUS(fn(newStorage), "failed to run function with new storage")
      .thereafter(_ => newStorage.close())

  protected def awaitActive(f: FixtureParam): Assertion =
    eventually(storageTimeout) {
      f.clock.advance(connectionPoolConfig.healthCheckPeriod.asJava)
      assert(f.storage.isActive)
    }

  "DbStorageMulti" can {

    "run write queries" in { f =>
      awaitActive(f)
      verifyActiveStorage(f.storage).failOnShutdown
    }

    "synchronize two instances" in { f =>
      awaitActive(f)

      val storage1 = f.storage
      import storage1.api.*

      withNewStorage(
        createStorage(
          name = "storage2",
          mainLockCounter = f.mainLockCounter,
          poolLockCounter = f.poolLockCounter,
        )
      ) { storage2 =>
        for {
          _ <- createTable(f)
          testString1 = "foobar"
          testString2 = "barfoo"
          _ <- writeToTable(f, testString1)
          // The insertion with the second storage should fail as it is not active
          failedUpdate = storage2.update(
            writeToTableQuery(storage2, testString2),
            "failing insert",
            maxRetries = 1,
          )
          result1 <- storage1.query(sql"select foo from #$tableName".as[String], "select foo")
          result2 <- storage2.query(sql"select foo from #$tableName".as[String], "select foo")
          _ <- dropTable(f)
        } yield {
          failedUpdate.failOnShutdown.failed.futureValue shouldBe a[PassiveInstanceException]

          // We do not expect the testString2 to have been inserted
          result1 shouldEqual Vector(testString1)
          result2 shouldEqual Vector(testString1)
        }
      }.failOnShutdown
    }

    "graceful fail-over with set passive" in { f =>
      awaitActive(f)

      val replicaActive2 = new AtomicBoolean(false)

      withNewStorage(
        createStorageForFailover(replicaActive2, f.mainLockCounter, f.poolLockCounter)
      ) { storage2 =>
        assert(!storage2.isActive, "New storage must not be active")

        // Initiate the graceful fail-over by setting the active replica to passive
        val setPassiveF = f.storage.setPassive()

        eventually(storageTimeout) {
          // Advance time to trigger health checks on storage1
          f.clock.advance(connectionPoolConfig.healthCheckPeriod.asJava)

          // Eventually the passive replica must have taken over
          assert(storage2.isActive)
        }

        setPassiveF.value.map { _ =>
          // Once the command as been completed, the active replica must be passive
          eventually(storageTimeout) {
            assert(!f.storage.isActive)
          }
        }

      }.failOnShutdown
    }

    "set passive with single replica should fail" in { f =>
      awaitActive(f)

      // Command succeeds but there is no other replica active to take over
      f.storage.setPassive().valueOrFailShutdown("set passive").futureValue

      // Storage must still be active afterwards
      awaitActive(f)
    }

    "fail-over to passive replica" in { f =>
      val storage1 = f.storage

      val replicaActive2 = new AtomicBoolean(false)

      // Wait until storage1 has acquired the lock and is considered active
      awaitActive(f)

      withNewStorage(
        createStorageForFailover(replicaActive2, f.mainLockCounter, f.poolLockCounter)
      ) { storage2 =>
        assert(!storage2.isActive, "New storage must not be active")

        // Terminate the connection of the active storage to the DB
        terminateConnection(storage1)

        // Wait until storage2 main connection is active
        eventually(storageTimeout) {
          assert(storage2.writeConnectionPool.mainConnection.isActive)
        }

        // Trigger disconnect check for previous active storage
        triggerDisconnectCheck(f)

        eventually(storageTimeout) {
          // Advance time to make sure we trigger the connection and lock checks
          f.clock.advance(connectionPoolConfig.healthCheckPeriod.asJava)

          // Eventually the passive replica is active
          assert(storage2.isActive)

          // The onActive for storage2 must have been called
          assert(replicaActive2.get())

          // Eventually the active replica must be marked passive
          assert(!storage1.isActive)

          // The onPassive for storage1 must have been called
          assert(f.onPassiveCalled.get())
        }

        // We can now write using storage2
        verifyActiveStorage(storage2)
      }.failOnShutdown
    }
  }
}

class DbStorageMultiTestPostgres extends DbStorageMultiTest {

  override protected lazy val setup: PostgresDbStorageSetup = DbStorageSetup.postgres(loggerFactory)
}
