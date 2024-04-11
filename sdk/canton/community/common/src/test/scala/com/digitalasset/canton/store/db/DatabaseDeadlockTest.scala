// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.CommunityDbConfig.Postgres
import com.digitalasset.canton.config.DbParametersConfig
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.DbStorageSetup.DbBasicConfig
import com.digitalasset.canton.util.retry.DbRetries
import com.digitalasset.canton.{BaseTestWordSpec, HasExecutionContext}
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{Assertion, BeforeAndAfterAll}
import slick.jdbc.PositionedParameters
import slick.sql.SqlAction

import java.sql.SQLException
import scala.concurrent.Future
import scala.util.{Failure, Random, Success, Try}

trait DatabaseDeadlockTest
    extends BaseTestWordSpec
    with BeforeAndAfterAll
    with HasExecutionContext {
  this: DbTest =>

  lazy val rawStorage: DbStorage = storage.underlying
  import rawStorage.api.*

  val batchSize = 100
  val roundsNegative = 50
  val roundsPositive = 1
  val maxRetries = 3

  implicit override val defaultPatience: PatienceConfig =
    PatienceConfig(timeout = Span(60, Seconds), interval = Span(100, Millis))

  def createTableAction: SqlAction[Int, NoStream, Effect.Write]

  override def beforeAll(): Unit = {
    super.beforeAll()

    rawStorage
      .queryAndUpdate(
        DBIO.seq(
          sqlu"drop table database_deadlock_test".asTry, // Try to drop, in case it already exists.
          createTableAction,
        ),
        functionFullName,
      )
      .futureValue
  }

  override def cleanDb(storage: DbStorage): Future[Unit] =
    rawStorage.update_(
      sqlu"truncate table database_deadlock_test",
      functionFullName,
    )

  case class DbBulkCommand(sql: String, setParams: PositionedParameters => Int => Unit) {
    def run(
        ascending: Boolean,
        maxRetries: Int,
    ): Future[Array[Int]] = {
      rawStorage.queryAndUpdate(
        DbStorage.bulkOperation(
          sql,
          if (ascending) 0 until batchSize else (0 until batchSize).reverse,
          storage.profile,
        )(setParams),
        s"$functionFullName-${sql.take(10)}",
        DbRetries(maxRetries = maxRetries),
      )
    }
  }

  def setIdValue(sp: PositionedParameters)(id: Int): Unit = {
    sp >> id
    sp >> Random.nextInt()
  }
  def setValueId(sp: PositionedParameters)(id: Int): Unit = {
    sp >> Random.nextInt()
    sp >> id
  }
  def setId(sp: PositionedParameters)(id: Int): Unit = {
    sp >> id
  }

  def upsertCommand: DbBulkCommand

  def updateCommand: DbBulkCommand =
    DbBulkCommand("update database_deadlock_test set v = ? where id = ?", setValueId)

  def deleteCommand: DbBulkCommand =
    DbBulkCommand("delete from database_deadlock_test where id = ?", setId)

  "The storage" when {
    // Test upserts
    testQuery("bulk inserts", upsertCommand, upsertCommand)

    testQueryWithSetup(
      // insert rows first to test the update part of the upsert
      setup = upsertCommand.run(ascending = true, 0).futureValue,
      "bulk upserts",
      upsertCommand,
      upsertCommand,
    )

    // Test updates
    testQueryWithSetup(
      setup = upsertCommand.run(ascending = true, 0).futureValue,
      "bulk updates",
      updateCommand,
      updateCommand,
    )

    // Test deletes
    testQuery(
      "bulk delete + insert",
      deleteCommand,
      upsertCommand,
    )
  }

  def testQuery(
      description: String,
      command1: DbBulkCommand,
      command2: DbBulkCommand,
  ): Unit = {
    testQueryWithSetup((), description, command1, command2)
  }

  def testQueryWithSetup(
      setup: => Unit,
      description: String,
      command1: DbBulkCommand,
      command2: DbBulkCommand,
  ): Unit = {
    if (dbCanDeadlock) {
      s"running conflicting $description" can {
        "abort with a deadlock" in {
          setup
          assertSQLException(runWithConflictingRowOrder(command1, command2, 0))
        }
      }
    }

    s"running conflicting $description with retry" must {
      "succeed" in {
        setup
        assertNoException(runWithConflictingRowOrder(command1, command2, maxRetries))
      }
    }
  }

  def dbCanDeadlock: Boolean = true

  def assertSQLException(body: => Try[_]): Assertion = {
    forAtLeast(1, 0 until roundsNegative) { _ =>
      // Note that we can also hit spurious constraint violation errors here, as the query may be MERGE (see UpsertTestOracle).
      // This is no problem as long as there is at least one deadlock.
      inside(body) { case Failure(e: SQLException) =>
        assertDeadlock(e)
      }
    }
  }

  def assertDeadlock(e: SQLException): Assertion

  def assertNoException(body: => Try[_]): Assertion =
    forAll(0 until roundsPositive) { _ =>
      inside(body) { case Success(_) => succeed }
    }

  def runWithConflictingRowOrder(
      command1: DbBulkCommand,
      command2: DbBulkCommand,
      maxRetries: Int,
  ): Try[Seq[Array[Int]]] =
    Future
      .sequence(
        Seq(
          command1.run(ascending = true, maxRetries),
          command2.run(ascending = false, maxRetries),
        )
      )
      .transform(Try(_))
      .futureValue
}

class DatabaseDeadlockTestH2 extends DatabaseDeadlockTest with H2Test {
  import rawStorage.api.*

  // H2 cannot deadlock at the moment, because we are enforcing a single connection.
  // Therefore disabling negative tests.
  override def dbCanDeadlock: Boolean = false

  override lazy val createTableAction: SqlAction[Int, NoStream, Effect.Write] =
    sqlu"create table database_deadlock_test(id bigint primary key, v bigint not null)"

  override lazy val upsertCommand: DbBulkCommand = DbBulkCommand(
    """merge into database_deadlock_test
      |using (select cast(? as bigint) id, cast(? as bigint) v from dual) as input
      |on (database_deadlock_test.id = input.id)
      |when not matched then
      |  insert(id, v)
      |  values(input.id, input.v)
      |when matched then
      |  update set v = input.v""".stripMargin,
    setIdValue,
  )

  override def assertDeadlock(e: SQLException): Assertion = fail("unimplemented")
}

class DatabaseDeadlockTestPostgres extends DatabaseDeadlockTest with PostgresTest {
  import rawStorage.api.*

  override def mkDbConfig(basicConfig: DbBasicConfig): Postgres = {
    // Enforce 8 connections. If there is only one connection, the test will fail to produce deadlocks.
    val defaultDbConfig = super.mkDbConfig(basicConfig)
    defaultDbConfig.copy(parameters = DbParametersConfig(maxConnections = Some(8)))
  }

  override lazy val createTableAction: SqlAction[Int, NoStream, Effect.Write] =
    sqlu"create table database_deadlock_test(id bigint primary key, v bigint not null)"

  override lazy val upsertCommand: DbBulkCommand = DbBulkCommand(
    """insert into database_deadlock_test(id, v)
      |values (?, ?)
      |on conflict (id) do
      |update set v = excluded.v""".stripMargin,
    setIdValue,
  )

  override def assertDeadlock(e: SQLException): Assertion = e.getSQLState shouldBe "40P01"
}
