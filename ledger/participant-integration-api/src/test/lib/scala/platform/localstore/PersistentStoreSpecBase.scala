// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.localstore

import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

import com.daml.ledger.resources.ResourceContext
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.{DatabaseMetrics, DatabaseTrackerFactory, Metrics}
import com.daml.platform.configuration.ServerRole
import com.daml.platform.store.DbSupport.{ConnectionPoolConfig, DbConfig}
import com.daml.platform.store.backend.StorageBackendProvider
import com.daml.platform.store.{DbSupport, FlywayMigrations}
import com.daml.resources.Resource
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

/** Base class for running persistent store-level tests.
  *
  * Features:
  * - Before all test cases creates a new database and applies the flyway migrations to it.
  * - Before each test case resets the contents of the database.
  * - Ensures that at most one test case runs at a time.
  */
trait PersistentStoreSpecBase extends BeforeAndAfterEach with BeforeAndAfterAll {
  this: Suite with StorageBackendProvider =>

  implicit protected val loggingContext: LoggingContext = LoggingContext.ForTesting

  protected val logger: ContextualizedLogger = ContextualizedLogger.get(getClass)

  private val runningTests = new AtomicInteger(0)
  private val thisSimpleName = getClass.getSimpleName

  protected var dbSupport: DbSupport = _
  protected var dbSupportResource: Resource[ResourceContext, DbSupport] = _

  // Each test should start with an empty database to allow testing low-level behavior
  // However, creating a fresh database for each test would be too expensive.
  // Instead, we truncate all tables using the reset() call before each test.
  override protected def beforeEach(): Unit = {
    super.beforeEach()
    val dbMetrics = DatabaseMetrics.ForTesting(getClass.getSimpleName)
    val resetDbF = dbSupport.dbDispatcher.executeSql(dbMetrics) { connection =>
      backend.reset.resetAll(connection)
    }
    Await.ready(resetDbF, 10.seconds)
    assert(
      runningTests.incrementAndGet() == 1,
      s"$thisSimpleName tests must not run in parallel, as they all run against the same database.",
    )
    ()
  }

  override protected def afterEach(): Unit = {
    assert(
      runningTests.decrementAndGet() == 0,
      s"$thisSimpleName tests must not run in parallel, as they all run against the same database.",
    )
    super.afterEach()
  }

  override protected def afterAll(): Unit = {
    Await.ready(dbSupportResource.release(), 15.seconds)
    super.afterAll()
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutor(
      Executors.newFixedThreadPool(
        2
      )
    )
    implicit val resourceContext: ResourceContext = ResourceContext(executionContext)
    dbSupportResource = DbSupport
      .owner(
        dbConfig = DbConfig(
          jdbcUrl,
          connectionPool = ConnectionPoolConfig(
            connectionPoolSize = 2,
            connectionTimeout = 250.millis,
          ),
        ),
        serverRole = ServerRole.Testing(getClass),
        metrics = Metrics.ForTesting,
        poolMetrics = DatabaseTrackerFactory.ForTesting,
      )
      .acquire()
    val initializeDbAndGetDbSupportFuture: Future[DbSupport] = for {
      dbSupport <- dbSupportResource.asFuture
      _ = logger.warn(s"$thisSimpleName About to do Flyway migrations")
      _ <- new FlywayMigrations(jdbcUrl).migrate()
      _ = logger.warn(s"$thisSimpleName Completed Flyway migrations")
    } yield dbSupport
    dbSupport = Await.result(initializeDbAndGetDbSupportFuture, 30.seconds)
  }

}
