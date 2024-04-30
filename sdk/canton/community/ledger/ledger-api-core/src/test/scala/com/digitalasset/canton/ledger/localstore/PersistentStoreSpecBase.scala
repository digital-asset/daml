// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.localstore

import com.daml.ledger.resources.ResourceContext
import com.daml.metrics.DatabaseMetrics
import com.daml.resources.Resource
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.{LoggingContextWithTrace, SuppressingLogger}
import com.digitalasset.canton.metrics.Metrics
import com.digitalasset.canton.platform.config.ServerRole
import com.digitalasset.canton.platform.store.DbSupport.{ConnectionPoolConfig, DbConfig}
import com.digitalasset.canton.platform.store.backend.StorageBackendProvider
import com.digitalasset.canton.platform.store.{DbSupport, FlywayMigrations}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContext, Future}

/** Base class for running persistent store-level tests.
  *
  * Features:
  * - Before all test cases creates a new database and applies the flyway migrations to it.
  * - Before each test case resets the contents of the database.
  * - Ensures that at most one test case runs at a time.
  */
trait PersistentStoreSpecBase extends BaseTest with BeforeAndAfterEach with BeforeAndAfterAll {
  this: Suite & StorageBackendProvider =>

  implicit protected val loggingContext: LoggingContextWithTrace =
    LoggingContextWithTrace.ForTesting

  override val loggerFactory: SuppressingLogger = SuppressingLogger(getClass)
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
    Await.ready(resetDbF, 10.seconds).discard
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
    Await.ready(dbSupportResource.release(), 15.seconds).discard
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
        loggerFactory = loggerFactory,
      )
      .acquire()
    val initializeDbAndGetDbSupportFuture: Future[DbSupport] = for {
      dbSupport <- dbSupportResource.asFuture
      _ = logger.info(s"$thisSimpleName About to do Flyway migrations")
      _ <- new FlywayMigrations(jdbcUrl, loggerFactory = loggerFactory).migrate()
      _ = logger.info(s"$thisSimpleName Completed Flyway migrations")
    } yield dbSupport
    dbSupport = Await.result(initializeDbAndGetDbSupportFuture, 2.minutes)
  }

}
