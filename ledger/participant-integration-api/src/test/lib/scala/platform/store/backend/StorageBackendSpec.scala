// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import java.sql.Connection
import java.util.concurrent.atomic.AtomicInteger

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.resources.{Resource, ResourceContext}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.configuration.ServerRole
import com.daml.platform.store.FlywayMigrations
import com.daml.platform.store.appendonlydao.DbDispatcher
import org.scalatest.{AsyncTestSuite, BeforeAndAfterEach}

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{Await, Future}

private[backend] trait StorageBackendSpec
    extends AkkaBeforeAndAfterAll
    with BeforeAndAfterEach
    with StorageBackendProvider { this: AsyncTestSuite =>

  protected val logger: ContextualizedLogger = ContextualizedLogger.get(getClass)
  implicit protected val loggingContext: LoggingContext = LoggingContext.ForTesting

  private val connectionPoolSize: Int = 16
  private val metrics = new Metrics(new MetricRegistry)

  // Initialized in beforeAll()
  private var dbDispatcherResource: Resource[DbDispatcher] = _
  private var dbDispatcher: DbDispatcher = _

  protected def executeSql[T](sql: Connection => T): Future[T] = {
    dbDispatcher.executeSql(metrics.test.db)(sql)
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    implicit val resourceContext: ResourceContext = ResourceContext(system.dispatcher)
    implicit val loggingContext: LoggingContext = LoggingContext.ForTesting
    dbDispatcherResource = for {
      _ <- Resource.fromFuture(
        new FlywayMigrations(jdbcUrl).migrate()
      )
      dispatcher <- DbDispatcher
        .owner(
          dataSource = backend.createDataSource(jdbcUrl),
          serverRole = ServerRole.Testing(this.getClass),
          connectionPoolSize = connectionPoolSize,
          connectionTimeout = FiniteDuration(250, "millis"),
          metrics = metrics,
        )
        .acquire()
    } yield dispatcher

    dbDispatcher = Await.result(dbDispatcherResource.asFuture, 60.seconds)
    logger.info(
      s"Finished setting up database $jdbcUrl for tests. You can now connect to this database to debug failed tests. Note that tables are truncated between each test."
    )
  }

  override protected def afterAll(): Unit = {
    Await.result(dbDispatcherResource.release(), 60.seconds)
    super.afterAll()
  }

  private val runningTests = new AtomicInteger(0)

  // Each test should start with an empty database to allow testing low-level behavior
  // However, creating a fresh database for each test would be too expensive.
  // Instead, we truncate all tables using the reset() call before each test.
  override protected def beforeEach(): Unit = {
    super.beforeEach()

    assert(
      runningTests.incrementAndGet() == 1,
      "StorageBackendSpec tests must not run in parallel, as they all run against the same database.",
    )
    Await.result(executeSql(backend.resetAll), 60.seconds)
  }

  override protected def afterEach(): Unit = {
    assert(
      runningTests.decrementAndGet() == 0,
      "StorageBackendSpec tests must not run in parallel, as they all run against the same database.",
    )

    super.afterEach()
  }
}
