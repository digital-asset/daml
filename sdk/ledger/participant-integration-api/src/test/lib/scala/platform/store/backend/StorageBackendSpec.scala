// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import java.sql.Connection
import java.util.concurrent.atomic.AtomicInteger

import com.daml.ledger.resources.ResourceContext
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.store.FlywayMigrations
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, TestSuite}
import java.util.concurrent.Executors

import javax.sql.DataSource

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Try, Using}

private[backend] trait StorageBackendSpec
    extends StorageBackendProvider
    with BeforeAndAfterEach
    with BeforeAndAfterAll { this: TestSuite =>

  protected val logger: ContextualizedLogger = ContextualizedLogger.get(getClass)
  implicit protected val loggingContext: LoggingContext = LoggingContext.ForTesting

  // Data source (initialized once)
  private var dataSource: DataSource = _

  // Default connection for database operations (initialized for each test, since connections are stateful)
  private var defaultConnection: Connection = _

  // Execution context with a fixed number of threads, used for running parallel queries (where each query is
  // executed in its own thread).
  private val connectionPoolSize = 16
  private val connectionPoolExecutionContext = ExecutionContext.fromExecutor(
    Executors.newFixedThreadPool(
      connectionPoolSize
    )
  )

  /** Runs the given database operations in parallel.
    * Each operation will run in a separate thread and will use a separate database connection.
    */
  protected def executeParallelSql[T](fs: Vector[Connection => T]): Vector[T] = {
    require(fs.size <= connectionPoolSize)

    val connections = Vector.fill(fs.size)(dataSource.getConnection())

    implicit val ec: ExecutionContext = connectionPoolExecutionContext
    val result = Try(
      Await.result(
        Future.sequence(
          Vector.tabulate(fs.size)(i => Future(fs(i)(connections(i))))
        ),
        60.seconds,
      )
    )

    connections.foreach(_.close())
    result.get
  }

  /** Runs the given database operation */
  protected def executeSql[T](f: Connection => T): T = f(defaultConnection)

  protected def withConnections[T](n: Int)(f: List[Connection] => T): T =
    Using.Manager { manager =>
      val connections = List.fill(n)(manager(dataSource.getConnection))
      f(connections)
    }.get

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    // Note: reusing the connection pool EC for initialization
    implicit val ec: ExecutionContext = connectionPoolExecutionContext
    implicit val resourceContext: ResourceContext = ResourceContext(ec)
    implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

    val dataSourceFuture = for {
      _ <- new FlywayMigrations(jdbcUrl).migrate()
      dataSource <- VerifiedDataSource(jdbcUrl)
    } yield dataSource

    dataSource = Await.result(dataSourceFuture, 60.seconds)

    logger.info(
      s"Finished setting up database $jdbcUrl for tests. You can now connect to this database to debug failed tests. Note that tables are truncated between each test."
    )
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
  }

  private val runningTests = new AtomicInteger(0)

  // Each test should start with an empty database to allow testing low-level behavior
  // However, creating a fresh database for each test would be too expensive.
  // Instead, we truncate all tables using the reset() call before each test.
  override protected def beforeEach(): Unit = {
    super.beforeEach()

    defaultConnection = dataSource.getConnection()

    assert(
      runningTests.incrementAndGet() == 1,
      "StorageBackendSpec tests must not run in parallel, as they all run against the same database.",
    )

    // Reset the content of the index database
    backend.reset.resetAll(defaultConnection)
    updateLedgerEndCache(defaultConnection)

    // Note: here we reset the MockStringInterning object to make sure each test starts with empty interning state.
    // This is not strictly necessary, as tryInternalize() always succeeds in MockStringInterning - we don't have
    // a problem where the interning would be affected by data left over by previous tests.
    // To write tests that are sensitive to interning unknown data, we would have to use a custom storage backend
    // implementation.
    backend.stringInterningSupport.reset()
  }

  override protected def afterEach(): Unit = {
    assert(
      runningTests.decrementAndGet() == 0,
      "StorageBackendSpec tests must not run in parallel, as they all run against the same database.",
    )

    defaultConnection.close()

    super.afterEach()
  }
}
