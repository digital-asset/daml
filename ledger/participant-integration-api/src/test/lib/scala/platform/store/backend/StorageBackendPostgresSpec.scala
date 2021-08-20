// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import java.sql.Connection

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.resources.{Resource, ResourceContext}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.configuration.ServerRole
import com.daml.platform.store.appendonlydao.DbDispatcher
import com.daml.platform.store.{DbType, FlywayMigrations}
import com.daml.testing.postgresql.PostgresAroundEach
import org.scalatest.AsyncTestSuite

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.{DurationInt, FiniteDuration}

private[backend] trait StorageBackendPostgresSpec
    extends AkkaBeforeAndAfterAll
    with PostgresAroundEach { this: AsyncTestSuite =>

  protected val logger: ContextualizedLogger = ContextualizedLogger.get(getClass)
  implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  private val dbType: DbType = DbType.Postgres
  private def jdbcUrl: String = postgresDatabase.url

  private val connectionPoolSize: Int = 16
  private val metrics = new Metrics(new MetricRegistry)

  // The storage backend is stateless
  protected val storageBackend: StorageBackend[_] = StorageBackend.of(dbType)

  // Each test gets its own database and its own connection pool
  private var resource: Resource[DbDispatcher] = _
  protected var dbDispatcher: DbDispatcher = _

  protected def executeSql[T](sql: Connection => T): Future[T] = {
    dbDispatcher.executeSql(metrics.test.db)(sql)
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()

    // TODO: use a custom execution context, like JdbcLedgeDao.beforeAll()
    implicit val resourceContext: ResourceContext = ResourceContext(system.dispatcher)

    resource = for {
      _ <- Resource.fromFuture(
        new FlywayMigrations(jdbcUrl).migrate(enableAppendOnlySchema = true)
      )
      dispatcher <- DbDispatcher
        .owner(
          dataSource = storageBackend.createDataSource(jdbcUrl),
          serverRole = ServerRole.Testing(this.getClass),
          connectionPoolSize = connectionPoolSize,
          connectionTimeout = FiniteDuration(250, "millis"),
          metrics = metrics,
        )
        .acquire()
    } yield dispatcher
    dbDispatcher = Await.result(resource.asFuture, 30.seconds)
  }

  override protected def afterEach(): Unit = {
    Await.result(resource.release(), 30.seconds)
    super.afterEach()
  }
}
