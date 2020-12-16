// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import java.sql.{Connection, SQLTransientConnectionException}
import java.util.concurrent.atomic.AtomicInteger
import java.util.{Timer, TimerTask}

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.api.health.{HealthStatus, Healthy, Unhealthy}
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.{DatabaseMetrics, Timed}
import com.daml.platform.configuration.ServerRole
import com.daml.platform.store.DbType
import com.daml.platform.store.dao.HikariJdbcConnectionProvider._
import com.daml.timer.RetryStrategy
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}

import scala.concurrent.Future
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.control.NonFatal

private[platform] final class HikariConnection(
    serverRole: ServerRole,
    jdbcUrl: String,
    minimumIdle: Int,
    maxPoolSize: Int,
    connectionTimeout: FiniteDuration,
    metrics: Option[MetricRegistry],
    connectionPoolPrefix: String,
    maxInitialConnectRetryAttempts: Int,
)(implicit loggingContext: LoggingContext)
    extends ResourceOwner[HikariDataSource] {

  private val logger = ContextualizedLogger.get(this.getClass)

  override def acquire()(implicit context: ResourceContext): Resource[HikariDataSource] = {
    val config = new HikariConfig
    config.setJdbcUrl(jdbcUrl)
    config.setDriverClassName(DbType.jdbcType(jdbcUrl).driver)
    config.addDataSourceProperty("cachePrepStmts", "true")
    config.addDataSourceProperty("prepStmtCacheSize", "128")
    config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048")
    config.setConnectionInitSql("SET synchronous_commit=OFF")
    config.setAutoCommit(false)
    config.setMaximumPoolSize(maxPoolSize)
    config.setMinimumIdle(minimumIdle)
    config.setConnectionTimeout(connectionTimeout.toMillis)
    config.setPoolName(s"$connectionPoolPrefix.${serverRole.threadPoolSuffix}")
    metrics.foreach(config.setMetricRegistry)

    // Hikari dies if a database connection could not be opened almost immediately
    // regardless of any connection timeout settings. We retry connections so that
    // Postgres and Sandbox can be started in any order.
    Resource(
      RetryStrategy.constant(
        attempts = maxInitialConnectRetryAttempts,
        waitTime = 1.second
      ) { (i, _) =>
        Future {
          logger.info(
            s"Attempting to connect to $jdbcUrl (attempt $i/$maxInitialConnectRetryAttempts)")
          new HikariDataSource(config)
        }
      }
    )(conn => Future { conn.close() })
  }
}

private[platform] object HikariConnection {
  private val MaxInitialConnectRetryAttempts = 600
  private val ConnectionPoolPrefix: String = "daml.index.db.connection"

  def owner(
      serverRole: ServerRole,
      jdbcUrl: String,
      minimumIdle: Int,
      maxPoolSize: Int,
      connectionTimeout: FiniteDuration,
      metrics: Option[MetricRegistry],
  )(
      implicit loggingContext: LoggingContext
  ): HikariConnection =
    new HikariConnection(
      serverRole,
      jdbcUrl,
      minimumIdle,
      maxPoolSize,
      connectionTimeout,
      metrics,
      ConnectionPoolPrefix,
      MaxInitialConnectRetryAttempts,
    )
}

private[platform] class HikariJdbcConnectionProvider(
    dataSource: HikariDataSource,
    healthPoller: Timer,
) extends JdbcConnectionProvider {
  private val transientFailureCount = new AtomicInteger(0)

  private val checkHealth = new TimerTask {
    override def run(): Unit = {
      try {
        dataSource.getConnection().close()
        transientFailureCount.set(0)
      } catch {
        case _: SQLTransientConnectionException =>
          val _ = transientFailureCount.incrementAndGet()
      }
    }
  }

  healthPoller.schedule(checkHealth, 0, HealthPollingSchedule.toMillis)

  override def currentHealth(): HealthStatus =
    if (transientFailureCount.get() < MaxTransientFailureCount)
      Healthy
    else
      Unhealthy

  override def runSQL[T](databaseMetrics: DatabaseMetrics)(block: Connection => T): T = {
    val conn = dataSource.getConnection()
    conn.setAutoCommit(false)
    try {
      val res = Timed.value(
        databaseMetrics.queryTimer,
        block(conn)
      )
      Timed.value(
        databaseMetrics.commitTimer,
        conn.commit()
      )
      res
    } catch {
      case e: SQLTransientConnectionException =>
        transientFailureCount.incrementAndGet()
        throw e
      case NonFatal(t) =>
        // Log the error in the caller with access to more logging context (such as the sql statement description)
        conn.rollback()
        throw t
    } finally {
      conn.close()
    }
  }
}

private[platform] object HikariJdbcConnectionProvider {
  private val MaxTransientFailureCount: Int = 5
  private val HealthPollingSchedule: FiniteDuration = 1.second

  def owner(
      serverRole: ServerRole,
      jdbcUrl: String,
      maxConnections: Int,
      metrics: MetricRegistry,
  )(implicit loggingContext: LoggingContext): ResourceOwner[HikariJdbcConnectionProvider] =
    for {
      // these connections should never time out as we have the same number of threads as connections
      dataSource <- HikariConnection.owner(
        serverRole,
        jdbcUrl,
        maxConnections,
        maxConnections,
        250.millis,
        Some(metrics),
      )
      healthPoller <- ResourceOwner.forTimer(() =>
        new Timer(s"${classOf[HikariJdbcConnectionProvider].getName}#healthPoller"))
    } yield new HikariJdbcConnectionProvider(dataSource, healthPoller)
}
