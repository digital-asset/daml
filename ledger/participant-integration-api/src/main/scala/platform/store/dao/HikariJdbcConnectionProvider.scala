// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import java.sql.{Connection, SQLTransientConnectionException}
import java.util.concurrent.atomic.AtomicInteger
import java.util.{Timer, TimerTask}

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.api.health.{HealthStatus, Healthy, Unhealthy}
import com.daml.ledger.resources.ResourceOwner
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.{DatabaseMetrics, Timed}
import com.daml.platform.configuration.ServerRole
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import javax.sql.DataSource

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.control.NonFatal

private[platform] object HikariDataSourceOwner {

  def apply(
      dataSource: DataSource,
      serverRole: ServerRole,
      minimumIdle: Int,
      maxPoolSize: Int,
      connectionTimeout: FiniteDuration,
      metrics: Option[MetricRegistry],
      connectionPoolPrefix: String = "daml.index.db.connection",
  ): ResourceOwner[DataSource] =
    ResourceOwner.forCloseable { () =>
      val config = new HikariConfig
      config.setDataSource(dataSource)
      config.setAutoCommit(false)
      config.setMaximumPoolSize(maxPoolSize)
      config.setMinimumIdle(minimumIdle)
      config.setConnectionTimeout(connectionTimeout.toMillis)
      config.setPoolName(s"$connectionPoolPrefix.${serverRole.threadPoolSuffix}")
      metrics.foreach(config.setMetricRegistry)
      new HikariDataSource(config)
    }
}

object DataSourceConnectionProvider {
  private val MaxTransientFailureCount: Int = 5
  private val HealthPollingSchedule: FiniteDuration = 1.second
  private val logger = ContextualizedLogger.get(this.getClass)

  def owner(dataSource: DataSource): ResourceOwner[JdbcConnectionProvider] =
    for {
      healthPoller <- ResourceOwner.forTimer(() =>
        new Timer("DataSourceConnectionProvider#healthPoller")
      )
    } yield {
      val transientFailureCount = new AtomicInteger(0)

      val checkHealth = new TimerTask {
        override def run(): Unit = {
          LoggingContext.newLoggingContext { implicit loggingContext =>
            try {
              dataSource.getConnection().close()
              transientFailureCount.set(0)
            } catch {
              case _: SQLTransientConnectionException =>
                val _ = transientFailureCount.incrementAndGet()
              case _: Throwable =>
                val count = transientFailureCount.incrementAndGet()
                if (count == 1)
                  logger.info("Hikari connection health check failed unexpectedly")
                ()
            }
          }
        }
      }

      healthPoller.schedule(checkHealth, 0, HealthPollingSchedule.toMillis)

      new JdbcConnectionProvider {
        override def runSQL[T](databaseMetrics: DatabaseMetrics)(block: Connection => T): T = {
          val conn = dataSource.getConnection()
          conn.setAutoCommit(false)
          try {
            val res = Timed.value(
              databaseMetrics.queryTimer,
              block(conn),
            )
            Timed.value(
              databaseMetrics.commitTimer,
              conn.commit(),
            )
            res
          } catch {
            case e: SQLTransientConnectionException =>
              transientFailureCount.incrementAndGet()
              conn.rollback()
              throw e
            case NonFatal(t) =>
              // Log the error in the caller with access to more logging context (such as the sql statement description)
              conn.rollback()
              throw t
          } finally {
            conn.close()
          }
        }

        override def currentHealth(): HealthStatus =
          if (transientFailureCount.get() < MaxTransientFailureCount)
            Healthy
          else
            Unhealthy
      }
    }
}
