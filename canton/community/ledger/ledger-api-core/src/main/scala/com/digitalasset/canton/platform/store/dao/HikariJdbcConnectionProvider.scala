// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.resources.ResourceOwner
import com.daml.metrics.{DatabaseMetrics, Timed}
import com.daml.scalautil.Statement.discard
import com.digitalasset.canton.ledger.api.health.{HealthStatus, Healthy, Unhealthy}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.config.ServerRole
import com.digitalasset.canton.tracing.TraceContext
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}

import java.sql.{Connection, SQLTransientConnectionException}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.{Timer, TimerTask}
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

  def owner(
      dataSource: DataSource,
      logMarker: String,
      loggerFactory: NamedLoggerFactory,
  ): ResourceOwner[JdbcConnectionProvider] =
    for {
      healthPoller <- ResourceOwner.forTimer(
        () => new Timer(s"DataSourceConnectionProvider-$logMarker#healthPoller", true),
        waitForRunningTasks = false, // do not stop resource release with ongoing healthcheck
      )
      transientFailureCount = new AtomicInteger(0)
      checkHealth <- ResourceOwner.forCloseable(() =>
        new HealthCheckTask(
          dataSource = dataSource,
          transientFailureCount = transientFailureCount,
          logMarker = logMarker,
          loggerFactory = loggerFactory,
        )
      )
    } yield {
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

class HealthCheckTask(
    dataSource: DataSource,
    transientFailureCount: AtomicInteger,
    logMarker: String,
    val loggerFactory: NamedLoggerFactory,
) extends TimerTask
    with AutoCloseable
    with NamedLogging {
  private val closed = new AtomicBoolean(false)

  private implicit val emptyTraceContext: TraceContext = TraceContext.empty

  private def printProblem(problem: String): Unit = {
    val count = transientFailureCount.incrementAndGet()
    if (count == 1) {
      if (closed.get()) {
        logger.debug(
          s"$logMarker Hikari connection health check failed after health checking stopped with: $problem"
        )
      } else {
        logger.info(s"$logMarker Hikari connection health check failed with: $problem")
      }
    }
  }

  override def run(): Unit =
    try {
      dataSource.getConnection.close()
      transientFailureCount.set(0)
    } catch {
      case e: SQLTransientConnectionException =>
        printProblem(s"transient connection exception: $e")
      case NonFatal(e) =>
        printProblem(s"unexpected exception: $e")
    }

  override def close(): Unit = {
    discard(this.cancel()) // this prevents further tasks to execute
    closed.set(true) // to emit log on debug level instead of info
  }
}
