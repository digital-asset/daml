// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger.sql.dao

import java.sql.Connection

import com.codahale.metrics.MetricRegistry
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}

import scala.concurrent.duration.{FiniteDuration, _}
import scala.util.control.NonFatal

/** A helper to run JDBC queries using a pool of managed connections */
trait JdbcConnectionProvider extends AutoCloseable {

  /** Blocks are running in a single transaction as the commit happens when the connection
    * is returned to the pool.
    * The block must not recursively call [[runSQL]], as this could result in a deadlock
    * waiting for a free connection from the same pool. */
  def runSQL[T](block: Connection => T): T
}

object HikariConnection {
  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def createDataSource(
      jdbcUrl: String,
      poolName: String,
      minimumIdle: Int,
      maxPoolSize: Int,
      connectionTimeout: FiniteDuration,
      metrics: Option[MetricRegistry]): HikariDataSource = {
    val config = new HikariConfig
    config.setJdbcUrl(jdbcUrl)
    config.setDriverClassName(DbType.jdbcType(jdbcUrl).driver)
    config.addDataSourceProperty("cachePrepStmts", "true")
    config.addDataSourceProperty("prepStmtCacheSize", "128")
    config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048")
    config.setAutoCommit(false)
    config.setMaximumPoolSize(maxPoolSize)
    config.setMinimumIdle(minimumIdle)
    config.setConnectionTimeout(connectionTimeout.toMillis)
    config.setPoolName(poolName)
    metrics.foreach(config.setMetricRegistry)

    //note that Hikari uses auto-commit by default.
    //in `runSql` below, the `.close()` will automatically trigger a commit.
    new HikariDataSource(config)
  }
}

class HikariJdbcConnectionProvider(
    jdbcUrl: String,
    noOfShortLivedConnections: Int,
    metrics: MetricRegistry)
    extends JdbcConnectionProvider {

  // these connections should never timeout as we have exactly the same number of threads using them as many connections we have
  private val shortLivedDataSource =
    HikariConnection.createDataSource(
      jdbcUrl,
      "Short-Lived-Connections",
      noOfShortLivedConnections,
      noOfShortLivedConnections,
      250.millis,
      Some(metrics))

  override def runSQL[T](block: Connection => T): T = {
    val conn = shortLivedDataSource.getConnection()
    conn.setAutoCommit(false)
    try {
      val res = block(conn)
      conn.commit()
      res
    } catch {
      case NonFatal(t) =>
        // Log the error in the caller with access to more logging context (such as the sql statement description)
        conn.rollback()
        throw t
    } finally {
      conn.close()
    }
  }

  override def close(): Unit = {
    shortLivedDataSource.close()
  }
}
