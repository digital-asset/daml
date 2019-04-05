// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger.sql.migration

import java.sql.Connection

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import org.slf4j.LoggerFactory

import scala.concurrent.duration.{FiniteDuration, _}
import scala.util.control.NonFatal

/** A helper to run JDBC queries using a pool of managed connections */
trait JdbcConnectionProvider {

  /** Blocks are running in a single transaction as the commit happens when the connection
    * is returned to the pool.
    * The block must not recursively call [[runSQL]], as this could result in a deadlock
    * waiting for a free connection from the same pool. */
  def runSQL[T](block: Connection => T): T

  /** Returns a connection meant to be used for long running streaming queries. The Connection has to be closed manually! */
  def getStreamingConnection(): Connection
}

class HikariJdbcConnectionProvider(
                                    jdbcUrl: String,
                                    noOfShortLivedConnections: Int,
                                    noOfStreamingConnections: Int)
  extends JdbcConnectionProvider {

  private val logger = LoggerFactory.getLogger(getClass)
  // these connections should never timeout as we have exactly the same number of threads using them as many connections we have
  private val shortLivedDataSource =
    createDataSource(noOfShortLivedConnections, noOfShortLivedConnections, 250.millis)

  // this a dynamic pool as it's used for serving ACS snapshot requests, which we don't expect to get a lot
  private val streamingDataSource =
    createDataSource(1, noOfStreamingConnections, 60.seconds)

  private def createDataSource(
                                minimumIdle: Int,
                                maxPoolSize: Int,
                                connectionTimeout: FiniteDuration) = {
    val config = new HikariConfig
    config.setJdbcUrl(jdbcUrl)
    //TODO put these defaults out to a config file
    config.addDataSourceProperty("cachePrepStmts", "true")
    config.addDataSourceProperty("prepStmtCacheSize", "128")
    config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048")
    config.addDataSourceProperty("minimumIdle", minimumIdle)
    config.addDataSourceProperty("maximumPoolSize", maxPoolSize)
    config.addDataSourceProperty("connectionTimeout", connectionTimeout.toMillis)
    config.addDataSourceProperty("autoCommit", false)

    //note that Hikari uses auto-commit by default.
    //in `runSql` below, the `.close()` will automatically trigger a commit.
    new HikariDataSource(config)
  }

  private val flyway = FlywayMigrations(shortLivedDataSource)
  flyway.migrate()

  override def runSQL[T](block: Connection => T): T = {
    val conn = shortLivedDataSource.getConnection()
    conn.setAutoCommit(false)
    try {
      val res = block(conn)
      conn.commit()
      res
    } catch {
      case NonFatal(t) =>
        logger.error(
          "Got an exception while executing a SQL query. Rolling back the transaction.",
          t)
        conn.rollback()
        throw t
    } finally {
      conn.close()
    }
  }

  override def getStreamingConnection(): Connection =
    streamingDataSource.getConnection()

}

object HikariJdbcConnectionProvider {
  def apply(
             jdbcUrl: String,
             noOfShortLivedConnections: Int,
             noOfStreamingConnections: Int): JdbcConnectionProvider =
    new HikariJdbcConnectionProvider(jdbcUrl, noOfShortLivedConnections, noOfStreamingConnections)
}
