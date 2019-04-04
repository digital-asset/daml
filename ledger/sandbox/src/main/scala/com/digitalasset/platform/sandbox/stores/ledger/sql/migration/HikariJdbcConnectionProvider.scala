// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger.sql.migration

import java.sql.Connection

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}

/** A helper to run JDBC queries using a pool of managed connections */
trait JdbcConnectionProvider {

  /** Blocks are running in a single transaction as the commit happens when the connection
    * is returned to the pool */
  def runSQL[T](block: Connection => T): T
}

class HikariJdbcConnectionProvider(jdbcUrl: String, userName: String, poolSize: Int)
    extends JdbcConnectionProvider {

  private val hikariDataSource = {
    val config = new HikariConfig
    config.setJdbcUrl(jdbcUrl)
    //TODO put these defaults out to a config file

    config.addDataSourceProperty("cachePrepStmts", "true")
    config.addDataSourceProperty("prepStmtCacheSize", "128")
    config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048")
    config.addDataSourceProperty("maximumPoolSize", poolSize)
    config.setUsername(userName)
    //note that Hikari uses auto-commit by default.
    //in `runSql` below, the `.close()` will automatically trigger a commit.
    new HikariDataSource(config)
  }

  private val flyway = FlywayMigrations(hikariDataSource)
  flyway.migrate()

  override def runSQL[T](block: Connection => T): T = {
    val conn = hikariDataSource.getConnection()
    try {
      block(conn)
    } finally {
      conn.close()
    }
  }

}

object HikariJdbcConnectionProvider {
  def apply(jdbcUrl: String, userName: String, poolSize: Int): JdbcConnectionProvider =
    new HikariJdbcConnectionProvider(jdbcUrl, userName, poolSize)
}
