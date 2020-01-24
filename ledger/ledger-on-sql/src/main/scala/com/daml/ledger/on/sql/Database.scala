// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql

import com.daml.ledger.on.sql.queries.Queries.InvalidDatabaseException
import com.daml.ledger.on.sql.queries.{H2Queries, Queries, SqliteQueries}
import com.digitalasset.logging.{ContextualizedLogger, LoggingContext}
import com.digitalasset.resources.ResourceOwner
import com.zaxxer.hikari.HikariDataSource
import javax.sql.DataSource

case class Database(
    queries: Queries,
    readerConnectionPool: DataSource,
    writerConnectionPool: DataSource,
)

object Database {
  private val logger = ContextualizedLogger.get(classOf[Database])

  // This *must* be 1 right now. We need to insert entries into the log in order; otherwise, we
  // might end up dispatching (head + 2) before (head + 1), which will result in missing out an
  // event when reading the log.
  //
  // To be able to process commits in parallel, we will need to fail reads and retry if there are
  // entries missing.
  private val MaximumWriterConnectionPoolSize: Int = 1

  def owner(jdbcUrl: String)(implicit logCtx: LoggingContext): ResourceOwner[Database] =
    (jdbcUrl match {
      case url if url.startsWith("jdbc:h2:") =>
        MultipleReaderSingleWriterDatabase.owner(jdbcUrl, new H2Queries)
      case url if url.startsWith("jdbc:sqlite:") =>
        SingleConnectionDatabase.owner(jdbcUrl, new SqliteQueries)
      case _ => throw new InvalidDatabaseException(jdbcUrl)
    }).map { database =>
      logger.info(s"Connected to the ledger over JDBC: $jdbcUrl")
      database
    }

  object MultipleReaderSingleWriterDatabase {
    def owner(jdbcUrl: String, queries: Queries): ResourceOwner[Database] =
      for {
        readerConnectionPool <- ResourceOwner.forCloseable(() =>
          newHikariDataSource(jdbcUrl, maximumPoolSize = None))
        writerConnectionPool <- ResourceOwner.forCloseable(() =>
          newHikariDataSource(jdbcUrl, maximumPoolSize = Some(MaximumWriterConnectionPoolSize)))
      } yield new Database(queries, readerConnectionPool, writerConnectionPool)
  }

  object SingleConnectionDatabase {
    def owner(jdbcUrl: String, queries: Queries): ResourceOwner[Database] =
      for {
        connectionPool <- ResourceOwner.forCloseable(() =>
          newHikariDataSource(jdbcUrl, maximumPoolSize = Some(MaximumWriterConnectionPoolSize)))
      } yield new Database(queries, connectionPool, connectionPool)
  }

  private def newHikariDataSource(
      jdbcUrl: String,
      maximumPoolSize: Option[Int],
  ): HikariDataSource = {
    val pool = new HikariDataSource()
    pool.setAutoCommit(false)
    pool.setJdbcUrl(jdbcUrl)
    maximumPoolSize.foreach { maximumPoolSize =>
      pool.setMaximumPoolSize(maximumPoolSize)
    }
    pool
  }
}
