// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql

import com.daml.ledger.on.sql.queries.Queries.InvalidDatabaseException
import com.daml.ledger.on.sql.queries.{H2Queries, Queries, SqliteQueries}
import com.digitalasset.logging.{ContextualizedLogger, LoggingContext}
import com.digitalasset.resources.ResourceOwner
import com.zaxxer.hikari.HikariDataSource
import javax.sql.DataSource

sealed trait Database {
  val queries: Queries

  val readerConnectionPool: DataSource

  val writerConnectionPool: DataSource
}

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
      case url if url.startsWith("jdbc:h2:") => MultipleReaderSingleWriterDatabase.owner(jdbcUrl)
      case url if url.startsWith("jdbc:sqlite:") => SingleConnectionDatabase.owner(jdbcUrl)
      case _ => throw new InvalidDatabaseException(jdbcUrl)
    }).map { database =>
      logger.info(s"Connected to the ledger over JDBC: $jdbcUrl")
      database
    }

  final class MultipleReaderSingleWriterDatabase(
      override val readerConnectionPool: DataSource,
      override val writerConnectionPool: DataSource,
  ) extends Database {
    override val queries: Queries = new H2Queries
  }

  object MultipleReaderSingleWriterDatabase {
    def owner(jdbcUrl: String): ResourceOwner[MultipleReaderSingleWriterDatabase] =
      for {
        readerConnectionPool <- ResourceOwner.forCloseable(() =>
          newHikariDataSource(jdbcUrl, maximumPoolSize = None))
        writerConnectionPool <- ResourceOwner.forCloseable(() =>
          newHikariDataSource(jdbcUrl, maximumPoolSize = Some(MaximumWriterConnectionPoolSize)))
      } yield new MultipleReaderSingleWriterDatabase(readerConnectionPool, writerConnectionPool)
  }

  final class SingleConnectionDatabase(connectionPool: DataSource) extends Database {
    override val queries: Queries = new SqliteQueries

    override val readerConnectionPool: DataSource = connectionPool

    override val writerConnectionPool: DataSource = connectionPool
  }

  object SingleConnectionDatabase {
    def owner(jdbcUrl: String): ResourceOwner[SingleConnectionDatabase] =
      for {
        connectionPool <- ResourceOwner.forCloseable(() =>
          newHikariDataSource(jdbcUrl, maximumPoolSize = Some(MaximumWriterConnectionPoolSize)))
      } yield new SingleConnectionDatabase(connectionPool)
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
