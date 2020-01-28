// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql

import com.daml.ledger.on.sql.queries.Queries.InvalidDatabaseException
import com.daml.ledger.on.sql.queries.{H2Queries, PostgresqlQueries, Queries, SqliteQueries}
import com.digitalasset.logging.{ContextualizedLogger, LoggingContext}
import com.digitalasset.resources.ResourceOwner
import com.zaxxer.hikari.HikariDataSource
import javax.sql.DataSource
import org.flywaydb.core.Flyway

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

  def owner(jdbcUrl: String)(
      implicit logCtx: LoggingContext
  ): ResourceOwner[UninitializedDatabase] =
    (jdbcUrl match {
      case url if url.startsWith("jdbc:h2:") =>
        MultipleConnectionDatabase.owner(RDBMS.H2, new H2Queries, jdbcUrl)
      case url if url.startsWith("jdbc:postgresql:") =>
        MultipleConnectionDatabase.owner(RDBMS.PostgreSQL, new PostgresqlQueries, jdbcUrl)
      case url if url.startsWith("jdbc:sqlite:") =>
        SingleConnectionDatabase.owner(RDBMS.SQLite, new SqliteQueries, jdbcUrl)
      case _ => throw new InvalidDatabaseException(jdbcUrl)
    }).map { database =>
      logger.info(s"Connected to the ledger over JDBC: $jdbcUrl")
      database
    }

  object MultipleConnectionDatabase {
    def owner(
        system: RDBMS,
        queries: Queries,
        jdbcUrl: String,
    ): ResourceOwner[UninitializedDatabase] =
      for {
        readerConnectionPool <- ResourceOwner.forCloseable(() =>
          newHikariDataSource(jdbcUrl, readOnly = true))
        writerConnectionPool <- ResourceOwner.forCloseable(() =>
          newHikariDataSource(jdbcUrl, maxPoolSize = Some(MaximumWriterConnectionPoolSize)))
        adminConnectionPool <- ResourceOwner.forCloseable(() => newHikariDataSource(jdbcUrl))
      } yield
        new UninitializedDatabase(
          system,
          queries,
          readerConnectionPool,
          writerConnectionPool,
          adminConnectionPool,
        )
  }

  object SingleConnectionDatabase {
    def owner(
        system: RDBMS,
        queries: Queries,
        jdbcUrl: String,
    ): ResourceOwner[UninitializedDatabase] =
      for {
        connectionPool <- ResourceOwner.forCloseable(() => newHikariDataSource(jdbcUrl))
      } yield
        new UninitializedDatabase(
          system,
          queries,
          readerConnectionPool = connectionPool,
          writerConnectionPool = connectionPool,
          adminConnectionPool = connectionPool,
          afterMigration = () => {
            // Flyway needs 2 database connections: one for locking its own table, and then one for
            // performing the migration. We allow it to do this, then drop the connection pool cap
            // to 1 afterwards. With SQLite in-memory, we can't use a separate connection pool, it
            // will create a new in-memory database for each connection (and therefore each
            // connection pool).
            connectionPool.setMaximumPoolSize(MaximumWriterConnectionPoolSize)
          }
        )
  }

  private def newHikariDataSource(
      jdbcUrl: String,
      readOnly: Boolean = false,
      maxPoolSize: Option[Int] = None,
  ): HikariDataSource = {
    val pool = new HikariDataSource()
    pool.setAutoCommit(false)
    pool.setJdbcUrl(jdbcUrl)
    pool.setReadOnly(readOnly)
    maxPoolSize.foreach(pool.setMaximumPoolSize)
    pool
  }

  sealed trait RDBMS {
    val name: String
  }

  object RDBMS {
    object H2 extends RDBMS {
      override val name: String = "h2"
    }

    object PostgreSQL extends RDBMS {
      override val name: String = "postgresql"
    }

    object SQLite extends RDBMS {
      override val name: String = "sqlite"
    }
  }

  class UninitializedDatabase(
      system: RDBMS,
      queries: Queries,
      readerConnectionPool: DataSource,
      writerConnectionPool: DataSource,
      adminConnectionPool: DataSource,
      afterMigration: () => Unit = () => (),
  ) {
    private val flyway: Flyway =
      Flyway
        .configure()
        .dataSource(adminConnectionPool)
        .locations(s"classpath:/com/daml/ledger/on/sql/migrations/${system.name}")
        .load()

    def migrate(): Database = {
      flyway.migrate()
      afterMigration()
      Database(queries, readerConnectionPool, writerConnectionPool)
    }

    def clear(): this.type = {
      flyway.clean()
      this
    }
  }
}
