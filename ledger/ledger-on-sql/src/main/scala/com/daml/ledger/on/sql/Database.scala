// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql

import java.sql.Connection

import com.daml.ledger.on.sql.queries.{
  H2Queries,
  PostgresqlQueries,
  Queries,
  ReadQueries,
  SqliteQueries
}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.resources.ProgramResource.StartupException
import com.daml.resources.ResourceOwner
import com.zaxxer.hikari.HikariDataSource
import javax.sql.DataSource
import org.flywaydb.core.Flyway

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

final class Database(
    queries: Connection => Queries,
    readerConnectionPool: DataSource,
    writerConnectionPool: DataSource,
) {
  private val logger = ContextualizedLogger.get(this.getClass)

  def inReadTransaction[T](message: String)(
      body: ReadQueries => Future[T],
  )(implicit executionContext: ExecutionContext, logCtx: LoggingContext): Future[T] = {
    inTransaction(message, readerConnectionPool)(connection => body(queries(connection)))
  }

  def inWriteTransaction[T](message: String)(
      body: Queries => Future[T],
  )(implicit executionContext: ExecutionContext, logCtx: LoggingContext): Future[T] = {
    inTransaction(message, writerConnectionPool)(connection => body(queries(connection)))
  }

  private def inTransaction[T](message: String, connectionPool: DataSource)(
      body: Connection => Future[T],
  )(implicit executionContext: ExecutionContext, logCtx: LoggingContext): Future[T] = {
    val connection =
      time(s"$message: acquiring connection")(connectionPool.getConnection())
    timeFuture(message) {
      body(connection)
        .andThen {
          case Success(_) => connection.commit()
          case Failure(_) => connection.rollback()
        }
        .andThen {
          case _ => connection.close()
        }
    }
  }

  private def time[T](message: String)(body: => T)(implicit logCtx: LoggingContext): T = {
    val startTime = timeStart(message)
    val result = body
    timeEnd(message, startTime)
    result
  }

  private def timeFuture[T](message: String)(
      body: => Future[T],
  )(implicit executionContext: ExecutionContext, logCtx: LoggingContext): Future[T] = {
    val startTime = timeStart(message)
    body.andThen {
      case _ => timeEnd(message, startTime)
    }
  }

  private def timeStart(message: String)(implicit logCtx: LoggingContext): Long = {
    logger.trace(s"$message: starting")
    System.nanoTime()
  }

  private def timeEnd(message: String, startTime: Long)(implicit logCtx: LoggingContext): Unit = {
    val endTime = System.nanoTime()
    logger.trace(s"$message: finished in ${Duration.fromNanos(endTime - startTime).toMillis}ms")
  }
}

object Database {
  private val logger = ContextualizedLogger.get(classOf[Database])

  // We use a prefix for all tables to ensure the database schema can be shared with the index.
  private val TablePrefix = "ledger_"

  // This *must* be 1 right now. We need to insert entries into the log in order; otherwise, we
  // might end up dispatching (head + 2) before (head + 1), which will result in missing out an
  // event when reading the log.
  //
  // To be able to process commits in parallel, we will need to fail reads and retry if there are
  // entries missing.
  private val MaximumWriterConnectionPoolSize: Int = 1

  def owner(jdbcUrl: String)(
      implicit logCtx: LoggingContext,
  ): ResourceOwner[UninitializedDatabase] =
    (jdbcUrl match {
      case "jdbc:h2:mem:" =>
        throw new InvalidDatabaseException(
          "Unnamed in-memory H2 databases are not supported. Please name the database using the format \"jdbc:h2:mem:NAME\".",
        )
      case url if url.startsWith("jdbc:h2:mem:") =>
        SingleConnectionDatabase.owner(RDBMS.H2, jdbcUrl)
      case url if url.startsWith("jdbc:h2:") =>
        MultipleConnectionDatabase.owner(RDBMS.H2, jdbcUrl)
      case url if url.startsWith("jdbc:postgresql:") =>
        MultipleConnectionDatabase.owner(RDBMS.PostgreSQL, jdbcUrl)
      case url if url.startsWith("jdbc:sqlite::memory:") =>
        throw new InvalidDatabaseException(
          "Unnamed in-memory SQLite databases are not supported. Please name the database using the format \"jdbc:sqlite:file:NAME?mode=memory&cache=shared\".",
        )
      case url if url.startsWith("jdbc:sqlite:") =>
        SingleConnectionDatabase.owner(RDBMS.SQLite, jdbcUrl)
      case _ =>
        throw new InvalidDatabaseException(s"Unknown database: $jdbcUrl")
    }).map { database =>
      logger.info(s"Connected to the ledger over JDBC: $jdbcUrl")
      database
    }

  object MultipleConnectionDatabase {
    def owner(
        system: RDBMS,
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
          readerConnectionPool,
          writerConnectionPool,
          adminConnectionPool,
        )
  }

  object SingleConnectionDatabase {
    def owner(
        system: RDBMS,
        jdbcUrl: String,
    ): ResourceOwner[UninitializedDatabase] =
      for {
        readerWriterConnectionPool <- ResourceOwner.forCloseable(() =>
          newHikariDataSource(jdbcUrl, maxPoolSize = Some(MaximumWriterConnectionPoolSize)))
        adminConnectionPool <- ResourceOwner.forCloseable(() => newHikariDataSource(jdbcUrl))
      } yield
        new UninitializedDatabase(
          system,
          readerWriterConnectionPool,
          readerWriterConnectionPool,
          adminConnectionPool,
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

    val queries: Connection => Queries
  }

  object RDBMS {
    object H2 extends RDBMS {
      override val name: String = "h2"

      override val queries: Connection => Queries = H2Queries.apply
    }

    object PostgreSQL extends RDBMS {
      override val name: String = "postgresql"

      override val queries: Connection => Queries = PostgresqlQueries.apply
    }

    object SQLite extends RDBMS {
      override val name: String = "sqlite"

      override val queries: Connection => Queries = SqliteQueries.apply
    }
  }

  class UninitializedDatabase(
      system: RDBMS,
      readerConnectionPool: DataSource,
      writerConnectionPool: DataSource,
      adminConnectionPool: DataSource,
  ) {
    private val flyway: Flyway =
      Flyway
        .configure()
        .placeholders(Map("table.prefix" -> TablePrefix).asJava)
        .table(TablePrefix + Flyway.configure().getTable)
        .dataSource(adminConnectionPool)
        .locations(
          "classpath:/com/daml/ledger/on/sql/migrations/common",
          s"classpath:/com/daml/ledger/on/sql/migrations/${system.name}",
        )
        .load()

    def migrate(): Database = {
      flyway.migrate()
      new Database(system.queries, readerConnectionPool, writerConnectionPool)
    }

    def clear(): this.type = {
      flyway.clean()
      this
    }
  }

  class InvalidDatabaseException(message: String)
      extends RuntimeException(message)
      with StartupException
}
