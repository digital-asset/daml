// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql

import java.sql.Connection
import java.util.concurrent.Executors

import com.daml.ledger.on.sql.queries._
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.{Metrics, Timed}
import com.daml.resources.ProgramResource.StartupException
import com.daml.resources.ResourceOwner
import com.zaxxer.hikari.HikariDataSource
import javax.sql.DataSource
import org.flywaydb.core.Flyway

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

final class Database(
    queries: Connection => Queries,
    readerConnectionPool: DataSource,
    readerExecutionContext: ExecutionContext,
    writerConnectionPool: DataSource,
    writerExecutionContext: ExecutionContext,
    metrics: Metrics,
) {
  def inReadTransaction[T](name: String)(
      body: ReadQueries => Future[T],
  )(implicit logCtx: LoggingContext): Future[T] =
    inTransaction(name, readerConnectionPool)(connection =>
      Future(body(new TimedQueries(queries(connection), metrics)))(readerExecutionContext).flatten)

  def inWriteTransaction[T](name: String)(
      body: Queries => Future[T],
  )(implicit logCtx: LoggingContext): Future[T] =
    inTransaction(name, writerConnectionPool)(connection =>
      Future(body(new TimedQueries(queries(connection), metrics)))(writerExecutionContext).flatten)

  private def inTransaction[T](name: String, connectionPool: DataSource)(
      body: Connection => Future[T],
  )(implicit logCtx: LoggingContext): Future[T] = {
    val connection = Timed.value(
      metrics.daml.ledger.database.transactions.acquireConnection(name),
      connectionPool.getConnection())
    Timed.future(
      metrics.daml.ledger.database.transactions.run(name), {
        body(connection)
          .andThen {
            case Success(_) => connection.commit()
            case Failure(_) => connection.rollback()
          }(writerExecutionContext)
          .andThen {
            case _ => connection.close()
          }(writerExecutionContext)
      }
    )
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

  def owner(jdbcUrl: String, metrics: Metrics)(
      implicit logCtx: LoggingContext,
  ): ResourceOwner[UninitializedDatabase] =
    (jdbcUrl match {
      case "jdbc:h2:mem:" =>
        throw new InvalidDatabaseException(
          "Unnamed in-memory H2 databases are not supported. Please name the database using the format \"jdbc:h2:mem:NAME\".",
        )
      case url if url.startsWith("jdbc:h2:mem:") =>
        SingleConnectionDatabase.owner(RDBMS.H2, jdbcUrl, metrics)
      case url if url.startsWith("jdbc:h2:") =>
        MultipleConnectionDatabase.owner(RDBMS.H2, jdbcUrl, metrics)
      case url if url.startsWith("jdbc:postgresql:") =>
        MultipleConnectionDatabase.owner(RDBMS.PostgreSQL, jdbcUrl, metrics)
      case url if url.startsWith("jdbc:sqlite::memory:") =>
        throw new InvalidDatabaseException(
          "Unnamed in-memory SQLite databases are not supported. Please name the database using the format \"jdbc:sqlite:file:NAME?mode=memory&cache=shared\".",
        )
      case url if url.startsWith("jdbc:sqlite:") =>
        SingleConnectionDatabase.owner(RDBMS.SQLite, jdbcUrl, metrics)
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
        metrics: Metrics,
    ): ResourceOwner[UninitializedDatabase] =
      for {
        readerConnectionPool <- ResourceOwner.forCloseable(() =>
          newHikariDataSource(jdbcUrl, readOnly = true))
        writerConnectionPool <- ResourceOwner.forCloseable(() =>
          newHikariDataSource(jdbcUrl, maxPoolSize = Some(MaximumWriterConnectionPoolSize)))
        adminConnectionPool <- ResourceOwner.forCloseable(() => newHikariDataSource(jdbcUrl))
        readerExecutorService <- ResourceOwner.forExecutorService(() =>
          Executors.newCachedThreadPool())
        readerExecutionContext = ExecutionContext.fromExecutorService(readerExecutorService)
        writerExecutorService <- ResourceOwner.forExecutorService(() =>
          Executors.newFixedThreadPool(MaximumWriterConnectionPoolSize))
        writerExecutionContext = ExecutionContext.fromExecutorService(writerExecutorService)
      } yield
        new UninitializedDatabase(
          system = system,
          readerConnectionPool = readerConnectionPool,
          readerExecutionContext = readerExecutionContext,
          writerConnectionPool = writerConnectionPool,
          writerExecutionContext = writerExecutionContext,
          adminConnectionPool = adminConnectionPool,
          metrics = metrics,
        )
  }

  object SingleConnectionDatabase {
    def owner(
        system: RDBMS,
        jdbcUrl: String,
        metrics: Metrics,
    ): ResourceOwner[UninitializedDatabase] =
      for {
        readerWriterConnectionPool <- ResourceOwner.forCloseable(() =>
          newHikariDataSource(jdbcUrl, maxPoolSize = Some(MaximumWriterConnectionPoolSize)))
        adminConnectionPool <- ResourceOwner.forCloseable(() => newHikariDataSource(jdbcUrl))
        readerWriterExecutorService <- ResourceOwner.forExecutorService(() =>
          Executors.newFixedThreadPool(MaximumWriterConnectionPoolSize))
        readerWriterExecutionContext = ExecutionContext.fromExecutorService(
          readerWriterExecutorService)
      } yield
        new UninitializedDatabase(
          system = system,
          readerConnectionPool = readerWriterConnectionPool,
          readerExecutionContext = readerWriterExecutionContext,
          writerConnectionPool = readerWriterConnectionPool,
          writerExecutionContext = readerWriterExecutionContext,
          adminConnectionPool = adminConnectionPool,
          metrics = metrics,
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
      readerExecutionContext: ExecutionContext,
      writerConnectionPool: DataSource,
      writerExecutionContext: ExecutionContext,
      adminConnectionPool: DataSource,
      metrics: Metrics,
  ) {
    private val flyway: Flyway =
      Flyway
        .configure()
        .placeholders(Map("table.prefix" -> TablePrefix).asJava)
        .table(TablePrefix + Flyway.configure().getTable)
        .dataSource(adminConnectionPool)
        .locations(s"classpath:/com/daml/ledger/on/sql/migrations/${system.name}")
        .load()

    def migrate(): Database = {
      flyway.migrate()
      new Database(
        queries = system.queries,
        readerConnectionPool = readerConnectionPool,
        readerExecutionContext = readerExecutionContext,
        writerConnectionPool = writerConnectionPool,
        writerExecutionContext = writerExecutionContext,
        metrics = metrics,
      )
    }

    def migrateAndReset()(
        implicit executionContext: ExecutionContext,
        loggerCtx: LoggingContext): Future[Database] = {
      val db = migrate()
      db.inWriteTransaction("ledger_reset") { queries =>
          Future.fromTry(queries.truncate())
        }
        .map(_ => db)
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
