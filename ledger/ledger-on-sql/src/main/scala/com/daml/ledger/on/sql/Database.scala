// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql

import java.sql.{Connection, SQLException}
import java.util.concurrent.Executors

import com.daml.concurrent.FutureOf._
import com.daml.concurrent.{ExecutionContext, Future}
import com.daml.ledger.on.sql.Database._
import com.daml.ledger.on.sql.queries._
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.{Metrics, Timed}
import com.daml.resources.ProgramResource.StartupException
import com.daml.resources.ResourceOwner
import com.zaxxer.hikari.HikariDataSource
import javax.sql.DataSource
import org.flywaydb.core.Flyway
import scalaz.syntax.bind._

import scala.collection.JavaConverters._
import scala.util.{Failure, Success}

final class Database(
    queries: Connection => Queries,
    readerConnectionPool: DataSource,
    writerConnectionPool: DataSource,
    metrics: Metrics,
)(
    implicit readerExecutionContext: ExecutionContext[Reader],
    writerExecutionContext: ExecutionContext[Writer],
) {
  def inReadTransaction[T](name: String)(
      body: ReadQueries => Future[Reader, T]
  ): Future[Reader, T] =
    inTransaction(name, readerConnectionPool)(queries => Future[Reader](body(queries)).join)

  def inWriteTransaction[T](name: String)(body: Queries => Future[Writer, T]): Future[Writer, T] =
    inTransaction(name, writerConnectionPool)(queries => Future[Writer](body(queries)).join)

  private def inTransaction[X, T](name: String, connectionPool: DataSource)(
      body: Queries => Future[X, T],
  )(implicit executionContext: ExecutionContext[X]): Future[X, T] = {
    val connection = try {
      Timed.value(
        metrics.daml.ledger.database.transactions.acquireConnection(name),
        connectionPool.getConnection())
    } catch {
      case exception: SQLException =>
        throw new ConnectionAcquisitionException(name, exception)
    }
    Timed.future(
      metrics.daml.ledger.database.transactions.run(name), {
        body(new TimedQueries(queries(connection), metrics))
          .andThen {
            case Success(_) => connection.commit()
            case Failure(_) => connection.rollback()
          }
          .andThen {
            case _ => connection.close()
          }
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
      implicit loggingContext: LoggingContext,
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
        writerExecutorService <- ResourceOwner.forExecutorService(() =>
          Executors.newFixedThreadPool(MaximumWriterConnectionPoolSize))
      } yield {
        implicit val readerExecutionContext: ExecutionContext[Reader] =
          ExecutionContext.fromExecutorService[Reader](readerExecutorService)
        implicit val writerExecutionContext: ExecutionContext[Writer] =
          ExecutionContext.fromExecutorService[Writer](writerExecutorService)
        new UninitializedDatabase(
          system = system,
          readerConnectionPool = readerConnectionPool,
          writerConnectionPool = writerConnectionPool,
          adminConnectionPool = adminConnectionPool,
          metrics = metrics,
        )
      }
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
      } yield {
        implicit val readerWriterExecutionContext: ExecutionContext[Reader with Writer] =
          ExecutionContext.fromExecutorService[Reader with Writer](readerWriterExecutorService)
        new UninitializedDatabase(
          system = system,
          readerConnectionPool = readerWriterConnectionPool,
          writerConnectionPool = readerWriterConnectionPool,
          adminConnectionPool = adminConnectionPool,
          metrics = metrics,
        )
      }
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
      metrics: Metrics,
  )(
      implicit readerExecutionContext: ExecutionContext[Reader],
      writerExecutionContext: ExecutionContext[Writer],
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
        writerConnectionPool = writerConnectionPool,
        metrics = metrics,
      )
    }

    def migrateAndReset()(
        implicit executionContext: ExecutionContext[Migrator]
    ): Future[Migrator, Database] = {
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

  sealed trait Context

  sealed trait Reader extends Context

  sealed trait Writer extends Context

  sealed trait Migrator extends Writer

  class InvalidDatabaseException(message: String)
      extends RuntimeException(message)
      with StartupException

  class ConnectionAcquisitionException(name: String, cause: Throwable)
      extends RuntimeException(s"""Failed to acquire the connection during "$name".""", cause)

}
