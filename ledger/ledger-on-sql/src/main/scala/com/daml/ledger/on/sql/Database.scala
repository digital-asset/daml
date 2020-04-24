// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql

import java.sql.Connection
import java.util.concurrent.TimeUnit

import com.codahale.metrics.{MetricRegistry, Timer}
import com.daml.ledger.on.sql.queries.{H2Queries, PostgresqlQueries, Queries, ReadQueries, SqliteQueries, TimedQueries}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.{MetricName, Timed}
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
    writerConnectionPool: DataSource,
    metricRegistry: MetricRegistry,
) {
  def inReadTransaction[T](name: String)(
      body: ReadQueries => Future[T],
  )(implicit executionContext: ExecutionContext, logCtx: LoggingContext): Future[T] =
    inTransaction(name, readerConnectionPool)(connection =>
      body(new TimedQueries(queries(connection), metricRegistry)))

  def inWriteTransaction[T](name: String)(
      body: Queries => Future[T],
  )(implicit executionContext: ExecutionContext, logCtx: LoggingContext): Future[T] =
    inTransaction(name, writerConnectionPool)(connection =>
      body(new TimedQueries(queries(connection), metricRegistry)))

  private def inTransaction[T](name: String, connectionPool: DataSource)(
      body: Connection => Future[T],
  )(implicit executionContext: ExecutionContext, logCtx: LoggingContext): Future[T] = {
    val connection = Timed.value(Metrics.acquireConnection(name), connectionPool.getConnection())
    Timed.future(
      Metrics.run(name), {
        body(connection)
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

  private object Metrics {
    private val prefix = MetricName.DAML :+ "ledger" :+ "database" :+ "transactions"

    def acquireConnection(name: String): Timer =
      metricRegistry.timer(prefix :+ name :+ "acquire_connection")

    def run(name: String): Timer =
      metricRegistry.timer(prefix :+ name :+ "run")
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

  def owner(jdbcUrl: String, metricRegistry: MetricRegistry)(
      implicit logCtx: LoggingContext,
  ): ResourceOwner[UninitializedDatabase] =
    (jdbcUrl match {
      case "jdbc:h2:mem:" =>
        throw new InvalidDatabaseException(
          "Unnamed in-memory H2 databases are not supported. Please name the database using the format \"jdbc:h2:mem:NAME\".",
        )
      case url if url.startsWith("jdbc:h2:mem:") =>
        SingleConnectionDatabase.owner(RDBMS.H2, jdbcUrl, metricRegistry)
      case url if url.startsWith("jdbc:h2:") =>
        MultipleConnectionDatabase.owner(RDBMS.H2, jdbcUrl, metricRegistry)
      case url if url.startsWith("jdbc:postgresql:") =>
        MultipleConnectionDatabase.owner(RDBMS.PostgreSQL, jdbcUrl, metricRegistry)
      case url if url.startsWith("jdbc:sqlite::memory:") =>
        throw new InvalidDatabaseException(
          "Unnamed in-memory SQLite databases are not supported. Please name the database using the format \"jdbc:sqlite:file:NAME?mode=memory&cache=shared\".",
        )
      case url if url.startsWith("jdbc:sqlite:") =>
        SingleConnectionDatabase.owner(RDBMS.SQLite, jdbcUrl, metricRegistry)
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
        metricRegistry: MetricRegistry,
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
          metricRegistry,
        )
  }

  object SingleConnectionDatabase {
    def owner(
        system: RDBMS,
        jdbcUrl: String,
        metricRegistry: MetricRegistry,
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
          metricRegistry,
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
      metricRegistry: MetricRegistry,
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
      new Database(system.queries, readerConnectionPool, writerConnectionPool, metricRegistry)
    }

    def migrateAndReset()(implicit executionContext: ExecutionContext, loggerCtx: LoggingContext): Future[Database] = {
//      val db = new Database(system.queries, readerConnectionPool, writerConnectionPool, metricRegistry)
      val start = System.nanoTime()
      val db = migrate()
      val elapsed = System.nanoTime() - start
      logger.error(s"######## ELAPSED KV MIGRATION: ${TimeUnit.NANOSECONDS.toMillis(elapsed)}")
      val before = System.nanoTime()
      val result = db
        .inWriteTransaction("ledger_reset") { queries => Future.fromTry(queries.truncate()) }
        .map(_ => db)
      result.foreach { _ =>
        val elapsed = System.nanoTime() - before
        logger.error(s"######### ELAPSED KV TRUNCATION: ${TimeUnit.NANOSECONDS.toMillis(elapsed)}")
      }
      result

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
