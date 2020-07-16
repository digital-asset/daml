// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql

import java.sql.Connection
import java.util.concurrent.Executors

import com.daml.ledger.on.sql.queries._
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.resources.ProgramResource.StartupException
import com.daml.resources.ResourceOwner
import com.zaxxer.hikari.HikariDataSource
import org.flywaydb.core.Flyway

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

final class Database(
    readOnlyTransactor: Transactor[ReadQueries],
    readWriteTransactor: Transactor[ReadWriteQueries],
    metrics: Metrics,
) {

  def inReadOnlyTransaction[T](name: String)(
      body: ReadQueries => Try[T],
  )(implicit logCtx: LoggingContext): Future[T] =
    readOnlyTransactor.inTransaction(name)(readQueries =>
      body(new TimedReadQueries(readQueries, metrics)))

  def inTransaction[T](name: String)(
      body: ReadWriteQueries => Try[T],
  )(implicit logCtx: LoggingContext): Future[T] =
    readWriteTransactor.inTransaction(name)(readWriteQueries =>
      body(new TimedReadWriteQueries(readWriteQueries, metrics)))

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

  private final class ConnectionPoolName(val string: String) extends AnyVal

  private object ConnectionPoolName {
    private val Prefix = "Ledger-Pool"
    val ReadWrite = new ConnectionPoolName(s"$Prefix-ReadWrite")
    val ReadOnly = new ConnectionPoolName(s"$Prefix-ReadOnly")
    val SingleConnection = new ConnectionPoolName(s"$Prefix-SingleConnection")
    val Admin = new ConnectionPoolName(s"$Prefix-Admin")
  }

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
        readOnlyConnectionPool <- ResourceOwner.forCloseable(() =>
          newHikariDataSource(ConnectionPoolName.ReadOnly, jdbcUrl, readOnly = true))
        readWriteConnectionPool <- ResourceOwner.forCloseable(
          () =>
            newRestrictedHikariDataSource(
              ConnectionPoolName.ReadOnly,
              jdbcUrl,
              MaximumWriterConnectionPoolSize))
        adminConnectionPool <- ResourceOwner.forCloseable(() =>
          newHikariDataSource(ConnectionPoolName.Admin, jdbcUrl))
        readOnlyExecutorService <- ResourceOwner.forExecutorService(() =>
          Executors.newCachedThreadPool())
        readOnlyExecutionContext = ExecutionContext.fromExecutorService(readOnlyExecutorService)
        readWriteExecutorService <- ResourceOwner.forExecutorService(() =>
          Executors.newFixedThreadPool(MaximumWriterConnectionPoolSize))
        readWriteExecutionContext = ExecutionContext.fromExecutorService(readWriteExecutorService)
      } yield
        new UninitializedDatabase(
          system = system,
          readOnlyConnectionPool = readOnlyConnectionPool,
          readOnlyExecutionContext = readOnlyExecutionContext,
          readWriteConnectionPool = readWriteConnectionPool,
          readWriteExecutionContext = readWriteExecutionContext,
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
        readerWriterConnectionPool <- ResourceOwner.forCloseable(
          () =>
            newRestrictedHikariDataSource(
              ConnectionPoolName.SingleConnection,
              jdbcUrl,
              MaximumWriterConnectionPoolSize))
        adminConnectionPool <- ResourceOwner.forCloseable(() =>
          newHikariDataSource(ConnectionPoolName.Admin, jdbcUrl))
        readerWriterExecutorService <- ResourceOwner.forExecutorService(() =>
          Executors.newFixedThreadPool(MaximumWriterConnectionPoolSize))
        readerWriterExecutionContext = ExecutionContext.fromExecutorService(
          readerWriterExecutorService)
      } yield
        new UninitializedDatabase(
          system = system,
          readOnlyConnectionPool = readerWriterConnectionPool,
          readOnlyExecutionContext = readerWriterExecutionContext,
          readWriteConnectionPool = readerWriterConnectionPool,
          readWriteExecutionContext = readerWriterExecutionContext,
          adminConnectionPool = adminConnectionPool,
          metrics = metrics,
        )
  }

  private def newHikariDataSource(
      name: ConnectionPoolName,
      jdbcUrl: String,
      readOnly: Boolean = false,
  ): HikariDataSource = {
    val pool = new HikariDataSource()
    pool.setPoolName(name.string)
    pool.setAutoCommit(false)
    pool.setJdbcUrl(jdbcUrl)
    pool.setReadOnly(readOnly)
    pool
  }

  private def newRestrictedHikariDataSource(
      name: ConnectionPoolName,
      jdbcUrl: String,
      maxPoolSize: Int,
      readOnly: Boolean = false,
  ): HikariDataSource = {
    val pool = newHikariDataSource(name, jdbcUrl, readOnly)
    pool.setMaximumPoolSize(maxPoolSize)
    pool
  }

  sealed trait RDBMS {
    val name: String

    val queries: Connection => ReadWriteQueries
  }

  object RDBMS {
    object H2 extends RDBMS {
      override val name: String = "h2"

      override val queries: Connection => ReadWriteQueries = H2Queries.apply
    }

    object PostgreSQL extends RDBMS {
      override val name: String = "postgresql"

      override val queries: Connection => ReadWriteQueries = PostgresqlQueries.apply
    }

    object SQLite extends RDBMS {
      override val name: String = "sqlite"

      override val queries: Connection => ReadWriteQueries = SqliteQueries.apply
    }
  }

  final class UninitializedDatabase(
      system: RDBMS,
      readOnlyConnectionPool: HikariDataSource,
      readOnlyExecutionContext: ExecutionContext,
      readWriteConnectionPool: HikariDataSource,
      readWriteExecutionContext: ExecutionContext,
      adminConnectionPool: HikariDataSource,
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
        readOnlyTransactor = new Transactor.ReadOnly(
          system.queries,
          readOnlyConnectionPool,
          metrics,
          readOnlyExecutionContext,
        ),
        readWriteTransactor = new Transactor.ReadWrite(
          system.queries,
          readWriteConnectionPool,
          metrics,
          readWriteExecutionContext,
        ),
        metrics = metrics,
      )
    }

    def migrateAndReset()(
        implicit executionContext: ExecutionContext,
        loggerCtx: LoggingContext): Future[Database] = {
      val db = migrate()
      db.inTransaction("ledger_reset")(_.truncate()).map(_ => db)
    }

    def clear(): this.type = {
      flyway.clean()
      this
    }
  }

  final class InvalidDatabaseException(message: String)
      extends RuntimeException(message)
      with StartupException
}
