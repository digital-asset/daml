// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.testing.postgresql

import org.postgresql.ds.PGSimpleDataSource
import org.slf4j.LoggerFactory
import org.testcontainers.containers.PostgreSQLContainer

import java.sql.Statement
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference
import scala.util.Using

trait PostgresAround {
  import PostgresAround.*

  private val server: AtomicReference[PostgresServer] = new AtomicReference
  private val ownedServerContainer: AtomicReference[Option[PostgreSQLContainer[_]]] =
    new AtomicReference(None)

  protected def connectToPostgresqlServer(): Unit = {
    val isCI = sys.env.contains("CI")
    val isMachine = sys.env.contains("MACHINE")
    val forceTestContainer = sys.env.contains("DB_FORCE_TEST_CONTAINER")

    if (!forceTestContainer && (isCI && !isMachine)) {
      // using specified resource
      val hostName = sys.env.getOrElse("POSTGRES_HOST", "localhost")
      val port = 5432
      server.set(
        PostgresServer(
          hostName = hostName,
          port = port,
          userName = env("POSTGRES_USER"),
          password = env("POSTGRES_PASSWORD"),
          baseDatabase = env("POSTGRES_DB"),
        )
      )
      logger.info(s"Using PostgreSQL on $hostName:$port.")
    } else {
      // using own temporal resource
      val container = new PostgreSQLContainer(s"${PostgreSQLContainer.IMAGE}:12")
      ownedServerContainer.set(Some(container))
      logger.info(s"Starting PostgreSQL Container...")
      container.start()
      logger.info(s"PostgreSQL Container started.")
      val hostName = container.getHost
      val port = container.getFirstMappedPort
      server.set(
        PostgresServer(
          hostName = hostName,
          port = port,
          userName = container.getUsername,
          password = container.getPassword,
          baseDatabase = container.getDatabaseName,
        )
      )
      logger.info(s"Using PostgreSQL Container on $hostName:$port.")
    }

  }

  protected def disconnectFromPostgresqlServer(): Unit = {
    ownedServerContainer.get().foreach { container =>
      logger.info(s"Stopping PostgreSQL Container...")
      container.close()
      logger.info(s"PostgreSQL Container stopped.")
    }
  }

  protected def createNewRandomDatabase(): PostgresDatabase = {
    val database = executeAdminStatement(server.get()) { statement =>
      val databaseName = UUID.randomUUID().toString
      statement.execute(s"CREATE DATABASE \"$databaseName\"")
      statement.execute(s"CREATE USER \"$databaseName-user\" WITH PASSWORD 'user'")
      statement.execute(
        s"GRANT ALL PRIVILEGES ON DATABASE \"$databaseName\" TO \"$databaseName-user\""
      )
      PostgresDatabase(server.get(), databaseName, s"$databaseName-user", "user")
    }
    executeAdminStatement(server.get().copy(baseDatabase = database.databaseName))(
      _.execute(s"GRANT ALL ON SCHEMA public TO \"${database.userName}\"")
    )
    database
  }

  protected def dropDatabase(database: PostgresDatabase): Unit =
    executeAdminStatement(server.get()) { statement =>
      statement.execute(s"DROP DATABASE \"${database.databaseName}\"")
      statement.execute(s"DROP USER \"${database.databaseName}-user\"")
    }

}

object PostgresAround {
  private val logger = LoggerFactory.getLogger(getClass)

  private def executeAdminStatement[T](
      connectedPostgresServer: PostgresServer
  )(body: Statement => T): T = {
    val baseDatabase =
      PostgresDatabase(
        connectedPostgresServer,
        connectedPostgresServer.baseDatabase,
        connectedPostgresServer.userName,
        connectedPostgresServer.password,
      )
    Using.resource {
      val dataSource = new PGSimpleDataSource()
      dataSource.setUrl(baseDatabase.url)
      dataSource.getConnection
    } { connection =>
      Using.resource(connection.createStatement())(body)
    }
  }

  private def env(name: String): String =
    sys.env.getOrElse(name, sys.error(s"Environment variable not set [$name]"))
}
