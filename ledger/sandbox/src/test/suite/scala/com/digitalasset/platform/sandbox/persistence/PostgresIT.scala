// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.persistence

import com.codahale.metrics.MetricRegistry
import com.daml.dec.DirectExecutionContext
import com.daml.logging.LoggingContext.newLoggingContext
import com.daml.platform.configuration.ServerRole
import com.daml.platform.store.FlywayMigrations
import com.daml.platform.store.dao.{HikariJdbcConnectionProvider, JdbcConnectionProvider}
import com.daml.resources.Resource
import com.daml.testing.postgresql.PostgresAroundAll
import org.scalatest.{AsyncWordSpec, BeforeAndAfterAll, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

class PostgresIT extends AsyncWordSpec with Matchers with PostgresAroundAll with BeforeAndAfterAll {

  private var connectionProviderResource: Resource[JdbcConnectionProvider] = _
  private var connectionProvider: JdbcConnectionProvider = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    connectionProviderResource = HikariJdbcConnectionProvider
      .owner(
        ServerRole.Testing(getClass),
        postgresDatabase.url,
        maxConnections = 4,
        new MetricRegistry,
      )
      .acquire()(DirectExecutionContext)
    connectionProvider = Await.result(connectionProviderResource.asFuture, 10.seconds)
  }

  override protected def afterAll(): Unit = {
    Await.result(connectionProviderResource.release(), 10.seconds)
    super.afterAll()
  }

  "Postgres" when {
    "running queries using Hikari" should {
      "be accessible" in {
        connectionProvider.runSQL { conn =>
          val resultSet = conn.createStatement().executeQuery("SELECT 1")
          resultSet.next()
          val result = resultSet.getInt(1)
          result shouldEqual 1
        }
      }
    }
  }

  "Flyway" should {
    "execute initialisation script" in {
      newLoggingContext { implicit logCtx =>
        new FlywayMigrations(postgresDatabase.url).migrate()(DirectExecutionContext)
      }.map { _ =>
        connectionProvider.runSQL { conn =>
          def checkTableExists(table: String) = {
            val resultSet = conn.createStatement().executeQuery(s"SELECT * from $table")
            resultSet.next shouldEqual false
          }

          checkTableExists("parameters")
          checkTableExists("configuration_entries")

          checkTableExists("participant_command_completions")
          checkTableExists("participant_command_submissions")
          checkTableExists("participant_contract_witnesses")
          checkTableExists("participant_contracts")
          checkTableExists("participant_event_flat_transaction_witnesses")
          checkTableExists("participant_event_transaction_tree_witnesses")
          checkTableExists("participant_events")

          checkTableExists("parties")
          checkTableExists("party_entries")

          checkTableExists("packages")
          checkTableExists("package_entries")

        }
      }
    }
  }
}
