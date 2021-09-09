// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.persistence

import com.codahale.metrics.{MetricRegistry, SharedMetricRegistries}
import com.daml.dec.DirectExecutionContext
import com.daml.ledger.resources.{Resource, ResourceContext}
import com.daml.logging.LoggingContext.newLoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.configuration.ServerRole
import com.daml.platform.store.FlywayMigrations
import com.daml.platform.store.dao.{HikariJdbcConnectionProvider, JdbcConnectionProvider}
import com.daml.testing.postgresql.PostgresAroundAll
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

class PostgresIT extends AsyncWordSpec with Matchers with PostgresAroundAll with BeforeAndAfterAll {

  private var connectionProviderResource: Resource[JdbcConnectionProvider] = _
  private var connectionProvider: JdbcConnectionProvider = _
  private val metrics = new Metrics(SharedMetricRegistries.getOrCreate("PostgresIT"))

  override def beforeAll(): Unit = {
    super.beforeAll()
    newLoggingContext { implicit loggingContext =>
      connectionProviderResource = HikariJdbcConnectionProvider
        .owner(
          ServerRole.Testing(getClass),
          postgresDatabase.url,
          maxConnections = 4,
          3.seconds,
          new MetricRegistry,
        )
        .acquire()(ResourceContext(DirectExecutionContext))
      connectionProvider = Await.result(connectionProviderResource.asFuture, 10.seconds)
    }
  }

  override protected def afterAll(): Unit = {
    Await.result(connectionProviderResource.release(), 10.seconds)
    super.afterAll()
  }

  "Postgres" when {
    "running queries using Hikari" should {
      "be accessible" in {
        connectionProvider.runSQL(metrics.test.db) { conn =>
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
      newLoggingContext { implicit loggingContext =>
        new FlywayMigrations(postgresDatabase.url)(
          ResourceContext(DirectExecutionContext),
          implicitly,
        )
          .migrate()
      }.map { _ =>
        connectionProvider.runSQL(metrics.test.db) { conn =>
          def checkTableExists(table: String) = {
            val resultSet = conn.createStatement().executeQuery(s"SELECT * from $table")
            resultSet.next shouldEqual false
          }

          def checkTableDoesNotExist(table: String) = {
            val resultSet = conn.createStatement().executeQuery(s"SELECT to_regclass('$table')")
            resultSet.next shouldEqual true
            Option(resultSet.getString(1)) shouldEqual Option.empty[String]
            resultSet.wasNull() shouldEqual true
          }

          checkTableExists("parameters")
          checkTableExists("configuration_entries")

          checkTableExists("participant_command_completions")
          checkTableExists("participant_command_submissions")
          checkTableExists("participant_contract_witnesses")
          checkTableExists("participant_contracts")
          checkTableExists("participant_events")

          checkTableExists("parties")
          checkTableExists("party_entries")

          checkTableExists("packages")
          checkTableExists("package_entries")

          checkTableDoesNotExist("participant_event_flat_transaction_witnesses")
          checkTableDoesNotExist("participant_event_transaction_tree_witnesses")
        }
      }
    }
  }
}
