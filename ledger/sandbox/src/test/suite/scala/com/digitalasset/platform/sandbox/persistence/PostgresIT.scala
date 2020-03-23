// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.persistence

import com.codahale.metrics.MetricRegistry
import com.digitalasset.dec.DirectExecutionContext
import com.digitalasset.logging.LoggingContext.newLoggingContext
import com.digitalasset.platform.configuration.ServerRole
import com.digitalasset.platform.store.FlywayMigrations
import com.digitalasset.platform.store.dao.{HikariJdbcConnectionProvider, JdbcConnectionProvider}
import com.digitalasset.resources.Resource
import com.digitalasset.testing.postgresql.PostgresAroundAll
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
        postgresFixture.jdbcUrl,
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
        new FlywayMigrations(postgresFixture.jdbcUrl).migrate()(DirectExecutionContext)
      }.map { _ =>
        connectionProvider.runSQL { conn =>
          def checkTableExists(table: String) = {
            val resultSet = conn.createStatement().executeQuery(s"SELECT * from $table")
            resultSet.next shouldEqual false
          }

          checkTableExists("ledger_entries")
          checkTableExists("contracts")
          checkTableExists("disclosures")
          checkTableExists("contract_witnesses")
          checkTableExists("parameters")
          checkTableExists("parties")
        }
      }
    }
  }
}
