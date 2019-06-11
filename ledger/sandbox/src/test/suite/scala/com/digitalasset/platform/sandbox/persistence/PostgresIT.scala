// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.persistence

import com.digitalasset.platform.sandbox.stores.ledger.sql.dao.HikariJdbcConnectionProvider
import org.scalatest._

class PostgresIT extends WordSpec with Matchers with PostgresAroundAll {

  private lazy val connectionProvider = HikariJdbcConnectionProvider(postgresFixture.jdbcUrl, 4, 4)

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

  override protected def afterAll(): Unit = {
    connectionProvider.close()
    super.afterAll()
  }

}
