// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.persistence

import org.scalatest._

class PostgresIT extends WordSpec with Matchers with PostgresAroundAll {

  "Postgres" when {

    "running queries using Hikari" should {

      "be accessible" in {
        postgresFixture.connectionProvider.runSQL { conn =>
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
      postgresFixture.connectionProvider.runSQL { conn =>
        def checkTableExists(table: String) = {
          val resultSet = conn.createStatement().executeQuery(s"SELECT * from $table")
          resultSet.next shouldEqual false
        }

        checkTableExists("ledger_entries")
        checkTableExists("contracts")
        checkTableExists("disclosures")
        checkTableExists("contract_witnesses")
        checkTableExists("parameters")
      }
    }

  }

}
