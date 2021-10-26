// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.on.sql

import com.daml.ledger.api.domain.LedgerId
import com.daml.on.sql.CliSpec._
import com.daml.platform.apiserver.SeedService.Seeding
import com.daml.platform.common.LedgerIdMode
import com.daml.platform.sandbox.cli.CommonCliSpecBase
import com.daml.platform.sandbox.cli.CommonCliSpecBase._
import org.scalatest.BeforeAndAfterEach

import scala.collection.mutable

class CliSpec
    extends CommonCliSpecBase(
      cli = new Cli(DefaultConfig, getEnv = fakeEnv.get),
      requiredArgs = Array(
        "--ledgerid",
        "test-ledger",
        "--sql-backend-jdbcurl",
        exampleJdbcUrl,
      ),
      expectedDefaultConfig = Some(
        DefaultConfig.copy(
          ledgerIdMode = LedgerIdMode.Static(LedgerId("test-ledger")),
          jdbcUrl = Some(exampleJdbcUrl),
        )
      ),
    )
    with BeforeAndAfterEach {

  "Cli" should {
    "reject when the ledgerid is not provided" in {
      val config = cli.parse(Array("--sql-backend-jdbcurl", exampleJdbcUrl))
      config shouldEqual None
    }

    "reject when the sql-backend-jdbcurl is not provided" in {
      val config = cli.parse(Array("--ledgerid", "test-ledger"))
      config shouldEqual None
    }

    "reject when the sql-backend-jdbcurl is not a PostgreSQL URL" in {
      val config =
        cli.parse(Array("--ledgerid", "test-ledger", "--sql-backend-jdbcurl", "jdbc:h2:mem"))
      config shouldEqual None
    }

    "reject implicit party allocation" in {
      val config =
        cli.parse(Array("--ledgerid", "test-ledger", "--implicit-party-allocation", "true"))
      config shouldEqual None
    }

    "parse the eager package loading flag when given" in {
      checkOption(Array("--eager-package-loading"), _.copy(eagerPackageLoading = true))
    }

    "reject when sql-backend-jdbcurl-env points to a non-existing environment variable" in {
      val config =
        cli.parse(Array("--ledgerid", "test-ledger", "--sql-backend-jdbcurl-env", "JDBC_URL"))
      config shouldEqual None
    }

    "reject when sql-backend-jdbcurl-env is not a PostgreSQL URL" in {
      fakeEnv += "JDBC_URL" -> "jdbc:h2:mem"
      val config =
        cli.parse(Array("--ledgerid", "test-ledger", "--sql-backend-jdbcurl-env", "JDBC_URL"))
      config shouldEqual None
    }

    "accept a PostgreSQL JDBC URL from sql-backend-jdbcurl-env" in {
      val jdbcUrl = "jdbc:postgresql://"
      fakeEnv += "JDBC_URL" -> jdbcUrl
      val config =
        cli.parse(Array("--ledgerid", "test-ledger", "--sql-backend-jdbcurl-env", "JDBC_URL"))
      config
        .getOrElse(fail("The configuration was not parsed correctly"))
        .jdbcUrl
        .getOrElse(fail("The JDBC URL was not set")) should be(jdbcUrl)
    }

    "parse the contract-id-seeding mode when given" in {
      checkOption(
        Array("--contract-id-seeding", "testing-weak"),
        _.copy(seeding = Some(Seeding.Weak)),
      )
    }

    "reject a contract-id-seeding mode of 'no'" in {
      val config = cli.parse(requiredArgs ++ Array("--contract-id-seeding", "no"))
      config shouldEqual None
    }
  }

  override def beforeEach(): Unit = {
    fakeEnv.clear()
  }
}

object CliSpec {
  private val fakeEnv = mutable.Map[String, String]()
}
