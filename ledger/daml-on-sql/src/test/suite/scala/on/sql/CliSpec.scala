// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.on.sql

import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.participant.state.v1.SeedService.Seeding
import com.daml.platform.common.LedgerIdMode
import com.daml.platform.sandbox.cli.CommonCliSpecBase
import com.daml.platform.sandbox.cli.CommonCliSpecBase._

class CliSpec
    extends CommonCliSpecBase(
      cli = new Cli(DefaultConfig),
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
        )),
    ) {

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

}
