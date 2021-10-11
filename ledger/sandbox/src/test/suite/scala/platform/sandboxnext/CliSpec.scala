// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandboxnext

import com.daml.ledger.api.domain.LedgerId
import com.daml.lf.data.Ref
import com.daml.platform.apiserver.SeedService.Seeding
import com.daml.platform.common.LedgerIdMode
import com.daml.platform.sandbox.cli.CommonCliSpecBase

class CliSpec extends CommonCliSpecBase(Cli) {

  "Cli" should {
    "parse the ledgerid when given" in {
      val ledgerId = "myledger"
      checkOption(
        Array("--ledgerid", ledgerId),
        _.copy(ledgerIdMode =
          LedgerIdMode.Static(LedgerId(Ref.LedgerString.assertFromString(ledgerId)))
        ),
      )
    }

    "parse the sql-backend-jdbcurl flag when given" in {
      val jdbcUrl = "jdbc:postgresql://localhost:5432/test?user=test"
      checkOption(Array("--sql-backend-jdbcurl", jdbcUrl), _.copy(jdbcUrl = Some(jdbcUrl)))
    }

    "parse the contract-id-seeding mode when given" in {
      checkOption(
        Array("--contract-id-seeding", "testing-weak"),
        _.copy(seeding = Some(Seeding.Weak)),
      )
    }

    "reject a contract-id-seeding mode of 'no'" in {
      val config = cli.parse(Array("--contract-id-seeding", "no"))
      config shouldEqual None
    }

    "parse implicit-party-allocation when given" in {
      checkOption(
        Array("--implicit-party-allocation", "false"),
        _.copy(implicitPartyAllocation = false),
      )
    }
  }

}
