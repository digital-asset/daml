// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.tests.integration.ledger.api.identity

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class LedgerIdentityServiceGivenIT extends LedgerIdentityServiceITBase {

  override protected def config: Config = Config.defaultWithLedgerId(givenId)

  "A platform" when {
    "started" should {
      "have a given ledger id" in allFixtures { context =>
        for {
          ledgerId <- getLedgerId(context.ledgerIdentityService)
        } yield {
          ledgerId should not be empty
          ledgerId shouldEqual givenId
        }
      }
    }
  }

}
