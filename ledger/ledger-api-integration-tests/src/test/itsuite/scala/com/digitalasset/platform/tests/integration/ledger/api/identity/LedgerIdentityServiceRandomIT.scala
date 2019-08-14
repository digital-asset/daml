// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.tests.integration.ledger.api.identity

import com.digitalasset.ledger.api.testing.utils.LedgerBackend
import com.digitalasset.platform.common.LedgerIdMode

import scala.concurrent.Promise

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class LedgerIdentityServiceRandomIT extends LedgerIdentityServiceITBase {

  override protected val config: Config =
    Config.default.withLedgerIdMode(LedgerIdMode.Dynamic())

  "A platform" when {

    //TODO: rewrite these tests so they are independent
    lazy val oldIdPromises: Map[LedgerBackend, Promise[String]] =
      fixtures.map(f => f.id -> Promise[String]).toMap

    "started" should {
      "have a random ID when none is given" in forAllFixtures {
        case TestFixture(ledgerBackend, context) =>
          for {
            ledgerId <- getLedgerId(context.ledgerIdentityService)
          } yield {
            oldIdPromises(ledgerBackend).success(ledgerId)

            ledgerId should not be empty
            ledgerId should not equal givenId
          }
      }
    }

    "started again" should {
      "have another random ID when none is given" in forAllFixtures {
        case TestFixture(ledgerBackend, context) =>
          for {
            oldId <- oldIdPromises(ledgerBackend).future
            ledgerId <- getLedgerId(context.ledgerIdentityService)
          } yield {
            ledgerId should not be empty
            ledgerId should not equal givenId
            ledgerId should not equal oldId
          }
      }
    }
  }
}
