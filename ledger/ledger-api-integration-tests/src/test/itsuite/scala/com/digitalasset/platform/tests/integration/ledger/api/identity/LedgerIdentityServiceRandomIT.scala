// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.tests.integration.ledger.api.identity

import com.digitalasset.platform.RequestedLedgerAPIMode
import com.digitalasset.platform.testing.LedgerBackend

import scala.concurrent.Promise

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class LedgerIdentityServiceRandomIT extends LedgerIdentityServiceITBase {

  override protected val config: Config = Config.default.withLedgerIdMode(RequestedLedgerAPIMode.Dynamic())

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
