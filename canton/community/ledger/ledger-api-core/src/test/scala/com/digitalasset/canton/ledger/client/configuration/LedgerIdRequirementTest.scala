// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.client.configuration

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class LedgerIdRequirementTest extends AnyWordSpec with Matchers {

  "LedgerIdRequirement" when {
    "matching a specific value" should {
      "accept a matching ledger ID" in {
        val expected = "ledger-a"
        val requirement = LedgerIdRequirement.matching(expected)
        requirement.isAccepted(expected) shouldBe true
      }

      "reject any other ledger ID" in {
        val requirement = LedgerIdRequirement.matching("ledger-b")
        requirement.isAccepted("not-b") shouldBe false
      }

      "copy as usual" in {
        val requirement = LedgerIdRequirement.matching("ledger-d")
        val copied = requirement.copy(ledgerId = Some("ledger-e"))
        copied shouldBe LedgerIdRequirement.matching("ledger-e")
      }
    }

    "none" should {
      "allow any ledger ID" in {
        val requirement = LedgerIdRequirement.none
        requirement.isAccepted("any-ledger") shouldBe true
      }

      "copy as usual" in {
        val requirement = LedgerIdRequirement.matching("ledger-2")
        val copied = requirement.copy(ledgerId = Some("ledger-3"))
        copied shouldBe LedgerIdRequirement.matching("ledger-3")
      }
    }
  }
}
