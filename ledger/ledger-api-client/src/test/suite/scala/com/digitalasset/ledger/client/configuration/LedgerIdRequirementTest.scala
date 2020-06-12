// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.configuration

import com.github.ghik.silencer.silent
import org.scalatest.{Matchers, WordSpec}

@silent("deprecated")
class LedgerIdRequirementTest extends WordSpec with Matchers {

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
        val requirement = LedgerIdRequirement.matching("ledger-c")
        val copied = requirement.copy(ledgerId = Some("ledger-d"))
        copied shouldBe LedgerIdRequirement.matching("ledger-d")
      }

      "copy, matching the deprecated constructor" in {
        val requirement = LedgerIdRequirement.matching("ledger-c")
        val copied = requirement.copy(ledgerId = "ledger-d")
        copied shouldBe LedgerIdRequirement.matching("ledger-d")
      }
    }

    "none" should {
      "allow any ledger ID" in {
        val requirement = LedgerIdRequirement.none
        requirement.isAccepted("any-ledger") shouldBe true
      }

      "copy as usual" in {
        val requirement = LedgerIdRequirement.matching("ledger-1")
        val copied = requirement.copy(ledgerId = Some("ledger-2"))
        copied shouldBe LedgerIdRequirement.matching("ledger-2")
      }

      "copy, matching the deprecated constructor" in {
        val requirement = LedgerIdRequirement.none
        val copied = requirement.copy(ledgerId = "ledger-3")
        copied shouldBe LedgerIdRequirement.none
      }
    }
  }
}
