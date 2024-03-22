// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.configuration

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.annotation.nowarn

@nowarn(
  "msg=LedgerIdRequirement is deprecated \\(since 1.3.0\\): Use Option-based (copy|constructor)"
)
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

      "construct, matching the deprecated constructor" in {
        val requirement = LedgerIdRequirement("ledger-c", enabled = true)
        requirement shouldBe LedgerIdRequirement.matching("ledger-c")
      }

      "copy as usual" in {
        val requirement = LedgerIdRequirement.matching("ledger-d")
        val copied = requirement.copy(ledgerId = Some("ledger-e"))
        copied shouldBe LedgerIdRequirement.matching("ledger-e")
      }

      "copy, matching the deprecated constructor" in {
        val requirement = LedgerIdRequirement.matching("ledger-f")
        val copied = requirement.copy(ledgerId = "ledger-g")
        copied shouldBe LedgerIdRequirement.matching("ledger-g")
      }
    }

    "none" should {
      "allow any ledger ID" in {
        val requirement = LedgerIdRequirement.none
        requirement.isAccepted("any-ledger") shouldBe true
      }

      "construct, matching the deprecated constructor" in {
        val requirement = LedgerIdRequirement("ledger-1", enabled = false)
        requirement shouldBe LedgerIdRequirement.none
      }

      "copy as usual" in {
        val requirement = LedgerIdRequirement.matching("ledger-2")
        val copied = requirement.copy(ledgerId = Some("ledger-3"))
        copied shouldBe LedgerIdRequirement.matching("ledger-3")
      }

      "copy, matching the deprecated constructor" in {
        val requirement = LedgerIdRequirement.none
        val copied = requirement.copy(ledgerId = "ledger-4")
        copied shouldBe LedgerIdRequirement.none
      }
    }
  }
}
