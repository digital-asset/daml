// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.configuration

import org.scalatest.{Matchers, WordSpec}

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
    }

    "none" should {
      "allow any ledger ID" in {
        val requirement = LedgerIdRequirement.none
        requirement.isAccepted("any-ledger") shouldBe true
      }
    }
  }
}
