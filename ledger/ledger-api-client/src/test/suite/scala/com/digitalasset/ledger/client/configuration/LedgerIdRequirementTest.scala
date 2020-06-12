// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.configuration

import org.scalatest.{Matchers, WordSpec}

class LedgerIdRequirementTest extends WordSpec with Matchers {

  "LedgerIdRequirement" should {

    "allow accept matching ledger" in {
      val expected = "ledger-a"
      LedgerIdRequirement.matching(expected).isAccepted(expected) shouldBe true
    }

    "allow not accept non-matching ledger" in {
      LedgerIdRequirement.matching("ledger-b").isAccepted("not-b") shouldBe false
    }

    "empty constructor should allow any ledger" in {
      LedgerIdRequirement.none.isAccepted("any-ledger") shouldBe true
    }

  }
}
