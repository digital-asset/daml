// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.configuration

import org.scalatest.{Matchers, WordSpec}

class LedgerIdRequirementTest extends WordSpec with Matchers {

  "LedgerIdRequirement" should {

    "allow verify ledger" in {
      val expected = "ledger-a"
      val underTest = LedgerIdRequirement(Some(expected))
      underTest.isAccepted(expected) shouldBe true
      underTest.isAccepted("not-a") shouldBe false
    }

    "empty constructor should allow any ledger" in {
      LedgerIdRequirement(None).isAccepted("any-ledger") shouldBe true
    }

  }
}
